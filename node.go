package kadnet

import (
	"errors"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/kadmux"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/response"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type RPC interface {
	FindNode(contact gokad.Contact, lookupID gokad.ID) (*response.Response, error)
}

type NodeConfig func(*Node)

type Node struct {
	K            int
	Alpha        int
	RoundTimeout time.Duration
	Host         string
	Port         int
	dht          *dhtProxy
	conn         kadconn.KadConn
	mux          kadmux.Mux
	started      chan bool
}

func NewNode(dht *gokad.DHT, configs ...NodeConfig) *Node {
	n := &Node{
		dht:          newDhtProxy(dht),
		K:            20,
		Alpha:        3,
		RoundTimeout: time.Second * 3,
		Host:         "127.0.0.1",
		Port:         5000,
		started:      make(chan bool, 1),
	}

	for _, config := range configs {
		config(n)
	}

	return n
}

// Bootstrap follows the following bootstrapping procedure
/**
	1. The gateway is inserted in the appropriate k-bucket.
	2. A node lookup for the own id is performed. Of course, the only node that will be contacted
	   initially is the gateway. Through the node lookup for the own id, the node gets to know its
	   closest neighbors.
	3. Node lookups in the range for all k-buckets with a higher index than the one of the lowest
       non-empty are performed. This fills the k-buckets of the joining node as well as communicates
       the arrival of the new node to the existing nodes. Notice that node lookups for k-buckets
       with index lower than the first non-empty would be useless, as there are no appropriate
	   contacts in the network (otherwise, the lookup for the own id would have revealed them).

@Source: Implementation of the Kademlia Hash Table by Bruno Spori Semester Thesis
https://pub.tik.ee.ethz.ch/students/2006-So/SA-2006-19.pdf
**/
func (n *Node) Bootstrap(port int, ip string) error {
	// 1. Insert Gateway into k-bucket
	c, err := n.pingAndGetFirst(net.ParseIP(ip), port)
	if err != nil {
		return err
	}

	if _, _, err := n.dht.insert(c); err != nil {
		return err
	}

	// 2. node lookup for own id
	if _, err := n.Lookup(n.ID()); err != nil {
		return err
	}

	// 3. @todo

	return nil

}
func (n *Node) ID() gokad.ID {
	return n.dht.getOwnID()
}

func (n *Node) Walk(f func(index int, c gokad.Contact)) {
	n.dht.walk(f)
}

func (n *Node) Seed(cs ...gokad.Contact) {
	for _, c := range cs {
		n.dht.insert(c)
	}
}

func (n *Node) Shutdown() {
	if n.mux != nil {
		n.mux.Close()
	}
}

func (n *Node) Listen(mux kadmux.Mux) error {
	if mux == nil {
		mux = defaultMux()
	}
	n.mux = mux
	n.registerRequestHandlers()

	c, err := n.listen()
	if err != nil {
		return err
	}
	n.conn = c
	n.started <- true

	defer c.Close()

	return n.mux.Handle(c)
}

func (n *Node) Ping(host net.IP, port int, id gokad.ID) (gokad.Contact, error) {
	if id == nil {
		return gokad.Contact{}, errors.New("id not provided")
	}

	return n.ping(host, port, id)

}

func (n *Node) Store(key string, ip net.IP, port int) (int, error) {
	buf := n.getBuffer(kadmux.StoreReplyBufferID)
	if buf == nil {
		return 0, errors.New("could not open buffer")
	}
	buf.Open()
	defer buf.Close()

	keyID, err := gokad.From(key)
	if err != nil {
		return 0, err
	}

	cs, err := n.Lookup(keyID)
	if err != nil {
		return 0, err
	}

	client := n.NewClient()
	storeSent := make(chan int)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(contact gokad.Contact) {
			defer wg.Done()
			res, err := client.Store(contact, keyID, gokad.Value{Host: ip, Port: port})
			if err != nil {
				storeSent <- 0
			}

			res.Read(&messages.StoreResponse{})
			storeSent <- 1
		}(c)
	}

	go func() {
		wg.Wait()
		close(storeSent)
	}()

	var total int
	for n := range storeSent {
		total += n
	}

	return total, nil
}

func (n *Node) Lookup(id gokad.ID) ([]gokad.Contact, error) {
	nodeLp, err := nodeLookup(func(l *lookup) {
		l.dht = n.dht
		l.nodeReplyBuffer = n.getBuffer(kadmux.NodeReplyBufferID)
		l.client = n.NewClient()
	})
	if err != nil {
		return nil, err
	}

	return nodeLp.do(id)

}

func (n *Node) NewClient() *Client {
	nodeReplyBuf, _ := n.getBuffer(kadmux.NodeReplyBufferID).(*buffers.NodeReplyBuffer)
	pingReplyBuf, _ := n.getBuffer(kadmux.PingReplyBufferID).(*buffers.PingReplyBuffer)
	storeReplyBuf, _ := n.getBuffer(kadmux.StoreReplyBufferID).(*buffers.StoreReplyBuffer)
	return &Client{
		ID:               n.dht.getOwnID(),
		Writer:           n.conn,
		NodeReplyBuffer:  nodeReplyBuf,
		PingReplyBuffer:  pingReplyBuf,
		StoreReplyBuffer: storeReplyBuf,
	}
}

func (n *Node) getBuffer(key string) buffers.Buffer {
	return n.mux.GetBuffer(key)

}

func (n *Node) registerRequestHandlers() {
	// register middlewares
	pingReplyBuffer := n.getBuffer(kadmux.PingReplyBufferID)
	n.mux.Use(
		kadmux.Logging(os.Stdout),               // Log requests to stdout
		kadmux.ExpectPingReply(pingReplyBuffer), // write the expected PingReplyMessage to the buffer
	)
	// handlers to run after middlewares executed
	n.mux.HandleFunc(messages.FindNodeReq, onFindNode(n.dht))
	n.mux.HandleFunc(messages.PingResImplicit, onPingReplyImplicit(n.dht, pingReplyBuffer))
	n.mux.HandleFunc(messages.PingReq, onPingRequest(n.ID()))
	n.mux.HandleFunc(messages.StoreReq, onStoreRequest(n.ID(), n.dht))
}

func (n *Node) listen() (kadconn.KadConn, error) {
	conn, err := net.ListenPacket("udp", net.JoinHostPort(n.Host, strconv.Itoa(n.Port)))

	return kadconn.New(conn), err
}

func (n *Node) ping(host net.IP, port int, id gokad.ID) (gokad.Contact, error) {
	res, err := n.sendPing(host, port, id)
	if err != nil {
		return gokad.Contact{}, err
	}

	var pr messages.PingResponse
	res.ReadTimeout(time.Second * 2)
	if _, err := res.Read(&pr); err != nil {
		return gokad.Contact{}, err
	}

	senderId, err := gokad.From(pr.SenderID)
	if err != nil {
		return gokad.Contact{}, err
	}

	return gokad.Contact{ID: senderId, IP: host, Port: port}, nil
}

// pingAndGetFirst pings the node at the given host and port
// and returns the first pingResponse it has in its buffer
// this function should only be used in the bootstrap procedure
// since we don't know our gateway's id yet
func (n *Node) pingAndGetFirst(host net.IP, port int) (gokad.Contact, error) {

	res, err := n.sendPing(host, port, nil)
	if err != nil {
		return gokad.Contact{}, err
	}

	prb, _ := res.Body.(*buffers.PingReplyBuffer)
	var pr messages.PingResponse
	prb.First(&pr)

	if pr.SenderID == "" {
		return gokad.Contact{}, errors.New("could not get first message of ping reply buffer")
	}

	idr, err := gokad.From(pr.SenderID)
	if err != nil {
		return gokad.Contact{}, err
	}

	return gokad.Contact{ID: idr, IP: host, Port: port}, nil
}

func (n *Node) sendPing(host net.IP, port int, id gokad.ID) (*response.Response, error) {
	client := n.NewClient()
	contact := gokad.Contact{
		ID:   id,
		IP:   host,
		Port: port,
	}

	return client.Ping(contact)
}

func defaultMux() kadmux.Mux {
	return kadmux.NewMux()
}
