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
	"strconv"
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
		dht:     newDhtProxy(dht),
		K: 20,
		Alpha: 3,
		RoundTimeout: time.Second * 3,
		Host: "127.0.0.1",
		Port: 5000,
		started: make(chan bool, 1),
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
func (n *Node) Bootstrap(port int, ip, idHex string) error {
	// 1. Insert Gateway into k-bucket
	_, _, err := n.dht.bootstrap(port, ip, idHex)
	if err != nil {
		return err
	}

	// 2. node lookup for own id
	//n.nodeLookup(n.ID())
	n.Lookup(n.ID())

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

func (n *Node) Lookup(id gokad.ID) error {
	nodeReplyBuffer := n.getBuffer(kadmux.NodeReplyBufferID)
	if nodeReplyBuffer == nil {
		return errors.New("cannot open Node Reply Buffer <nil>")
	}
	nodeReplyBuffer.Open()
	defer nodeReplyBuffer.Close()

	concurrency := n.Alpha
	pendingNodes := newMap(compareDistance)
	for _, c := range n.dht.getAlphaNodes(concurrency, id) {
		pendingNodes.Insert(id.DistanceTo(c.ID), &pendingNode{contact: c})
	}

	client := n.NewClient()
	timedOutNodes := make(chan findNodeResult)
	lateReplies := losers(timedOutNodes)
	next := make([]*pendingNode, concurrency)
	for nextRound(pendingNodes, concurrency, next) {
		rc := round(
			trim(next),
			client,
			id,
			n.RoundTimeout,
			timedOutNodes,
		)

		var atLeastOneNewNode bool
		for cs := range mergeLosersAndRound(lateReplies, rc) {
			if cs.err != nil {
				continue
			}

			cs.node.SetAnswered(true)
			for _, c := range cs.payload {
				distance := id.DistanceTo(c.ID)
				if _, ok := pendingNodes.Get(distance); !ok {
					atLeastOneNewNode = true
					pendingNodes.Insert(distance, &pendingNode{contact: c})
				}
				// contact details of any node that responded are attempted to be
				// inserted into the dht
				n.dht.insert(c)
			}
		}

		// if a round did not reveal at least one new node we take all K
		// closest nodes not already queried and send them FIND_NODE_RPC's
		if !atLeastOneNewNode {
			concurrency = n.K
		} else {
			concurrency = n.Alpha
		}

		next = make([]*pendingNode, concurrency)
	}


	return nil

}

func (n *Node) NewClient() *Client {
	nodeReplyBuf, _ := n.mux.GetBuffer(kadmux.NodeReplyBufferID).(*buffers.NodeReplyBuffer)
	return &Client{
		ID:              n.dht.getOwnID(),
		Writer:          n.conn,
		NodeReplyBuffer: nodeReplyBuf,
	}
}

func (n *Node) getBuffer(key string) buffers.Buffer {
	return n.mux.GetBuffer(key)

}

func (n *Node) registerRequestHandlers() {
	n.mux.HandleFunc(messages.FindNodeReq, onFindNode(n.dht))
}

func (n *Node) listen() (kadconn.KadConn, error) {
	conn, err := net.ListenPacket("udp", net.JoinHostPort(n.Host, strconv.Itoa(n.Port)))

	return kadconn.New(conn), err
}

func defaultMux() kadmux.Mux {
	return kadmux.NewMux()
}
