package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/kadmux"
	"net"
	"testing"
)

func TestNodeLookup_Basic(t *testing.T) {
	passive := make([]*Node, 10)
	for i := 0; i < 10; i++ {
		port := 5000 + i
		passive[i] = NewNode(gokad.NewDHT(), "127.0.0.1", port)
		go func(node *Node) {
			node.Listen(nil)
		}(passive[i])
	}

	bootstrapNode := NewNode(gokad.NewDHT(), "127.0.0.1", 6000)
	for _, n := range passive {
		bootstrapNode.Seed(gokad.Contact{
			ID:   n.dht.getOwnID(),
			IP:   net.ParseIP(n.host),
			Port: n.port,
		})
	}

	go func() {
		bootstrapNode.Listen(nil)
	}()

	joining := NewNode(gokad.NewDHT(), "", 7000)
	joining.Seed(gokad.Contact{
		ID:   bootstrapNode.dht.getOwnID(),
		IP:   net.ParseIP(bootstrapNode.host),
		Port: bootstrapNode.port,
	})

	defer func() {
		shutdown(passive...)
		shutdown(bootstrapNode)
		shutdown(joining)
	}()

	go func() {
		joining.Listen(nil)
	}()

	<-bootstrapNode.started
	<-joining.started
	config := func(n *NodeLookup) {
		n.LookupID = joining.dht.getOwnID()
		n.Alpha = joining.dht.getAlphaNodes(3, n.LookupID)
		n.Rpc = joining.NewClient()
		n.NodeReplyBuffer = joining.GetBuffer(kadmux.NodeReplyBufferID)
	}

	nodeLookup := NewNodeLookup(config)

	cs := Lookup(nodeLookup)

	if len(cs) != len(passive) + 1 {
		t.Fatalf("Expected %d to be returned but got %d\n", len(passive) + 1, len(cs))
	}

}

func TestNodeLookup_Nested(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), "127.0.0.1", 5000)
	node2 := NewNode(gokad.NewDHT(), "127.0.0.1", 5001)
	node3 := NewNode(gokad.NewDHT(), "127.0.0.1", 5002)
	node4 := NewNode(gokad.NewDHT(), "127.0.0.1", 5003)

	nodes := []*Node{node1, node2, node3, node4}

	node2.Seed(gokad.Contact{
		ID:   node1.dht.getOwnID(),
		IP:   net.ParseIP(node1.host),
		Port: node1.port,
	})

	node3.Seed(gokad.Contact{
		ID:   node2.dht.getOwnID(),
		IP:   net.ParseIP(node1.host),
		Port: node2.port,
	})

	node4.Seed(gokad.Contact{
		ID:   node3.dht.getOwnID(),
		IP:   net.ParseIP(node3.host),
		Port: node3.port,
	})

	for _, n := range nodes {
		go n.Listen(nil)
	}

	defer shutdown(nodes...)

	<-node4.started
	config := func(n *NodeLookup) {
		n.LookupID = node4.dht.getOwnID()
		n.Alpha = node4.dht.getAlphaNodes(3, n.LookupID)
		n.Rpc = node4.NewClient()
		n.NodeReplyBuffer = node4.GetBuffer(kadmux.NodeReplyBufferID)
	}

	lookup := NewNodeLookup(config)

	cs := Lookup(lookup)

	if len(cs) != 3 {
		t.Fatalf("Expected %d nodes, but got %d\n", 3, len(cs))
	}

}

func shutdown(nodes ...*Node) {
	for _, n := range nodes {
		n.Shutdown()
	}
}
