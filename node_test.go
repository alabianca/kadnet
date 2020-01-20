package kadnet

import (
	"github.com/alabianca/gokad"
	"net"
	"testing"
)


func TestNodeLookup_Basic(t *testing.T) {
	passive := make([]*Node, 10)

	for i := 0; i < len(passive); i++ {
		config := func(n *Node) {
			n.Port = 5000 + i
		}

		passive[i] = NewNode(gokad.NewDHT(), config)
		go passive[i].Listen(nil)
	}

	bootstrapNode := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 6000 })
	go bootstrapNode.Listen(nil)

	for _, n := range passive {
		bootstrapNode.Seed(gokad.Contact{
			ID:   n.ID(),
			IP:   net.ParseIP(n.Host),
			Port: n.Port,
		})
	}

	joining := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 7000 })
	go joining.Listen(nil)
	joining.Seed(gokad.Contact{
		ID:   bootstrapNode.ID(),
		IP:   net.ParseIP(bootstrapNode.Host),
		Port: bootstrapNode.Port,
	})

	defer func() {
		shutdown(passive...)
		shutdown(bootstrapNode)
		shutdown(joining)
	}()

	<-bootstrapNode.started
	<-joining.started

	err := joining.Lookup(joining.ID())
	if err != nil {
		t.Fatalf("Expected error to be nil, but got %s\n", err)
	}

	var count int
	joining.Walk(func(index int, c gokad.Contact) {
		count++
	})

	if count != len(passive) + 1 {
		t.Fatalf("Expected %d contacts in the routing table, but got %d\n", len(passive) + 1, count)
	}
}

func TestNodeLookup_Nested(t *testing.T) {
	nodes := make([]*Node, 0)
	max := 20
	for i := 0; i <= max; i++ {
		node := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5000 + i})
		nodes = append(nodes, node)

		if i > 0 {
			node.Seed(gokad.Contact{
				ID:   nodes[i-1].ID(),
				IP:   net.ParseIP(nodes[i-1].Host),
				Port: nodes[i-1].Port,
			})
		}

		go func(n *Node) {
			n.Listen(nil)
		}(node)
	}

	defer shutdown(nodes...)


	joining := nodes[len(nodes) - 1]
	<- joining.started
	if err := joining.Lookup(joining.ID()); err != nil {
		t.Fatalf("Expected error to be nil, but got %s\n", err)
	}

	var count int
	joining.Walk(func(index int, c gokad.Contact) {
		count++
	})

	if count != max {
		t.Fatalf("Expected %d nodes in the routing table, but got %d\n", max, count)
	}
}

func shutdown(nodes ...*Node) {
	for _, n := range nodes {
		n.Shutdown()
	}
}
