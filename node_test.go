package kadnet

import (
	"github.com/alabianca/gokad"
	"net"
	"reflect"
	"testing"
	"time"
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

	if count != len(passive)+1 {
		t.Fatalf("Expected %d contacts in the routing table, but got %d\n", len(passive)+1, count)
	}
}

func TestNodeLookup_Nested(t *testing.T) {
	nodes := make([]*Node, 0)
	max := 20
	for i := 0; i <= max; i++ {
		node := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5000 + i })
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

	joining := nodes[len(nodes)-1]
	<-joining.started
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

func TestPingReply(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5001 })
	node2 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5002 })
	go node1.Listen(nil)
	go node2.Listen(nil)
	defer func() {
		shutdown(node1)
		shutdown(node2)
	}()

	node2.Seed(gokad.Contact{
		ID:   node1.ID(),
		IP:   net.ParseIP(node1.Host),
		Port: node1.Port,
	})

	<-node2.started
	node2.Lookup(node2.ID())

	time.Sleep(time.Millisecond * 500)

	var c gokad.Contact
	var count int
	node1.Walk(func(index int, contact gokad.Contact) {
		c = contact
		count++
	})

	if count != 1 {
		t.Fatalf("Expected 1 contact, but got %d\n", count)
	}

	if !reflect.DeepEqual(c.ID, node2.ID()) {
		t.Fatalf("Expected ids to be equal, but got %s\n", c.ID)
	}
}

func TestNode_Ping(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5001 })
	node2 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5002 })

	go node1.Listen(nil)
	go node2.Listen(nil)
	defer func() {
		shutdown(node1, node2)
	}()

	<-node2.started
	<-node1.started

	c, err := node1.pingAndGetFirst(net.ParseIP("127.0.0.1"), 5002)
	if err != nil {
		t.Fatalf("Expected error to be nil, but got %s\n", err)
	}

	expectedID := make(gokad.ID, 20)
	copy(expectedID, node2.ID())
	expected := gokad.Contact{
		ID:   expectedID,
		IP:   net.ParseIP("127.0.0.1"),
		Port: 5002,
	}

	if !reflect.DeepEqual(c, expected) {
		t.Fatalf("Expected %s:%d:%s but got \n%s:%d:%s", expected.IP, expected.Port, expected.ID, c.IP, c.Port, c.ID)
	}
}

func TestNode_Bootstrap(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5001 })
	node2 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5002 })
	node3 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5003 })
	node4 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5004 })
	go node1.Listen(nil)
	go node2.Listen(nil)
	go node3.Listen(nil)
	go node4.Listen(nil)
	defer func() {
		shutdown(node1, node2, node3, node4)
	}()

	node2.Seed(gokad.Contact{ID: node3.ID(), IP: net.ParseIP("127.0.0.1"), Port: 5003})
	node2.Seed(gokad.Contact{ID: node4.ID(), IP: net.ParseIP("127.0.0.1"), Port: 5004})

	<-node2.started
	<-node1.started

	err := node1.Bootstrap(5002, "127.0.0.1")
	if err != nil {
		t.Fatalf("Expected err to be nil, but got %s\n", err)
	}

	// give all nodes time to update their tables since it is async
	time.Sleep(time.Millisecond * 500)
	checks := []struct {
		IN    *Node
		COUNT int
	}{
		{
			IN:    node1,
			COUNT: 3,
		},
		{
			IN:    node2,
			COUNT: 3,
		},
		{
			IN:    node3,
			COUNT: 1,
		},
		{
			IN:    node4,
			COUNT: 1,
		},
	}

	for _, check := range checks {
		var count int
		check.IN.Walk(func(index int, c gokad.Contact) {
			count++
		})

		if count != check.COUNT {
			t.Fatalf("Expected count to be %d, but got %d\n", check.COUNT, count)
		}
	}
}

func shutdown(nodes ...*Node) {
	for _, n := range nodes {
		n.Shutdown()
	}
}
