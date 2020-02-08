package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"net"
	"reflect"
	"sync"
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

	_, err := joining.Lookup(joining.ID())
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

// @ flaky test
//func TestNodeLookup_Nested(t *testing.T) {
//	nodes := make([]*Node, 0)
//	max := 20
//	for i := 0; i <= max; i++ {
//		node := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5000 + i })
//		nodes = append(nodes, node)
//
//		if i > 0 {
//			node.Seed(gokad.Contact{
//				ID:   nodes[i-1].ID(),
//				IP:   net.ParseIP(nodes[i-1].Host),
//				Port: nodes[i-1].Port,
//			})
//		}
//
//		go func(n *Node) {
//			n.Listen(nil)
//		}(node)
//	}
//
//	defer shutdown(nodes...)
//
//	joining := nodes[len(nodes)-1]
//	<-joining.started
//	if _, err := joining.Lookup(joining.ID()); err != nil {
//		t.Fatalf("Expected error to be nil, but got %s\n", err)
//	}
//
//	var count int
//	joining.Walk(func(index int, c gokad.Contact) {
//		count++
//	})
//
//	if count != max {
//		t.Fatalf("Expected %d nodes in the routing table, but got %d\n", max, count)
//	}
//}

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

	<-wait(node1, node2)
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

func TestNode_Store_Basic(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5001 })
	node2 := NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5002 })
	go node1.Listen(nil)
	go node2.Listen(nil)
	defer func() {
		shutdown(node1, node2)
	}()

	<-node1.started
	<-node2.started

	err := node1.Bootstrap(5002, "127.0.0.1")
	if err != nil {
		t.Fatalf("Expected err to be nil, but got %s\n", err)
	}

	key := gokad.GenerateRandomID()
	_, err = node1.Store(key.String(), net.ParseIP("127.0.0.1"), 7000)
	if err != nil {
		t.Fatalf("Expected err to be nil after Store, but got %s\n", err)
	}
}

func TestNode_Store_Many(t *testing.T) {
	nodes := make([]*Node, 10)
	for i := 0; i < len(nodes); i++ {
		nodes[i] = NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5000 + i })
		go func(node *Node) {
			err := node.Listen(nil)
			if err != nil {
				t.Fatalf("Listen error %s\n", err)
			}
		}(nodes[i])
	}

	defer func() {
		shutdown(nodes...)
	}()

	for i := 1; i < len(nodes); i++ {
		nodes[0].Seed(gokad.Contact{ID: nodes[i].ID(), IP: net.ParseIP(nodes[i].Host), Port: nodes[i].Port})
	}

	<-wait(nodes...)

	key := gokad.GenerateRandomID()
	n, err := nodes[0].Store(key.String(), net.ParseIP("127.0.0.1"), 8000)
	if err != nil {
		t.Fatalf("Expected error to be nil, but got %s\n", err)
	}

	if n != 3 {
		t.Fatalf("Expected a store rpc to be sent to %d nodes, but was only sent to %d", 3, n)
	}

}

func TestNode_NewResolver(t *testing.T) {
	nodes := make([]*Node, 10)
	for i := 0; i < len(nodes); i++ {
		nodes[i] = NewNode(gokad.NewDHT(), func(n *Node) { n.Port = 5000 + i })
		go func(node *Node) {
			err := node.Listen(nil)
			if err != nil {
				t.Fatalf("Listen error %s\n", err)
			}
		}(nodes[i])
	}

	defer func() {
		shutdown(nodes...)
	}()

	for i := 1; i < len(nodes); i++ {
		nodes[0].Seed(gokad.Contact{ID: nodes[i].ID(), IP: net.ParseIP(nodes[i].Host), Port: nodes[i].Port})
	}

	<-wait(nodes...)

	key := gokad.GenerateRandomID()
	// store the key in the network
	nodes[0].Store(key.String(), net.ParseIP("127.0.0.1"), 8000)
	// give the network some time to update itself
	time.Sleep(time.Second * 1)
	// create a resolve to resolve the key
	resolver, err := nodes[0].NewResolver()
	if err != nil {
		t.Fatalf("Expected error from NewResolver to be nil, but got %s\n", err)
	}
	// resolve the key
	addr, err := resolver.Resolve(key.String())
	if err != nil {
		t.Fatalf("Expected error from Resolve to be nil, but got %s\n", err)
	}

	if addr.String() != "127.0.0.1:8000" {
		t.Fatalf("Expected address to be 127.0.0.1:8000, but got %s\n", addr.String())
	}
}

// Send 10,000 pings to node1 and see how it handles it
func TestNode_Speed(t *testing.T) {
	node1 := NewNode(gokad.NewDHT(), func(n *Node) {n.Port = 5001})
	node2 := NewNode(gokad.NewDHT(), func(n *Node) {n.Port = 5002})

	go node1.Listen(nil)
	go node2.Listen(nil)
	defer shutdown(node1, node2)

	<-wait(node1, node2)

	for i:=0; i < 10000; i++ {
		c := node2.NewClient()
		res, err := c.Ping(gokad.Contact{
			ID: node1.ID(),
			IP:   net.ParseIP("127.0.0.1"),
			Port: 5001,
		})

		if err != nil {
			t.Fatalf("Expected err to be nil, but got %s\n", err)
		}

		res.ReadTimeout(time.Second * 3)
		if _, err := res.Read(messages.Explicit()); err != nil {
			t.Fatalf("Expected Read error for %d to be nil, but got %s\n", i, err)
		}
	}
}

func wait(nodes ...*Node) <-chan struct{} {
	out := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(node *Node) {
			<-node.started
			wg.Done()
		}(n)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func shutdown(nodes ...*Node) {
	for _, n := range nodes {
		n.Shutdown()
	}
}
