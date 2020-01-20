package kadnet

import (
	"github.com/alabianca/gokad"
	"net"
	"reflect"
	"testing"
)

func TestNewNodeLookup(t *testing.T) {
	id := gokad.GenerateRandomID()
	alpha := []gokad.Contact{
		generateContact("28f787e3b60f99fb29b14266c40b536d6037307e"),
		generateContact("68f787e3b60f99fb29b14266c40b536d6037307e"),
	}

	c := func(n *NodeLookup) {
		n.LookupID = id
		n.Alpha = alpha
	}

	n := NewNodeLookup(c)

	if !reflect.DeepEqual(n.Alpha, alpha) {
		t.Fatalf("Expected alpha to be equal")
	}

	if !reflect.DeepEqual(n.LookupID, id) {
		t.Fatalf("Expected ids to be equal")
	}

	if n.Concurrency() != 3 {
		t.Fatalf("Expected concurrency to be %d, but got %d\n", 3, n.Concurrency())
	}

	if n.MaxConcurrency() != 20 {
		t.Fatalf("Expected max concurrency to be %d, but got %d\n", 20, n.MaxConcurrency())
	}
}

func TestLookup(t *testing.T) {
	id := gokad.GenerateRandomID()
	lookup := MockXLookup(id)

	contacts := make([]gokad.Contact, 0)
	for i := 0; i < 10; i++ {
		contacts = append(contacts, generateContact(gokad.GenerateRandomID().String()))
	}


	lookup.Buffer().Insert(id.DistanceTo(contacts[0].ID), &enquiredNode{contact: contacts[0]})
	lookup.Buffer().Insert(id.DistanceTo(contacts[1].ID), &enquiredNode{contact: contacts[1]})
	lookup.Buffer().Insert(id.DistanceTo(contacts[2].ID), &enquiredNode{contact: contacts[2]})


	//go func() {
	//	cs := Lookup(lookup)
	//}()
	//
	//lookup.AddWinner(FindNodeResult{
	//	node: &enquiredNode{contact: contacts[0]},
	//	payload: []gokad.Contact{contacts[3]},
	//})
	//
	//lookup.AddWinner(FindNodeResult{
	//	node: &enquiredNode{contact: contacts[1]},
	//	//payload: []gokad.Contact{contacts[4]},
	//})
	//
	//lookup.AddWinner(FindNodeResult{
	//	node: &enquiredNode{contact: contacts[2]},
	//	//payload: []gokad.Contact{contacts[5]},
	//})


}

func MockXLookup(id gokad.ID) *mockXLookup {
	return &mockXLookup{
		buf:    NewMap(compareDistance),
		losers: make(chan FindNodeResult),
		round:  make(chan FindNodeResult),
		key:    id,
	}
}

type mockXLookup struct {
	buf *TreeMap
	losers chan FindNodeResult
	round chan FindNodeResult
	curr chan FindNodeResult
	currl chan FindNodeResult
	key gokad.ID
}

func (m *mockXLookup) AddWinner(x FindNodeResult) {
	m.curr <- x
}

func (m *mockXLookup) AddLoser(x FindNodeResult) {
	m.currl <- x
}

func (m *mockXLookup) Buffer() *TreeMap {
	return m.buf
}
func (m *mockXLookup) Concurrency() int {
	return 3
}
func (m *mockXLookup) MaxConcurrency() int {
	return 20
}
func (m *mockXLookup) Key() gokad.ID {
	return m.key
}
func (m *mockXLookup) NextRound(c int, next []PendingNode) bool {
	var index int
	m.buf.Traverse(func(k gokad.Distance, node PendingNode) bool {
		if !node.Queried() && (index < c) {
			node.SetQueried(true)
			next[index] = node
			index++
		}

		if index >= c {
			return false
		}

		return true
	})

	if index > 0 {
		return true
	}

	return false
}

func (m *mockXLookup) Round(nodes []PendingNode) chan FindNodeResult {
	m.curr = make(chan FindNodeResult)
	return m.curr
}
func (m *mockXLookup) Losers() chan FindNodeResult {
	m.currl = make(chan FindNodeResult)
	return m.currl
}

func generateContact(id string) gokad.Contact {
	x, _ := gokad.From(id)
	return gokad.Contact{
		ID:   x,
		IP:   net.ParseIP("127.0.0.1"),
		Port: 5050,
	}
}
