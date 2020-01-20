package kadnet_test

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet"
	"net"
	"reflect"
	"testing"
)

// @todo put this into the gokad package
var compareDistance = func(d1 gokad.Distance, d2 gokad.Distance) int {
	for i := 0; i < gokad.SIZE; i++ {
		if d1[i] > d2[i] {
			return -1
		}
		if d1[i] < d2[i] {
			return 1
		}
	}

	return 1
}

func TestTreeMap_Traverse(t *testing.T) {
	lookup, _ := gokad.From("28f787e3b60f99fb29b14266c40b536d6037307e")
	far := &pendingNode{generateContact("68f787e3b60f99fb29b14266c40b536d6037307e")}
	close := &pendingNode{generateContact("28f787e3b60f99fb29b14266c40b536d6037303e")}
	closer := &pendingNode{generateContact("28f787e3b60f99fb29b14266c40b536d6037307f")}
	furthest := &pendingNode{generateContact("a8f787e3b60f99fb29b14266c40b536d6037307e")}
	tm := kadnet.NewMap(compareDistance)

	order := []*pendingNode{
		closer,
		close,
		far,
		furthest,
	}

	tm.Insert(lookup.DistanceTo(far.contact.ID), far)
	tm.Insert(lookup.DistanceTo(closer.contact.ID), closer)
	tm.Insert(lookup.DistanceTo(furthest.contact.ID), furthest)
	tm.Insert(lookup.DistanceTo(close.contact.ID), close)

	var index int
	tm.Traverse(func(k gokad.Distance, v kadnet.PendingNode) bool {
		if !reflect.DeepEqual(v, order[index]) {
			t.Fatalf("Expected node with id %s at index %d, but got node %s\n", order[index].contact.ID, index, v.Contact().ID)
		}
		index++
		return true
	})
}

type pendingNode struct {
	contact gokad.Contact
}
func (p *pendingNode) Contact() gokad.Contact {
	return p.contact
}
func (p *pendingNode) Answered() bool {
	return false
}
func (p *pendingNode) Queried() bool {
	return false
}
func (p *pendingNode) SetAnswered(x bool) {

}
func (p *pendingNode) SetQueried(x bool) {
}

func generateContact(id string) gokad.Contact {
	x, _ := gokad.From(id)
	return gokad.Contact{
		ID:   x,
		IP:   net.ParseIP("127.0.0.1"),
		Port: 5050,
	}
}

