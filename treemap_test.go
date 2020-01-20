package kadnet

import (
	"github.com/alabianca/gokad"
	"net"
	"reflect"
	"testing"
)

func TestTreeMap_Traverse(t *testing.T) {
	lookup, _ := gokad.From("28f787e3b60f99fb29b14266c40b536d6037307e")
	far := &pendingNode{contact: generateContact("68f787e3b60f99fb29b14266c40b536d6037307e")}
	close := &pendingNode{contact: generateContact("28f787e3b60f99fb29b14266c40b536d6037303e")}
	closer := &pendingNode{contact: generateContact("28f787e3b60f99fb29b14266c40b536d6037307f")}
	furthest := &pendingNode{contact: generateContact("a8f787e3b60f99fb29b14266c40b536d6037307e")}
	tm := newMap(compareDistance)

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
	tm.Traverse(func(k gokad.Distance, v *pendingNode) bool {
		if !reflect.DeepEqual(v, order[index]) {
			t.Fatalf("Expected node with id %s at index %d, but got node %s\n", order[index].contact.ID, index, v.Contact().ID)
		}
		index++
		return true
	})
}


func generateContact(id string) gokad.Contact {
	x, _ := gokad.From(id)
	return gokad.Contact{
		ID:   x,
		IP:   net.ParseIP("127.0.0.1"),
		Port: 5050,
	}
}

