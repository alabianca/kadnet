package kadnet

import (
	"github.com/alabianca/gokad"
	"net"
	"sync"
)

type dhtProxy struct {
	dht *gokad.DHT
	mtx sync.Mutex
}

func newDhtProxy(dht *gokad.DHT) *dhtProxy {
	return &dhtProxy{
		dht: dht,
		mtx: sync.Mutex{},
	}
}

func (proxy *dhtProxy) getOwnID() []byte {
	id := make(gokad.ID, len(proxy.dht.ID))
	copy(id, proxy.dht.ID)
	return id
}

func (proxy *dhtProxy) insert(c gokad.Contact) (gokad.Contact, int, error) {
	proxy.mtx.Lock()
	defer proxy.mtx.Unlock()
	return proxy.dht.RoutingTable().Add(c)
}

func (proxy *dhtProxy) store(key gokad.ID, ip net.IP, port int) {
	proxy.mtx.Lock()
	defer proxy.mtx.Unlock()
	proxy.dht.Store(key, ip, port)
}

func (proxy *dhtProxy) getAlphaNodes(alpha int, id gokad.ID) []gokad.Contact {
	proxy.mtx.Lock()
	defer proxy.mtx.Unlock()
	return proxy.dht.GetAlphaNodes(alpha, id)
}

func (proxy *dhtProxy) findNode(id gokad.ID) []gokad.Contact {
	proxy.mtx.Lock()
	defer proxy.mtx.Unlock()

	return proxy.dht.FindNode(id)
}

func (proxy *dhtProxy) walk(f func(bucketIndex int, c gokad.Contact)) {
	routing := proxy.dht.RoutingTable()
	for i := 0; i < gokad.MaxRoutingTableSize; i++ {
		bucket, ok := routing.Bucket(i)
		if ok {
			bucket.Walk(func(c gokad.Contact) bool {
				f(i, c)
				return false
			})
		}
	}
}
