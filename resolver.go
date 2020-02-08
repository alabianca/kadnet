package kadnet

import (
	"errors"
	"github.com/alabianca/gokad"
	"net"
	"strconv"
)

type Resolver struct {
	client RpcFindValue
	lookup *lookup
}

func (r *Resolver) Resolve(hash string) (net.Addr, error) {
	if r.lookup == nil {
		return nil, errors.New("lookup strategy not set")
	}

	key, err := gokad.From(hash)
	if err != nil {
		return nil, err
	}

	cs, err := r.lookup.do(key)
	if err != nil || len(cs) == 0 {
		return nil, errors.New("no contacts found")
	}

	// for now just return the first ip port pair
	contact := cs[0]
	return net.ResolveTCPAddr("tcp", net.JoinHostPort(contact.IP.String(), strconv.Itoa(contact.Port)))
}
