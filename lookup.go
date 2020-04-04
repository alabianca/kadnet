package kadnet

import (
	"errors"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/response"
	"sync"
	"time"
)

var compareDistance = func(d1 gokad.Distance, d2 gokad.Distance) int {
	for i := 0; i < gokad.SIZE; i++ {
		if d1[i] > d2[i] {
			return -1
		}
		if d1[i] < d2[i] {
			return 1
		}
	}

	return 0
}

type lookupStrategy interface {
	round(nextNodes []*pendingNode, timeouts chan<-findXResult) chan findXResult
	send(node *pendingNode) chan findXResult
	messageTypeId() int
	setLookupKey(key gokad.ID)
}

type pendingNode struct {
	contact  gokad.Contact
	answered bool
	queried  bool
}

func (p *pendingNode) Contact() gokad.Contact {
	return p.contact
}
func (p *pendingNode) Answered() bool {
	return p.answered
}
func (p *pendingNode) SetAnswered(x bool) {
	p.answered = x
}
func (p *pendingNode) Queried() bool {
	return p.queried
}
func (p *pendingNode) SetQueried(x bool) {
	p.queried = x
}

type findXResultPayload struct {
	key string
	contacts []gokad.Contact
}

type findXResult struct {
	node     *pendingNode
	payload  findXResultPayload
	response *response.Response
	err      error
}

type lookup struct {
	buffer       buffers.Buffer
	concurrency  int
	k            int
	dht          *dhtProxy
	client       *Client
	roundTimeout time.Duration
	strategy lookupStrategy
	isNodeLookup bool
}

type lookupConfig func(l *lookup)

func nodeLookup(config lookupConfig) (*lookup, error) {
	lp := lookup{
		concurrency:  3,
		k:            20,
		roundTimeout: time.Second * 3,
	}

	config(&lp)
	if lp.dht == nil {
		return nil, errors.New("dht not specified")
	}
	if lp.buffer == nil {
		return nil, errors.New("node reply buffer not specified")
	}

	if lp.client == nil {
		return nil, errors.New("client not specified")
	}

	var strategy lookupStrategy
	if lp.isNodeLookup {
		strategy = &findNodeStrategy{
			nodeReplyBuffer: lp.buffer,
			concurrency:     lp.concurrency,
			k:               lp.k,
			dht:             lp.dht,
			client:          lp.client,
			roundTimeout:    lp.roundTimeout,
		}
	} else {
		strategy = &findValueStrategy{
			valueReplyBuffer: lp.buffer,
			concurrency:      lp.concurrency,
			k:                lp.k,
			dht:              lp.dht,
			client:           lp.client,
			roundTimeout:     lp.roundTimeout,
		}
	}

	lp.strategy = strategy
	return &lp, nil
}

func (l *lookup) do(key gokad.ID) ([]gokad.Contact, error) {
	buf := l.buffer
	if buf == nil {
		return nil, errors.New("cannot open Node Reply Buffer <nil>")
	}
	// the buffer must be opened
	// once opened it is ready to receive answers to FIND_NODE_RPC's
	buf.Open()
	defer buf.Close()

	concurrency := l.concurrency
	closestNodes := newMap(compareDistance)
	for _, c := range l.dht.getAlphaNodes(concurrency, key) {
		closestNodes.Insert(key.DistanceTo(c.ID), &pendingNode{contact: c})
	}

	strategy := l.strategy
	strategy.setLookupKey(key)
	timedOutNodes := make(chan findXResult)
	lateReplies := losers(timedOutNodes, strategy.messageTypeId())
	next := make([]*pendingNode, concurrency)
	var foundValue bool
	var value []gokad.Contact
	for nextRound(closestNodes, concurrency, next, l.k) {
		rc := strategy.round(trim(next), timedOutNodes)

		var atLeastOneNewNode bool
		for cs := range mergeLosersAndRound(lateReplies, rc) {
			if cs.err != nil {
				continue
			}

			if cs.payload.key != "" {
				foundValue = true
				value = cs.payload.contacts
				break
			}

			cs.node.SetAnswered(true)
			for _, c := range cs.payload.contacts {
				distance := key.DistanceTo(c.ID)
				if _, ok := closestNodes.Get(distance); !ok {
					atLeastOneNewNode = true
					closestNodes.Insert(distance, &pendingNode{contact: c})
				}
				// contact details of any node that responded are attempted to be
				// inserted into the dht
				l.dht.insert(c)
			}
		}

		// if a round did not reveal at least one new node we take all K
		// closest nodes not already queried and send them FIND_NODE_RPC's
		if !atLeastOneNewNode {
			concurrency = l.k
		} else {
			concurrency = l.concurrency
		}

		next = make([]*pendingNode, concurrency)
	}

	if foundValue && !l.isNodeLookup {
		return value, nil
	} else if (!l.isNodeLookup) {
		return value, errors.New("not found")
	}

	return getKClosestNodes(closestNodes, l.k), nil
}


type findNodeStrategy struct {
	nodeReplyBuffer buffers.Buffer
	concurrency     int
	k               int
	dht             *dhtProxy
	client          *Client
	roundTimeout    time.Duration
	key gokad.ID
}

func (str *findNodeStrategy) messageTypeId() int {
	return 1
}

func (str *findNodeStrategy) setLookupKey(key gokad.ID) {
	str.key = key
}

func (str *findNodeStrategy) round(nodes []*pendingNode, timeouts chan<- findXResult) chan findXResult {
	out := make(chan findXResult)
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(node *pendingNode) {
			defer wg.Done()
			res := <- str.send(node)
			if res.err != nil && res.err.Error() == buffers.TimeoutErr {
				timeouts <- res
			} else {
				out <- res
			}
		}(n)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (str *findNodeStrategy) send(node *pendingNode) chan findXResult {
	out := make(chan findXResult)

	go func() {
		defer close(out)
		res, err := str.client.FindNode(node.Contact(), str.key)
		if err != nil {
			out <- findXResult{node, findXResultPayload{}, res, err}
			return
		}

		var fnr messages.FindNodeResponse
		res.ReadTimeout(str.roundTimeout)
		rerr := read(res, &fnr)
		out <- findXResult{
			node:     node,
			payload:  findXResultPayload{
				contacts: fnr.Payload,
			},
			response: res,
			err:      rerr,
		}

	}()

	return out
}

type findValueStrategy struct {
	valueReplyBuffer buffers.Buffer
	concurrency     int
	k               int
	dht             *dhtProxy
	client          *Client
	roundTimeout    time.Duration
	key gokad.ID
}

func (v *findValueStrategy) messageTypeId() int {
	return 2
}

func (v *findValueStrategy) setLookupKey(key gokad.ID) {
	v.key = key
}

func (v *findValueStrategy) round(nodes []*pendingNode, timeouts chan<- findXResult) chan findXResult {
	out := make(chan findXResult)
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(node *pendingNode) {
			defer wg.Done()
			res := <-v.send(node)
			if res.err != nil && res.err.Error() == buffers.TimeoutErr {
				timeouts <- res
			} else {
				out <- res
			}
		}(n)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (v *findValueStrategy) send(node *pendingNode) chan findXResult {
	out := make(chan findXResult)

	go func() {
		defer close(out)
		res, err := v.client.FindValue(node.Contact(), v.key.String())
		if err != nil {
			out <- findXResult{node, findXResultPayload{}, res, err}
			return
		}

		var fvr messages.FindValueResponse
		res.ReadTimeout(v.roundTimeout)
		rerr := read(res, &fvr)
		out <- findXResult{
			node:     node,
			payload:  findXResultPayload{
				key: fvr.Payload.Key,
				contacts: fvr.Payload.Contacts,
			},
			response: res,
			err:      rerr,
		}

	}()

	return out
}

func getKClosestNodes(m *treeMap, K int) []gokad.Contact {
	var index int
	out := make([]gokad.Contact, 0)
	m.Traverse(func(k gokad.Distance, v *pendingNode) bool {
		if index == K {
			return false
		}

		out = append(out, v.contact)

		return true
	})

	return out
}

// nextRound traverses m up to K pendingNodes and fills nodes with max pendingNodes
func nextRound(m *treeMap, max int, nodes []*pendingNode, K int) bool {
	var count int
	var index int
	m.Traverse(func(k gokad.Distance, node *pendingNode) bool {
		if index == K {
			return false
		}
		if !node.Queried() && count < max {
			node.SetQueried(true)
			nodes[count] = node
			count++
		}
		index++
		if count >= max {
			return false
		}

		return true
	})

	if count > 0 {
		return true
	}

	return false
}

func losers(in <-chan findXResult, mkey int) chan findXResult {
	out := make(chan findXResult)
	go func() {
		for res := range in {
			// Note: We now read without a timeout until
			// the buffer is closed and push responses into it
			go func() {
				var km messages.KademliaMessage
				switch mkey {
				case 1:
					km = &messages.FindNodeResponse{}
				case 2:
					km = &messages.FindValueResponse{}
				}

				err := read(res.response, km)
				var p findXResultPayload
				switch v := km.(type) {
				case *messages.FindValueResponse:
					p.key = v.Payload.Key
					p.contacts = v.Payload.Contacts
				case *messages.FindNodeResponse:
					p.contacts = v.Payload
				}
				out <- findXResult{
					node:     res.node,
					payload:  p,
					response: res.response,
					err:      err,
				}
			}()
		}
	}()

	return out
}



func read(res *response.Response, km messages.KademliaMessage) error {
	_, err := res.Read(km)
	return err
}

func mergeLosersAndRound(losers, round <-chan findXResult) chan findXResult {
	out := make(chan findXResult)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case res, ok := <-round:
				if !ok {
					return
				}
				out <- res
			case res := <-losers:
				out <- res
			}
		}
	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func trim(p []*pendingNode) []*pendingNode {
	out := make([]*pendingNode, 0)
	for _, n := range p {
		if n != nil {
			out = append(out, n)
		}
	}

	return out
}
