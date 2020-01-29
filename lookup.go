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

type lookup struct {
	nodeReplyBuffer buffers.Buffer
	concurrency     int
	k               int
	dht             *dhtProxy
	client          *Client
	roundTimeout    time.Duration
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
	if lp.nodeReplyBuffer == nil {
		return nil, errors.New("node reply buffer not specified")
	}

	if lp.client == nil {
		return nil, errors.New("client not specified")
	}

	return &lp, nil
}

func (l *lookup) do(id gokad.ID) ([]gokad.Contact, error) {
	nodeReplyBuffer := l.nodeReplyBuffer
	if nodeReplyBuffer == nil {
		return nil, errors.New("cannot open Node Reply Buffer <nil>")
	}
	// the nodeReplyBuffer must be opened
	// once opened it is ready to receive answers to FIND_NODE_RPC's
	nodeReplyBuffer.Open()
	defer nodeReplyBuffer.Close()

	concurrency := l.concurrency
	closestNodes := newMap(compareDistance)
	for _, c := range l.dht.getAlphaNodes(concurrency, id) {
		closestNodes.Insert(id.DistanceTo(c.ID), &pendingNode{contact: c})
	}

	client := l.client
	timedOutNodes := make(chan findNodeResult)
	lateReplies := losers(timedOutNodes)
	next := make([]*pendingNode, concurrency)
	for nextRound(closestNodes, concurrency, next, l.k) {
		rc := round(
			trim(next),
			client,
			id,
			l.roundTimeout,
			timedOutNodes,
		)

		var atLeastOneNewNode bool
		for cs := range mergeLosersAndRound(lateReplies, rc) {
			if cs.err != nil {
				continue
			}

			cs.node.SetAnswered(true)
			for _, c := range cs.payload {
				distance := id.DistanceTo(c.ID)
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

	return getKClosestNodes(closestNodes, l.k), nil
}

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

type findNodeResult struct {
	node     *pendingNode
	payload  []gokad.Contact
	response *response.Response
	err      error
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

func losers(in <-chan findNodeResult) chan findNodeResult {
	out := make(chan findNodeResult)
	go func() {
		for res := range in {
			// Note: We now read without a tiemout until
			// the buffer is closed and push responses into it
			go read(res.node, res.response, out)
		}
	}()

	return out
}

func round(nodes []*pendingNode, rpc RPC, lookupID gokad.ID, timeout time.Duration, timeouts chan<- findNodeResult) chan findNodeResult {
	out := make(chan findNodeResult)
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(node *pendingNode) {
			defer wg.Done()
			res := <-sendFindNodeRPC(node, rpc, lookupID, timeout)
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

func sendFindNodeRPC(node *pendingNode, rpc RPC, lookupID gokad.ID, timeout time.Duration) chan findNodeResult {
	out := make(chan findNodeResult)
	go func() {
		defer close(out)
		res, err := rpc.FindNode(node.Contact(), lookupID)
		if err != nil {
			out <- findNodeResult{node, nil, res, err}
			return
		}

		res.ReadTimeout(timeout)
		read(node, res, out)

	}()

	return out
}

func read(node *pendingNode, res *response.Response, out chan<- findNodeResult) {
	var fnr messages.FindNodeResponse
	if _, err := res.Read(&fnr); err != nil {
		out <- findNodeResult{node, nil, res, err}
	} else {
		out <- findNodeResult{node, fnr.Payload, res, nil}
	}
}

func mergeLosersAndRound(losers, round <-chan findNodeResult) chan findNodeResult {
	out := make(chan findNodeResult)
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
