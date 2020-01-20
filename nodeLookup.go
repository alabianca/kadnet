package kadnet

import (
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

type NodeLookupConfig func(n *NodeLookup)

type PendingNode interface {
	Contact() gokad.Contact
	Queried() bool
	Answered() bool
	SetAnswered(x bool)
	SetQueried(x bool)
}

type XLookup interface {
	Concurrency() int
	Key() gokad.ID
	Buffer() *TreeMap
	MaxConcurrency() int
	Round(nodes []PendingNode) chan FindNodeResult
	Losers() chan FindNodeResult
	NextRound(concurrency int, next []PendingNode) bool
	ReplyBuffer() buffers.Buffer
}



type FindNodeResult struct {
	node     PendingNode
	payload  []gokad.Contact
	response *response.Response
	err      error
}

type enquiredNode struct {
	contact  gokad.Contact
	queried  bool
	answered bool
}

func (e *enquiredNode) Contact() gokad.Contact {
	return e.contact
}
func (e *enquiredNode) Queried() bool {
	return e.queried
}
func (e *enquiredNode) Answered() bool {
	return e.answered
}
func (e *enquiredNode) SetQueried(x bool) {
	e.queried = x
}
func (e *enquiredNode) SetAnswered(x bool) {
	e.answered = x
}


type NodeLookup struct {
	LookupID      gokad.ID
	Alpha         []gokad.Contact
	Rpc           RPC
	NodeReplyBuffer buffers.Buffer
	// Optionals -> will have a default
	RoundTimeout  time.Duration
	TimedOutNodes chan FindNodeResult
	K             int
	RoundConcurrency   int
	pendingNodes  *TreeMap
}

func NewNodeLookup(config ...NodeLookupConfig) *NodeLookup {
	n := NodeLookup{
		RoundTimeout:  time.Second * 3,
		TimedOutNodes: make(chan FindNodeResult),
		K:             20,
		RoundConcurrency: 3,
		pendingNodes:  NewMap(compareDistance),
	}

	for _, c := range config {
		c(&n)
	}

	for _, contact := range n.Alpha {
		delta := n.LookupID.DistanceTo(contact.ID)
		n.pendingNodes.Insert(delta, &enquiredNode{contact: contact})
	}

	return &n
}

func (n *NodeLookup) Concurrency() int {
	return n.RoundConcurrency
}

func (n *NodeLookup) Key() gokad.ID {
	return n.LookupID
}

func (n *NodeLookup) Buffer() *TreeMap {
	return n.pendingNodes
}

func (n *NodeLookup) MaxConcurrency() int {
	return n.K
}

func (n *NodeLookup) Round(nodes []PendingNode) chan FindNodeResult {
	out := make(chan FindNodeResult)
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, nd := range nodes {
		go func(node PendingNode) {
			defer wg.Done()
			res := <-n.sendFindNodeRPC(node)
			// if it timed out add them to the losers and consider them if the response arrives later
			if res.err != nil && res.err.Error() == buffers.TimeoutErr {
				n.TimedOutNodes <- res
			} else {
				out <- res
			}
		}(nd)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (n *NodeLookup) Losers() chan FindNodeResult {
	out := make(chan FindNodeResult)
	go func() {
		for res := range n.TimedOutNodes {
			// Note: We now read without a timeout until the buffer is closed and push
			// responses into out
			go n.read(res.node, res.response, out)
		}
	}()
	return out
}

func (n *NodeLookup) NextRound(concurrency int, nodes []PendingNode) bool {
	var index int
	n.pendingNodes.Traverse(func(k gokad.Distance, node PendingNode) bool {
		if !node.Queried() && (index < concurrency) {
			node.SetQueried(true)
			nodes[index] = node
			index++
		}

		if index >= concurrency {
			return false
		}

		return true
	})

	if index > 0 {
		return true
	}

	return false
}

func (n *NodeLookup) ReplyBuffer() buffers.Buffer {
	return n.NodeReplyBuffer
}

func (n *NodeLookup) sendFindNodeRPC(node PendingNode) chan FindNodeResult {
	out := make(chan FindNodeResult)
	go func() {
		defer close(out)
		res, err := n.Rpc.FindNode(node.Contact(), n.LookupID)
		if err != nil {
			out <- FindNodeResult{node, nil, res, err}
			return
		}

		res.ReadTimeout(n.RoundTimeout)
		n.read(node, res, out)
	}()

	return out
}

func (n *NodeLookup) read(node PendingNode, res *response.Response, out chan<- FindNodeResult) {
	var fnr messages.FindNodeResponse
	if _, err := res.Read(&fnr); err != nil {
		out <- FindNodeResult{node, nil, res, err}
	} else {
		out <- FindNodeResult{node, fnr.Payload, res, nil}
	}
}

func mergeLosersAndRound(losers, round <-chan FindNodeResult) chan FindNodeResult {
	out := make(chan FindNodeResult)
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

func Lookup(x XLookup) []gokad.Contact {
	nodeReplyBuffer := x.ReplyBuffer()
	nodeReplyBuffer.Open()
	defer nodeReplyBuffer.Close()
	lateReplies := x.Losers()
	concurrency := x.Concurrency()
	buffer := x.Buffer()
	key := x.Key()
	maxC := x.MaxConcurrency()

	next := make([]PendingNode, concurrency)
	for x.NextRound(concurrency, next) {
		rc := x.Round(trim(next))

		var atLeastOneNewNode bool
		for cs := range mergeLosersAndRound(lateReplies, rc) {
			if cs.err != nil {
				continue
			}

			cs.node.SetAnswered(true)
			for _, c := range cs.payload {
				delta := key.DistanceTo(c.ID)
				if _, ok := buffer.Get(delta); !ok {
					atLeastOneNewNode = true
					buffer.Insert(delta, &enquiredNode{contact: c})
				}
			}
		}

		// if a round did not reveal at least one new node we take all K closest
		// nodes not already queried and send them FIND_NODE_RPC's
		if !atLeastOneNewNode {
			concurrency = maxC
			next = make([]PendingNode, maxC)
		} else {
			x.Concurrency()
			next = make([]PendingNode, x.Concurrency())
		}
	}

	out := make([]gokad.Contact, maxC)
	var index int
	buffer.Traverse(func(k gokad.Distance, v PendingNode) bool {
		if v.Answered() {
			out[index] = v.Contact()
			index++
		}
		if index >= maxC {
			return false
		}
		return true
	})

	return out[:index]
}

func trim(p []PendingNode) []PendingNode {
	out := make([]PendingNode, 0)
	for _, n := range p {
		if n != nil {
			out = append(out, n)
		}
	}

	return out
}
