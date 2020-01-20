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

type pendingNode struct {
	contact gokad.Contact
	answered bool
	queried bool
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
	node *pendingNode
	payload []gokad.Contact
	response *response.Response
	err error

}

func nextRound(m *treeMap, max int, nodes []*pendingNode) bool {
	var index int
	m.Traverse(func(k gokad.Distance, node *pendingNode) bool {
		if !node.Queried() && index < max {
			node.SetQueried(true)
			nodes[index] = node
			index++
		}

		if index >= max {
			return false
		}

		return true
	})

	if index > 0 {
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
			res := <- sendFindNodeRPC(node, rpc, lookupID, timeout)
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
