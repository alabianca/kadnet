package kadmux

import (
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/request"
)

type WorkRequest struct {
	Handler    RpcHandler
	ArgConn    kadconn.KadWriter
	ArgRequest *request.Request
}

type Worker struct {
	id      int
	Work    chan WorkRequest
	Workers chan chan WorkRequest
	exit    chan bool
}

func NewWorker(id int) *Worker {
	return &Worker{
		id:   id,
		Work: make(chan WorkRequest),
		exit: make(chan bool),
	}
}

func (w *Worker) Start(queue chan chan WorkRequest) {

	for {

		queue <- w.Work

		select {
		case work := <-w.Work:
			work.Handler.Handle(work.ArgConn, work.ArgRequest)
		case <-w.exit:
			return

		}
	}

}

func (w *Worker) Stop() {
	w.exit <- true
}
