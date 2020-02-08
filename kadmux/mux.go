package kadmux

import (
	"github.com/alabianca/kadnet/buffers"
	"net"

	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
)

const HandlerNotFoundErr = "Handler Not Found"
const NodeReplyBufferID = "NodeReplyBuffer"
const PingReplyBufferID = "PingReplyBuffer"
const StoreReplyBufferID = "StoreReplyBuffer"
const ValueReplyBufferID = "ValueReplyBuffer"

type Mux interface {
	Handle(c kadconn.KadConn) error
	HandleFunc(m messages.MessageType, handler RpcHandlerFunc)
	GetBuffer(keystring string) buffers.Buffer
	Use(middlewares ...func(handler RpcHandler) RpcHandler)
	Close()
}

type RemoteMessage interface {
	Host() string
	Address() net.Addr
}

type kadMux struct {
	conn        kadconn.KadConn
	handlers    map[messages.MessageType]RpcHandler
	middlewares []func(handler RpcHandler) RpcHandler
	dispatcher  *Dispatcher
	// channels
	dispatchRequest chan WorkRequest
	onResponse      chan messages.Message
	onRequest       chan *request.Request
	stopReceiver    chan chan error
	stopReply       chan chan error
	stopDispatcher  chan bool
	exit            chan error
	// buffers
	buffers map[string]buffers.Buffer
}

func NewMux() Mux {
	return &kadMux{
		conn:            nil,
		dispatcher:      NewDispatcher(10),
		stopDispatcher:  make(chan bool),
		dispatchRequest: make(chan WorkRequest),
		handlers:        make(map[messages.MessageType]RpcHandler),
		middlewares:     make([]func(handler RpcHandler) RpcHandler, 0),
		onRequest:       make(chan *request.Request),
		onResponse:      make(chan messages.Message),
		exit:            make(chan error),
		buffers: map[string]buffers.Buffer{
			NodeReplyBufferID:  buffers.NewNodeReplyBuffer(),
			PingReplyBufferID:  buffers.NewPingReplyBuffer(),
			StoreReplyBufferID: buffers.NewStoreReplyBuffer(),
			ValueReplyBufferID: buffers.NewNodeReplyBuffer(),
		},
	}
}

func (k *kadMux) Use(middlewares ...func(handler RpcHandler) RpcHandler) {
	k.middlewares = append(k.middlewares, middlewares...)
}

func (k *kadMux) GetBuffer(key string) buffers.Buffer {
	b, _ := k.buffers[key]
	return b
}

func (k *kadMux) Close() {
	if k.stopReply != nil && k.stopReceiver != nil {
		stopReply := make(chan error)
		stopRec := make(chan error)
		k.stopReply <- stopReply
		k.stopReceiver <- stopRec

		<-stopReply
		<-stopRec
		k.stopDispatcher <- true
		k.exit <- nil
		// close all buffers
		for _, v := range k.buffers {
			v.Close()
		}
	}
}

// Handle the connection and start the request dispatcher, receiverThread and replyThread
// We also open all buffers that remain open through the lifetime of the mux
func (k *kadMux) Handle(conn kadconn.KadConn) error {
	k.conn = conn
	k.startDispatcher(10) // @todo get max workers from somewhere else
	receiver := NewReceiverThread(k.onResponse, k.onRequest, k.conn)
	reply := NewReplyThread(k.onResponse, k.onRequest, k.conn)

	// store the buffers in the reply thread so we can buffer incoming responses
	// requests are not buffered and are handled by the dispatcher
	reply.SetBuffers(k.buffers)

	k.buffers[PingReplyBufferID].Open()

	k.stopReceiver = make(chan chan error)
	k.stopReply = make(chan chan error)

	go k.handleRequests()
	go receiver.Run(k.stopReceiver)
	go reply.Run(k.dispatchRequest, k.stopReply)

	return <-k.exit
}

func (k *kadMux) startDispatcher(max int) {
	for i := 0; i < max; i++ {
		w := NewWorker(i)
		k.dispatcher.Dispatch(w)
	}

	go k.dispatcher.Start()
}

func (k *kadMux) handleRequests() {
	queue := make([]*WorkRequest, 0)
	for {

		var fanout chan<- WorkRequest
		var next WorkRequest
		if len(queue) > 0 {
			fanout = k.dispatcher.QueueWork()
			next = *queue[0]
		}

		select {
		case <-k.stopDispatcher:
			k.dispatcher.Stop()
			return
		case fanout <- next:
			queue = queue[1:]
		case work := <-k.dispatchRequest:
			handler, ok := k.handlers[work.ArgRequest.MultiplexKey()]
			if !ok {
				continue
			}
			work.Handler = handler
			queue = append(queue, &work)
		}
	}
}

func (k *kadMux) HandleFunc(m messages.MessageType, handler RpcHandlerFunc) {

	k.handlers[m] = k.handle(handler)
}

func (k *kadMux) handle(handler RpcHandler) RpcHandler {
	if len(k.middlewares) == 0 {
		return handler
	}

	h := k.middlewares[len(k.middlewares)-1](handler)
	for i := len(k.middlewares) - 2; i >= 0; i-- {
		h = k.middlewares[i](h)
	}

	return h
}
