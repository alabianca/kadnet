package kadmux

import (
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
)

type ReplyThread struct {
	onResponse <-chan messages.Message
	onRequest  <-chan *request.Request
	writer     kadconn.KadWriter
	// buffers
	nodeReplyBuffer *buffers.NodeReplyBuffer
}

func NewReplyThread(res chan messages.Message, req <-chan *request.Request, writer kadconn.KadWriter) *ReplyThread {
	return &ReplyThread{
		onRequest:       req,
		onResponse:      res,
		writer:          writer,
	}
}

func (r *ReplyThread) SetBuffers(bf ...buffers.Buffer) {
	for _, buf := range bf {
		switch v := buf.(type) {
		case *buffers.NodeReplyBuffer:
			r.nodeReplyBuffer = v
		}
	}
}

func (r *ReplyThread) Run(newWork chan<- WorkRequest, exit <-chan chan error) {
	queue := make([]*request.Request, 0)

	for {

		var next WorkRequest
		var fanout chan<- WorkRequest
		if len(queue) > 0 {
			next = r.newWorkRequest(queue[0])
			fanout = newWork
		}

		select {
		case msg := <-r.onResponse:
			r.tempStoreMsg(msg)
		case out := <-exit:
			out <- nil
			return
		case req := <-r.onRequest:
			queue = append(queue, req)

		case fanout <- next:
			queue = queue[1:]

		}
	}
}

func (r *ReplyThread) tempStoreMsg(km messages.Message) {
	key, _ := km.MultiplexKey()
	buf := r.getBuffer(key)

	writer := buf.NewWriter()
	writer.Write(km)
}

func (r *ReplyThread) getBuffer(key messages.MessageType) buffers.Buffer {
	var buf buffers.Buffer
	switch key {
	case messages.FindNodeRes:
		buf = r.nodeReplyBuffer
	}

	return buf
}

func (r *ReplyThread) newWorkRequest(req *request.Request) WorkRequest {
	wReq := WorkRequest{
		ArgConn:    r.writer,
		ArgRequest: req,
	}

	return wReq
}
