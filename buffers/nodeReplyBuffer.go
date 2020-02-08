package buffers

import (
	"errors"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"time"
)

const TimeoutErr = "timeout error"
const ClosedBufferErr = "closed buffer error"

type readQuery struct {
	id       string
	response chan messages.Message
	errc     chan error
}

type writeQuery struct {
	response chan int
	payload  messages.Message
	errc     chan error
}

type readWritePair struct {
	req readQuery
	res messages.Message
}

// NodeReplyBuffer stores FindNodeResponses to FindNodeRequests.
// NodeReplyBuffer implements the buffers.Buffer interface.
// When a response message is written, it is internally stored in a map
// Where the key is the id and echo random id combined.
type NodeReplyBuffer struct {
	active      bool
	waitTimeout time.Duration
	// channels
	newMessage chan messages.Message
	exit       chan bool
	readQuery  chan readQuery
	writeQuery chan writeQuery
	getMessage chan readQuery
	toReader   chan readWritePair
}

func NewNodeReplyBuffer() *NodeReplyBuffer {
	buf := NodeReplyBuffer{
		active:      false,
		waitTimeout: time.Second * 5,
		readQuery:   make(chan readQuery),
		writeQuery:  make(chan writeQuery),
		toReader:    make(chan readWritePair),
	}

	go buf.acceptReadQueries(buf.readQuery)
	go buf.acceptWrites(buf.writeQuery)
	go buf.process(buf.toReader)

	return &buf
}

func (n *NodeReplyBuffer) Open() {
	if n.IsOpen() {
		return
	}

	// open up the channels for processing read and write queries
	n.getMessage = make(chan readQuery)
	n.newMessage = make(chan messages.Message)
	n.exit = make(chan bool)

	n.active = true
	go n.accept()

}

func (n *NodeReplyBuffer) Close() {
	if !n.IsOpen() {
		return
	}
	n.active = false
	n.exit <- true
	n.getMessage = nil
	n.newMessage = nil
}

func (n *NodeReplyBuffer) IsOpen() bool {
	return n.active
}

func (n *NodeReplyBuffer) NewReader(id string) Reader {
	return &nodeReplyReader{
		query: n.readQuery,
		id:    id,
	}
}

func (n *NodeReplyBuffer) NewWriter() Writer {
	return &nodeReplyWriter{
		query: n.writeQuery,
	}
}

func (n *NodeReplyBuffer) accept() {
	pending := make(map[string]readQuery)
	buffer := make(map[string]messages.Message)

	for {

		var fanout chan<- readWritePair
		var next readWritePair
		if p, ok := nextPair(pending, buffer); ok {
			fanout = n.toReader
			next = p
		}

		select {
		case fanout <- next:
			delete(buffer, next.req.id)
			delete(pending, next.req.id)
		case m := <-n.newMessage:
			id, err := m.SenderID()
			if err != nil {
				continue
			}
			rid, err := m.EchoRandomID()
			if err != nil {
				continue
			}
			// combine sender id and echo random id to ensure reader will really get back the message we asked for
			key := id.String() + gokad.ID(rid).String()
			buffer[key] = m

		case sub := <-n.getMessage:
			pending[sub.id] = sub

		case <-n.exit:
			// We are closing the buffer. Loop over all pending readers and signal a closing of the buffer
			for _, v := range pending {
				v.errc <- errors.New(ClosedBufferErr)
				close(v.response)
			}
			return
		}
	}
}

func (n *NodeReplyBuffer) acceptReadQueries(query <-chan readQuery) {

	for q := range query {
		if !n.IsOpen() {
			q.errc <- errors.New(ClosedBufferErr)
		} else {
			n.getMessage <- q
		}
	}
}

func (n *NodeReplyBuffer) acceptWrites(query <-chan writeQuery) {
	for msg := range query {
		if !n.IsOpen() {
			msg.errc <- errors.New(ClosedBufferErr)
		} else {
			n.newMessage <- msg.payload
			msg.response <- len(msg.payload)
		}
	}
}

func (n *NodeReplyBuffer) process(pairs <-chan readWritePair) {
	for p := range pairs {
		p.req.response <- p.res
	}
}

func nextPair(req map[string]readQuery, buffer map[string]messages.Message) (readWritePair, bool) {
	for k, v := range req {
		if msg, ok := buffer[k]; ok {
			return readWritePair{
				req: v,
				res: msg,
			}, true
		}
	}

	return readWritePair{}, false
}

type nodeReplyReader struct {
	query        chan<- readQuery
	id           string
	readDeadline time.Duration
}

func (r *nodeReplyReader) Read(km messages.KademliaMessage) (int, error) {
	query := readQuery{
		r.id,
		make(chan messages.Message, 1), // it is important that these channels are buffered!
		make(chan error, 1)}
	r.query <- query
	var exit <-chan time.Time
	if r.readDeadline != EmptyTimeout {
		exit = time.After(r.readDeadline)
	}

	select {
	case msg := <-query.response:
		messages.ToKademliaMessage(msg, km)
		return len(msg), nil
	case err := <-query.errc:
		return 0, err
	case <-exit:
		return 0, errors.New(TimeoutErr)
	}

}

func (r *nodeReplyReader) SetDeadline(t time.Duration) {
	r.readDeadline = t
}

type nodeReplyWriter struct {
	query chan<- writeQuery
}

func (w *nodeReplyWriter) Write(msg messages.Message) (int, error) {
	query := writeQuery{
		payload:  msg,
		errc:     make(chan error, 1),
		response: make(chan int, 1),
	}

	w.query <- query

	select {
	case n := <-query.response:
		return n, nil
	case err := <-query.errc:
		return 0, err
	}
}
