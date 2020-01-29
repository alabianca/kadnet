package buffers

import (
	"errors"
	"github.com/alabianca/kadnet/messages"
	"sync"
	"time"
)

type storeWQuery struct {
	key  string
	msg  messages.Message
	errc chan error
}

type storeRQuery struct {
	key      string
	response chan messages.Message
	errc     chan error
}

type StoreReplyBuffer struct {
	open        bool
	store       chan storeWQuery
	read        chan storeRQuery
	acceptWrite chan storeWQuery
	acceptRead  chan storeRQuery
	exit        chan bool
	mtx         sync.Mutex
}

func NewStoreReplyBuffer() *StoreReplyBuffer {
	buf := StoreReplyBuffer{
		open:  false,
		store: make(chan storeWQuery),
		read:  make(chan storeRQuery),
		mtx:   sync.Mutex{},
	}

	go buf.acceptStoreQueries(buf.store)
	go buf.acceptReadQueries(buf.read)

	return &buf
}

func (s *StoreReplyBuffer) Open() {
	if s.IsOpen() {
		return
	}

	s.acceptWrite = make(chan storeWQuery)
	s.acceptRead = make(chan storeRQuery)
	s.exit = make(chan bool)

	go s.accept()

	s.open = true
}

func (s *StoreReplyBuffer) Close() {
	if !s.IsOpen() {
		return
	}

	s.open = false
	s.exit <- true
	s.exit = nil
	s.acceptRead = nil
	s.acceptWrite = nil
}

func (s *StoreReplyBuffer) IsOpen() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.open
}

func (s *StoreReplyBuffer) NewReader(id string) Reader {
	return &storeReplyReader{
		id:    id,
		query: s.read,
	}
}

func (s *StoreReplyBuffer) NewWriter() Writer {
	return &storeReplyWriter{query: s.store}
}

func (s *StoreReplyBuffer) acceptReadQueries(in <-chan storeRQuery) {
	for query := range in {
		if !s.IsOpen() {
			query.errc <- errors.New(ClosedBufferErr)
		} else {
			s.acceptRead <- query
		}
	}
}

func (s *StoreReplyBuffer) acceptStoreQueries(in <-chan storeWQuery) {
	for query := range in {
		if !s.IsOpen() {
			query.errc <- errors.New(ClosedBufferErr)
		} else {
			s.acceptWrite <- query
			query.errc <- nil
		}
	}
}

func (s *StoreReplyBuffer) accept() {
	buf := make(map[string]messages.Message)
	pending := make(map[string]storeRQuery)

	for {

		var fanout chan<- messages.Message
		var next messages.Message
		var nextKey string

		if key, ok := nextStoreMessage(buf, pending); ok {
			next = buf[key]
			nextKey = key
			fanout = pending[key].response
		}

		select {
		case <-s.exit:
			for _, v := range pending {
				v.errc <- errors.New(ClosedBufferErr)
			}
			return
		case fanout <- next:
			delete(buf, nextKey)
			delete(pending, nextKey)
		case query := <-s.acceptWrite:
			buf[query.key] = query.msg
		case query := <-s.acceptRead:
			pending[query.key] = query
		}
	}
}

func nextStoreMessage(buf map[string]messages.Message, pending map[string]storeRQuery) (key string, ok bool) {
	for k, _ := range pending {
		if _, kk := buf[k]; kk {
			key = k
			ok = true
			return
		}
	}

	return
}

type storeReplyReader struct {
	id       string
	query    chan storeRQuery
	deadline time.Duration
}

func (r *storeReplyReader) SetDeadline(t time.Duration) {
	r.deadline = t
}

func (r *storeReplyReader) Read(km messages.KademliaMessage) (int, error) {
	query := storeRQuery{
		key:      r.id,
		response: make(chan messages.Message, 1),
		errc:     make(chan error, 1),
	}

	r.query <- query

	var exit <-chan time.Time
	if r.deadline != EmptyTimeout {
		exit = time.After(r.deadline)
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

type storeReplyWriter struct {
	query chan<- storeWQuery
}

func (w *storeReplyWriter) Write(msg messages.Message) (int, error) {
	sid, _ := msg.SenderID()
	query := storeWQuery{
		key:  sid.String(),
		msg:  msg,
		errc: make(chan error, 1),
	}

	w.query <- query

	err := <-query.errc
	if err != nil {
		return 0, err
	}

	return len(msg), nil
}
