package buffers

import (
	"errors"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"time"
)

const PingReplyNotFoundErr = "ping reply message not found"

type expectedPingReply struct {
	expiry  time.Time
	message messages.Message
}

type pingReplyCheck struct {
	id       string
	response chan messages.Message
	expiry   time.Time
}

type PingReplyBuffer struct {
	open       bool
	expected   chan messages.Message
	getMessage chan pingReplyCheck
	getFirst   chan chan messages.Message
	exit       chan bool
	expiry     time.Duration
}

func NewPingReplyBuffer() *PingReplyBuffer {
	buf := PingReplyBuffer{
		open:       false,
		expected:   make(chan messages.Message),
		getMessage: make(chan pingReplyCheck),
		getFirst:   make(chan chan messages.Message),
		exit:       make(chan bool),
		expiry:     time.Second * 5,
	}

	return &buf
}

func (b *PingReplyBuffer) Open() {
	if b.IsOpen() {
		return
	}

	b.open = true
	go b.accept()
}

func (b *PingReplyBuffer) Close() {
	if !b.IsOpen() {
		return
	}
	b.open = false
	b.exit <- true
}

func (b *PingReplyBuffer) IsOpen() bool {
	return b.open
}

func (b *PingReplyBuffer) First(km messages.KademliaMessage) {
	in := make(chan messages.Message, 1)
	b.getFirst <- in
	msg := <-in
	messages.ToKademliaMessage(msg, km)
}

func (b *PingReplyBuffer) NewReader(id string) Reader {
	return &pingReplyReader{
		get: b.getMessage,
		id:  id,
	}
}

func (b *PingReplyBuffer) NewWriter() Writer {
	return &pingReplyWriter{setExpected: b.expected}
}

func (b *PingReplyBuffer) accept() {
	buf := make(map[string]expectedPingReply)
	pending := make(map[string]pingReplyCheck)
	var purge <-chan time.Time
	var getFirst chan messages.Message
	for {
		if purge == nil {
			purge = time.After(time.Second * 60)
		}

		var fanout chan<- messages.Message
		var next messages.Message
		var nextKey string
		if getFirst != nil && len(buf) > 0 {
			fanout = getFirst
			nextKey, next = getFirstInBuf(buf)
		} else if key, ok := nextPingReply(buf, pending); ok {
			nextKey = key
			next = buf[key].message
			fanout = pending[key].response
		}

		select {
		case fanout <- next:
			delete(buf, nextKey)
			delete(pending, nextKey)
			getFirst = nil
		case <-b.exit:
			for k, _ := range buf {
				delete(buf, k)
			}
			return
		case mc := <-b.getFirst:
			getFirst = mc
		case check := <-b.getMessage:
			now := time.Now()
			check.expiry = now.Add(b.expiry)
			pending[check.id] = check

		// delete all buffered expected pingReplyMessages
		// that reached their expiry deadline
		// also delete all pending queries that reached their
		// expiry
		case <-purge:
			purge = nil
			now := time.Now()
			for k, v := range buf {
				if v.expiry.Before(now) {
					delete(buf, k)
				}
			}
			for k, v := range pending {
				if v.expiry.Before(now) {
					v.response <- nil
					delete(pending, k)
				}

			}
		// store an expected pingReplyMessage by their echoRandomID
		// with appropriate expiration
		case m := <-b.expected:
			er, err := m.EchoRandomID()
			if err != nil {
				continue
			}

			now := time.Now()
			buf[gokad.ID(er).String()] = expectedPingReply{
				expiry:  now.Add(b.expiry),
				message: m,
			}
		}
	}
}

func nextPingReply(buf map[string]expectedPingReply, pending map[string]pingReplyCheck) (string, bool) {
	for k, _ := range pending {
		if _, ok := buf[k]; ok {
			return k, ok
		}
	}

	return "", false
}

func getFirstInBuf(buf map[string]expectedPingReply) (string, messages.Message) {
	var key string
	var msg messages.Message
	for k, v := range buf {
		key = k
		msg = v.message
		break
	}

	return key, msg
}

type pingReplyReader struct {
	get          chan<- pingReplyCheck
	id           string
	readDeadline time.Duration
}

func (r *pingReplyReader) SetDeadline(t time.Duration) {
	r.readDeadline = t
}

func (r *pingReplyReader) Read(km messages.KademliaMessage) (int, error) {
	query := pingReplyCheck{
		id:       r.id,
		response: make(chan messages.Message, 1),
	}

	r.get <- query
	var exit <-chan time.Time
	if r.readDeadline != EmptyTimeout {
		exit = time.After(r.readDeadline)
	}

	select {
	case msg := <-query.response:
		if msg != nil {
			messages.ToKademliaMessage(msg, km)
			return len(msg), nil
		}
		return 0, errors.New(PingReplyNotFoundErr)
	case <-exit:
		return 0, errors.New(TimeoutErr)
	}
}

type pingReplyWriter struct {
	setExpected chan messages.Message
}

func (w *pingReplyWriter) Write(msg messages.Message) (int, error) {
	cpy := make(messages.Message, len(msg))
	copy(cpy, msg)
	w.setExpected <- cpy
	return len(cpy), nil
}
