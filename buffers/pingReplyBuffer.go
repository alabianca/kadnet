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
}

type PingReplyBuffer struct {
	open       bool
	expected   chan messages.Message
	getMessage chan pingReplyCheck
	exit       chan bool
	expiry     time.Duration
}

func NewPingReplyBuffer() *PingReplyBuffer {
	buf := PingReplyBuffer{
		open:       false,
		expected:   make(chan messages.Message),
		getMessage: make(chan pingReplyCheck),
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
	var purge <-chan time.Time
	for {
		if purge == nil {
			purge = time.After(time.Second * 3)
		}

		select {
		case <-b.exit:
			for k, _ := range buf {
				delete(buf, k)
			}
			return
		case check := <-b.getMessage:
			msg, _ := buf[check.id]
			check.response <- msg.message
			delete(buf, check.id)
		// delete all buffered expected pingReplyMessages
		// that reached their expiry deadline
		case <-purge:
			purge = nil
			now := time.Now()
			for k, v := range buf {
				if v.expiry.Before(now) {
					delete(buf, k)
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

type pingReplyReader struct {
	get chan <- pingReplyCheck
	id string
}

func (r *pingReplyReader) SetDeadline(t time.Duration) {}

func (r *pingReplyReader) Read(km messages.KademliaMessage) (int, error) {
	query := pingReplyCheck{
		id:       r.id,
		response: make(chan messages.Message, 1),
	}

	r.get <- query

	res := <- query.response
	if res == nil {
		return 0, errors.New(PingReplyNotFoundErr)
	}

	messages.ToKademliaMessage(res, km)
	return len(res), nil
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
