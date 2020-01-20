package messages

import (
	"errors"
	"github.com/alabianca/gokad"
)

const (
	ErrInvalidMessage = "invalid Message"
)

type Message []byte

func (m Message) MultiplexKey() (MessageType, error) {
	if len(m) < 1 {
		return MessageType(0), invalidMessage()
	}
	return MessageType(m[0]), nil
}

func (m Message) SenderID() (gokad.ID, error) {
	if len(m) < 21 {
		return nil, invalidMessage()
	}

	out := make([]byte, 20)
	copy(out, m[1:21])

	return gokad.ID(out), nil
}

func (m Message) EchoRandomID() ([]byte, error) {
	key, _ := m.MultiplexKey()
	// a request does not have an echoRandomID
	if IsRequest(key) {
		return []byte{}, nil
	}

	if len(m) < 41 {
		return nil, invalidMessage()
	}

	out := make([]byte, 20)
	copy(out, m[21:42])

	return out, nil
}

func (m Message) RandomID() ([]byte, error) {
	l := len(m)
	if l < 41 {
		return nil, invalidMessage()
	}

	out := make([]byte, 20)
	copy(out, m[l-20:])

	return out, nil
}

func (m Message) Payload() ([]byte, error) {
	var startOfPayload int
	key, _ := m.MultiplexKey()
	l := len(m)
	isRes := IsResponse(key)

	if isRes {
		startOfPayload = 41
	} else {
		startOfPayload = 21
	}

	if isRes && l < (startOfPayload+20) {
		return nil, invalidMessage()
	}

	if l < (startOfPayload + 20) {
		return nil, invalidMessage()
	}

	// payload range is from startOfPayload (index 21 or 41) to startOf randomID
	endOfPayload := l - 20
	if endOfPayload < startOfPayload {
		return nil, invalidMessage()
	}

	out := make([]byte, (endOfPayload - startOfPayload))
	copy(out, m[startOfPayload:endOfPayload])

	return out, nil

}

func invalidMessage() error {
	return errors.New(ErrInvalidMessage)
}
