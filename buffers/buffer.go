package buffers

import (
	"github.com/alabianca/kadnet/messages"
	"time"
)

const EmptyTimeout = time.Duration(0)

type Buffer interface {
	Open()
	Close()
	NewReader(id string) Reader
	NewWriter() Writer
}

type BufferReader interface {
	Read(id string, km messages.KademliaMessage, timeout time.Duration) (int, error)
}

type BufferWriter interface {
	Write(msg messages.Message) (int, error)
}

type Reader interface {
	Read(km messages.KademliaMessage) (int, error)
	SetDeadline(t time.Duration)
}

type Writer interface {
	Write(msg messages.Message) (int, error)
}
