package kadconn

import (
	"errors"
	"github.com/alabianca/kadnet/messages"
	"net"
	"sync"
)

const ErrInvalidMessageType = "invalid message type"

type KadConn interface {
	KadReader
	KadWriter
	Close() error
}

type KadReader interface {
	Next() (messages.Message, net.Addr, error)
}

type KadWriter interface {
	Write(p []byte, addr net.Addr) (int, error)
}

func New(pc net.PacketConn) KadConn {
	c := conn{
		mtx: sync.Mutex{},
		pc: pc,
	}

	return &c
}

type conn struct {
	mtx sync.Mutex
	pc net.PacketConn

}

func (c *conn) Close() error {
	return c.pc.Close()
}

func (c *conn) Write(p []byte, addr net.Addr) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.write(p, addr)
}

func (c *conn) Next() (messages.Message, net.Addr, error) {
	buf := make([]byte, messages.FindNodeResSize) // this is the largest possible message
	n, r, err := c.read(buf)
	if err != nil {
		return nil, r, err
	}

	m, err := messages.Process(buf[:n])

	return m, r, err
}

func (c *conn) read(p []byte) (int, net.Addr, error) {
	n, r, err := c.pc.ReadFrom(p)
	if err != nil {
		return n, r, err
	}

	muxKey := messages.MessageType(p[0])
	if !messages.IsValid(muxKey) {
		return n, r, errors.New(ErrInvalidMessageType)
	}

	return n, r, err
}

func (c *conn) write(p []byte, addr net.Addr) (int, error) {
	var written int
	for written < len(p) {
		n, err := c.pc.WriteTo(p[written:], addr)
		if err != nil {
			return written, err
		}
		written += n
	}

	return written, nil
}