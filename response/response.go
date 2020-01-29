package response

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/messages"
	"net"
	"strconv"
	"time"
)

type Response struct {
	Contact gokad.Contact
	Body    buffers.Buffer
	// SendPingReplyFunc is called whenever a response is successfully
	// read in Response.Read
	SendPingReplyFunc func(echoRandomID string)
	matcher           string
	readTimeout       time.Duration
}

func New(c gokad.Contact, matcher string, buffer buffers.Buffer) *Response {
	return &Response{
		Contact: c,
		Body:    buffer,
		matcher: matcher,
	}
}

func (r *Response) Address() net.Addr {
	addr := net.JoinHostPort(r.Contact.IP.String(), strconv.Itoa(r.Contact.Port))
	updAddr, _ := net.ResolveUDPAddr("udp", addr)
	return updAddr
}

func (r *Response) Host() string {
	return r.Contact.ID.String()
}

func (r *Response) ReadTimeout(dur time.Duration) {
	r.readTimeout = dur
}

func (r *Response) Read(km messages.KademliaMessage) (int, error) {
	defer r.resetTimeout()
	reader := r.Body.NewReader(r.Contact.ID.String() + r.matcher)

	if r.readTimeout != time.Duration(0) {
		reader.SetDeadline(r.readTimeout)
	}
	n, err := reader.Read(km)

	if err == nil && r.SendPingReplyFunc != nil {
		r.SendPingReplyFunc(km.GetEchoRandomID())
	}

	return n, err
}

func (r *Response) resetTimeout() {
	r.readTimeout = time.Duration(0)
}
