package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"github.com/alabianca/kadnet/response"
	"net"
)

type Client struct {
	ID               gokad.ID
	Writer           kadconn.KadWriter
	NodeReplyBuffer  buffers.Buffer
	PingReplyBuffer  buffers.Buffer
	StoreReplyBuffer buffers.Buffer
}

func (c *Client) FindNode(contact gokad.Contact, lookupID gokad.ID) (*response.Response, error) {
	fnr := messages.FindNodeRequest{
		SenderID: c.ID.String(),
		Payload:  lookupID.String(),
		RandomID: gokad.GenerateRandomID().String(),
	}

	b, err := fnr.Bytes()
	if err != nil {
		return nil, err
	}

	req := request.New(contact, b)
	c.do(req)

	res := response.New(contact, fnr.RandomID, c.NodeReplyBuffer)
	// When the response is successfully read, send the appropriate implicit PingReply
	res.SendPingReplyFunc = c.implicitPingReplyFunc(req.Address())

	return res, nil
}

func (c *Client) Ping(contact gokad.Contact) (*response.Response, error) {
	ping := messages.PingRequest{
		SenderID: c.ID.String(),
		RandomID: gokad.GenerateRandomID().String(),
	}

	b, err := ping.Bytes()
	if err != nil {
		return nil, err
	}

	req := request.New(contact, b)
	c.do(req)

	res := response.New(contact, "", c.PingReplyBuffer)
	res.SendPingReplyFunc = c.implicitPingReplyFunc(req.Address())

	return res, nil
}

func (c *Client) Store(contact gokad.Contact, key gokad.ID, value gokad.Value) (*response.Response, error) {
	store := messages.StoreRequest{
		SenderID: c.ID.String(),
		RandomID: gokad.GenerateRandomID().String(),
		Payload:  messages.StoreRequestPayload{
			Key: key,
			Value: value,
		},
	}

	b, err := store.Bytes()
	if err != nil {
		return nil, err
	}

	req := request.New(contact, b)
	c.do(req)

	res := response.New(contact, "", c.StoreReplyBuffer)
	res.SendPingReplyFunc = c.implicitPingReplyFunc(req.Address())

	return res, nil
}

func (c *Client) implicitPingReplyFunc(address net.Addr) func(echoRandomID string) {
	return func(echoRandomID string) {
		pingRes := messages.Implicit()
		pingRes.SenderID = c.ID.String()
		pingRes.RandomID = gokad.GenerateRandomID().String()
		pingRes.EchoRandomID = echoRandomID
		if b, err := pingRes.Bytes(); err == nil {
			c.Writer.Write(b, address)
		}
	}
}

func (c *Client) do(req *request.Request) {
	go c.Writer.Write(req.Body, req.Address())
}
