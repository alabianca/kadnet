package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"github.com/alabianca/kadnet/response"
)

type Client struct {
	ID gokad.ID
	Writer kadconn.KadWriter
	NodeReplyBuffer *buffers.NodeReplyBuffer
}

func (c *Client) FindNode(contact gokad.Contact, lookupID gokad.ID) (*response.Response, error) {
	fnr := messages.FindNodeRequest{
		SenderID:     c.ID.String()	,
		Payload:      lookupID.String(),
		RandomID:     gokad.GenerateRandomID().String(),
	}

	b, err := fnr.Bytes()
	if err != nil {
		return nil, err
	}

	req := request.New(contact, b)
	c.do(req)

	res := response.New(contact, fnr.RandomID, c.NodeReplyBuffer)
	res.SendPingReplyFunc = func(echoRandomID string) {
		pingRes := messages.PingResponse{SenderID: c.ID.String(), RandomID: gokad.GenerateRandomID().String(), EchoRandomID: echoRandomID }
		if b, err := pingRes.Bytes(); err == nil {
			c.Writer.Write(b, req.Address())
		}
	}

	return res, nil
}

func (c *Client) do(req *request.Request) {
	go c.Writer.Write(req.Body, req.Address())
}

