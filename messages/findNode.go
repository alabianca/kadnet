package messages

import (
	"github.com/alabianca/gokad"
)

type FindNodeResponse struct {
	SenderID     string
	EchoRandomID string
	Payload      []gokad.Contact
	RandomID     string
}

func (n *FindNodeResponse) MultiplexKey() MessageType {
	return FindNodeRes
}

func (n *FindNodeResponse) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(FindNodeRes)
	sid, err := SerializeID(n.SenderID)
	if err != nil {
		return nil, err
	}

	eid, err := SerializeID(n.EchoRandomID)
	if err != nil {
		return nil, err
	}

	rid, err := SerializeID(n.RandomID)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, eid...)
	for _, c := range n.Payload {
		ser := c.Serialize()

		out = append(out, ser...)
	}

	out = append(out, rid...)

	return out, nil
}

func (n *FindNodeResponse) GetRandomID() string {
	return n.RandomID
}

func (n *FindNodeResponse) GetSenderID() string {
	return n.SenderID
}

func (n *FindNodeResponse) GetEchoRandomID() string {
	return n.EchoRandomID
}

// FIND NODE REQUEST

type FindNodeRequest struct {
	SenderID     string
	EchoRandomID string
	Payload      string
	RandomID     string
}

func NewFindNodeRequest(sID, eID, payload string) *FindNodeRequest {
	rID := gokad.GenerateRandomID().String()
	if eID == "" {
		eID = rID
	}

	return &FindNodeRequest{
		SenderID:     sID,
		EchoRandomID: eID,
		Payload:      payload,
		RandomID:     rID,
	}
}

func (n *FindNodeRequest) MultiplexKey() MessageType {
	return FindNodeReq
}

func (n *FindNodeRequest) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(FindNodeReq)

	sid, err := SerializeID(n.SenderID)
	if err != nil {
		return nil, err
	}

	pid, err := SerializeID(n.Payload)
	if err != nil {
		return nil, nil
	}

	rid, err := SerializeID(n.RandomID)
	if err != nil {
		return nil, nil
	}

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, pid...)
	out = append(out, rid...)

	return out, nil

}

func (n *FindNodeRequest) GetRandomID() string {
	return n.RandomID
}

func (n *FindNodeRequest) GetSenderID() string {
	return n.SenderID
}

func (n *FindNodeRequest) GetEchoRandomID() string {
	return n.EchoRandomID
}
