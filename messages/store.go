package messages

import (
	"encoding/binary"
	"github.com/alabianca/gokad"
)

type StoreRequestPayload struct {
	Key   gokad.ID
	Value gokad.Value
}

type StoreRequest struct {
	SenderID string
	RandomID string
	Payload  StoreRequestPayload
}

func (n *StoreRequest) MultiplexKey() MessageType {
	return StoreReq
}

func (n *StoreRequest) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(n.MultiplexKey())

	sid, err := SerializeID(n.SenderID)
	rid, err := SerializeID(n.RandomID)

	if err != nil {
		return nil, err
	}

	key := make([]byte, len(n.Payload.Key))
	copy(key, n.Payload.Key)

	port := make([]byte, 2)
	binary.BigEndian.PutUint16(port, uint16(n.Payload.Value.Port))

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, key...)
	out = append(out, port...)
	out = append(out, n.Payload.Value.Host...)
	out = append(out, rid...)

	return out, nil

}

func (n *StoreRequest) GetRandomID() string {
	return n.RandomID
}

func (n *StoreRequest) GetEchoRandomID() string {
	return ""
}

func (n *StoreRequest) GetSenderID() string {
	return n.SenderID
}

type StoreResponse struct {
	SenderID     string
	EchoRandomID string
	RandomID     string
}

func (s *StoreResponse) MultiplexKey() MessageType {
	return StoreRes
}

func (s *StoreResponse) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(s.MultiplexKey())

	sid, err := SerializeID(s.SenderID)
	eid, err := SerializeID(s.EchoRandomID)
	rid, err := SerializeID(s.RandomID)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, eid...)
	out = append(out, rid...)

	return out, nil
}

func (s *StoreResponse) GetSenderID() string {
	return s.SenderID
}

func (s *StoreResponse) GetRandomID() string {
	return s.RandomID
}

func (s *StoreResponse) GetEchoRandomID() string {
	return s.EchoRandomID
}
