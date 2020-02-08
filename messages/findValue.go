package messages

import (
	"errors"
	"github.com/alabianca/gokad"
)

type FindValueRequest struct {
	SenderID string
	Payload string
	EchoRandomID string
	RandomID string
}

func (r *FindValueRequest) MultiplexKey() MessageType {
	return FindValueReq
}

func (r *FindValueRequest) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(FindValueReq)

	sid, err := SerializeID(r.SenderID)
	if err != nil {
		return nil, err
	}

	pid, err := SerializeID(r.Payload)
	if err != nil {
		return nil, nil
	}

	rid, err := SerializeID(r.RandomID)
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

func (r *FindValueRequest) GetRandomID() string {
	return r.RandomID
}

func (r *FindValueRequest) GetSenderID() string {
	return r.SenderID
}

func (r *FindValueRequest) GetEchoRandomID() string {
	return r.EchoRandomID
}

type FindValueResponsePayload struct {
	Key string
	Contacts []gokad.Contact
}

type FindValueResponse struct {
	mkey MessageType
	SenderID string
	Payload FindValueResponsePayload
	RandomID string
	EchoRandomID string
}

func FindValueResponseOK() *FindValueResponse {
	r := new(FindValueResponse)
	r.mkey = FindValueResOK
	return r
}

func FindValueResponseNOK() *FindValueResponse {
	r := new(FindValueResponse)
	r.mkey = FindValueRes
	return r
}

func (f *FindValueResponse) MultiplexKey() MessageType {
	return f.mkey
}

func (f *FindValueResponse) GetSenderID() string {
	return f.SenderID
}

func (f *FindValueResponse) GetEchoRandomID() string {
	return f.EchoRandomID
}

func (f *FindValueResponse) GetRandomID() string {
	return f.RandomID
}

func (f *FindValueResponse) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(f.mkey)

	var key []byte
	var err error
	sid, err := SerializeID(f.SenderID)
	eid, err := SerializeID(f.EchoRandomID)
	rid, err := SerializeID(f.RandomID)

	if f.mkey == FindValueResOK {
		key, err = SerializeID(f.Payload.Key)
	}

	if err != nil {
		return nil, errors.New(ErrDeserializeErr)
	}

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, eid...)
	if len(key) > 0 {
		out = append(out, key...)
	}

	for _, c := range f.Payload.Contacts {
		out = append(out, c.Serialize()...)
	}

	out = append(out, rid...)

	return out, nil
}
