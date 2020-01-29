package messages


type PingResponse struct {
	mkey MessageType
	SenderID string
	EchoRandomID string
	RandomID string
}

func (m *PingResponse) MultiplexKey() MessageType {
	return m.mkey
}

func (m *PingResponse) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(m.mkey)
	sid, err := SerializeID(m.SenderID)
	if err != nil {
		return nil, err
	}

	rid, err := SerializeID(m.RandomID)
	if err != nil {
		return nil, err
	}

	eid, err := SerializeID(m.EchoRandomID)
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

func (m *PingResponse) GetRandomID() string {
	return m.RandomID
}

func (m *PingResponse) GetSenderID() string {
	return m.SenderID
}

func (m *PingResponse) GetEchoRandomID() string {
	return m.EchoRandomID
}

func Implicit() *PingResponse {
	pr := new(PingResponse)
	pr.mkey = PingResImplicit
	return pr
}

func Explicit() *PingResponse {
	pr := new(PingResponse)
	pr.mkey = PingResExplicit
	return pr
}

type PingRequest struct {
	SenderID string
	RandomID string
}

func (p *PingRequest) MultiplexKey() MessageType {
	return PingReq
}

func (p *PingRequest) Bytes() ([]byte, error) {
	mkey := make([]byte, 1)
	mkey[0] = byte(p.MultiplexKey())

	sid, err := SerializeID(p.SenderID)
	if err != nil {
		return nil, err
	}

	rid, err := SerializeID(p.RandomID)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0)
	out = append(out, mkey...)
	out = append(out, sid...)
	out = append(out, rid...)

	return out, nil
}

func (p *PingRequest) GetRandomID() string {
	return p.RandomID
}

func (p *PingRequest) GetSenderID() string {
	return p.SenderID
}

func (p *PingRequest) GetEchoRandomID() string {
	return ""
}