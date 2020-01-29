package messages

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/util"
	"math"
	"net"
)

type MessageType int

type KademliaMessage interface {
	MultiplexKey() MessageType
	Bytes() ([]byte, error)
	GetRandomID() string
	GetSenderID() string
	GetEchoRandomID() string
}

const (
	// Message Types
	FindNodeReq     = MessageType(20)
	FindNodeRes     = MessageType(21)
	PingReq         = MessageType(22)
	PingResImplicit = MessageType(23) // sent automatically after reading every response to a request
	PingResExplicit = MessageType(24) // sent explicitly as a response to a PingRequest
	FindValueReq    = MessageType(25)
	FindValueRes    = MessageType(26)
	StoreReq        = MessageType(27)
	StoreRes        = MessageType(28)
	// Message Sizes
	PingReqSize      = 41
	PingResSize      = 41
	PingReqResSize   = 61
	FindNodeReqSize  = 61
	FindValueReqSize = 61
	StoreReqSize     = 79
	FindNodeResSize  = 841
	FindValueResSize = 441 // Note: Assumes there are always k values in the payload
)

// Errors
const ErrNoMatchMessageSize = "byte length does not match message size"
const ErrContactsMalformed = "contacts malformed. should be an integer"

func GetMessageSize(x MessageType) int {
	var size int
	switch x {
	case FindNodeReq:
		size = FindNodeReqSize
	case FindNodeRes:
		size = FindNodeResSize
	case FindValueReq:
		size = FindValueReqSize
	case FindValueRes:
		size = FindValueResSize
	case PingReq:
		size = PingReqSize
	case PingResImplicit:
		size = PingResSize
	case StoreReq:
		size = StoreReqSize

	}

	return size
}

// Process - General structure of a message
// <-  1 Bytes    <- 20 Bytes  <- 20 Bytes       <- X Bytes  <- 20 Bytes
//  MultiplexKey      SenderID    EchoedRandomId    Payload     RandomID
func Process(raw []byte) (Message, error) {
	message := Message(raw)
	if _, err := message.MultiplexKey(); err != nil {
		return nil, err
	}

	return message, nil
}

func ToKademliaMessage(msg Message, km KademliaMessage) {

	rid, _ := msg.RandomID()
	eid, _ := msg.EchoRandomID()
	sid, _ := msg.SenderID()
	p, _ := msg.Payload()

	switch v := km.(type) {
	case *StoreResponse:
		*v = StoreResponse{
			SenderID:     ToStringId(sid),
			EchoRandomID: ToStringId(eid),
			RandomID:     ToStringId(rid),
		}
	case *StoreRequest:
		if p, err := parseStoreRequestPayload(p); err == nil {
			*v = StoreRequest{
				SenderID: ToStringId(sid),
				RandomID: ToStringId(rid),
				Payload:  p,
			}
		}
	case *PingRequest:
		*v = PingRequest{
			SenderID: ToStringId(sid),
			RandomID: ToStringId(rid),
		}
	case *PingResponse:
		*v = PingResponse{
			SenderID:     ToStringId(sid),
			EchoRandomID: ToStringId(eid),
			RandomID:     ToStringId(rid),
		}
	case *FindNodeRequest:
		*v = FindNodeRequest{
			RandomID:     ToStringId(rid),
			EchoRandomID: ToStringId(eid),
			SenderID:     ToStringId(sid),
			Payload:      ToStringId(p),
		}
	case *FindNodeResponse:
		if c, err := processContacts(p); err == nil {
			*v = FindNodeResponse{
				SenderID:     ToStringId(sid),
				EchoRandomID: ToStringId(eid),
				Payload:      c,
				RandomID:     ToStringId(rid),
			}
		}

	}

}

// every contact is 38 bytes long.
// split the raw bytes in chuncks of 38 bytes.
func processContacts(raw []byte) ([]gokad.Contact, error) {
	offset := 0
	cLen := 38
	l := float64(len(raw))
	x := float64(cLen)
	numContacts := l / x
	// if numContacts if not a flat integer we have some malformed payload.
	if numContacts != math.Trunc(numContacts) {
		return nil, errors.New(ErrContactsMalformed)
	}

	out := make([]gokad.Contact, int(numContacts))

	insert := 0
	for offset < int(l) {
		contact, err := toContact(raw[offset : offset+cLen])
		if err == nil {
			out[insert] = contact
			insert++
		}
		offset += cLen
	}

	return out, nil
}

func toContact(b []byte) (gokad.Contact, error) {
	l := len(b)
	if l == 0 {
		return gokad.Contact{}, errors.New("Empty Contact")
	}

	idOffset := 0
	portOffset := 20
	ipOffset := 22

	if l <= ipOffset {
		return gokad.Contact{}, errors.New("Malformed Contact")
	}

	idBytes := b[idOffset:portOffset]
	portBytes := b[portOffset:ipOffset]
	ipBytes := b[ipOffset:len(b)]
	ip := net.IP(ipBytes)

	port := binary.BigEndian.Uint16(portBytes)
	id, err := gokad.From(ToStringId(idBytes))
	if err != nil {
		return gokad.Contact{}, errors.New("Invalid ID")
	}

	c := gokad.Contact{
		ID:   id,
		IP:   ip,
		Port: int(port),
	}

	return c, nil

}

func parseStoreRequestPayload(b []byte) (StoreRequestPayload, error) {
	length := 38
	if len(b) != length {
		return StoreRequestPayload{}, errors.New("malformed Store Request")
	}

	// we can use toContact here as the payload uses the same structure
	c, err := toContact(b)
	if err != nil {
		return StoreRequestPayload{}, errors.New("malformed Store Request")
	}

	return StoreRequestPayload{Key: c.ID, Value: gokad.Value{Host: c.IP, Port: c.Port}}, nil
}

func IsValid(msgType MessageType) bool {
	return IsRequest(msgType) || IsResponse(msgType)
}

func IsResponse(msgType MessageType) bool {
	return msgType == FindNodeRes ||
		msgType == FindValueRes ||
		msgType == StoreRes ||
		msgType == PingResExplicit
}

func IsRequest(msgType MessageType) bool {
	return msgType == FindNodeReq ||
		msgType == PingReq ||
		msgType == FindValueReq ||
		msgType == StoreReq ||
		msgType == PingResImplicit
}

func SerializeID(id string) ([]byte, error) {
	return util.BytesFromHex(id)
}

func ToStringId(id []byte) string {
	return fmt.Sprintf("%x", id)
}
