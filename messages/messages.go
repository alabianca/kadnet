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
	NodeLookup   = MessageType(30)
	FindNodeReq  = MessageType(20)
	FindNodeRes  = MessageType(21)
	PingReq      = MessageType(22)
	PingRes      = MessageType(23)
	FindValueReq = MessageType(24)
	FindValueRes = MessageType(25)
	StoreReq     = MessageType(26)
	StoreRes     = MessageType(27)
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
	case PingRes:
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

func IsValid(msgType MessageType) bool {
	return IsRequest(msgType) || IsResponse(msgType)
}

func IsResponse(msgType MessageType) bool {
	return msgType == FindNodeRes ||
		msgType == FindValueRes ||
		msgType == StoreRes
}

func IsRequest(msgType MessageType) bool {
	return msgType == FindNodeReq ||
		msgType == PingReq ||
		msgType == FindValueReq ||
		msgType == StoreReq ||
		msgType == PingRes
}

func SerializeID(id string) ([]byte, error) {
	return util.BytesFromHex(id)
}

func ToStringId(id []byte) string {
	return fmt.Sprintf("%x", id)
}
