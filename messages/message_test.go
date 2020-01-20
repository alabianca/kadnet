package messages_test

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"net"
	"reflect"
	"testing"
)

func TestMessageX_FindNodeRequest(t *testing.T) {
	fnr := messages.FindNodeRequest{
		SenderID: "8bc8082329609092bf86dea25cf7784cd708cc5d",
		Payload:  "8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a",
		RandomID: "28f787e3b60f99fb29b14266c40b536d6037307e",
	}

	b, _ := fnr.Bytes()
	msg := messages.Message(b)

	if key, _ := msg.MultiplexKey(); key != messages.FindNodeReq {
		t.Fatalf("Expected Multiplex Key to be %d, but got %d\n", messages.FindNodeReq, key)
	}

	sender, _ := gokad.From("8bc8082329609092bf86dea25cf7784cd708cc5d")
	payload, _ := gokad.From("8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a")
	random, _ := gokad.From("28f787e3b60f99fb29b14266c40b536d6037307e")

	if senderID, _ := msg.SenderID(); !reflect.DeepEqual(senderID, sender) {
		t.Fatalf("Expected senderID to equal %s, but got %s\n", sender, senderID)
	}

	if p, _ := msg.Payload(); !reflect.DeepEqual(p, []byte(payload)) {
		t.Fatalf("Expected payload to equal %s, but got %s\n", payload, p)
	}

	if r, _ := msg.RandomID(); !reflect.DeepEqual(r, []byte(random)) {
		t.Fatalf("Expected random id to equal %s, but got %s\n", random, r)
	}

	if e, _ := msg.EchoRandomID(); !reflect.DeepEqual(e, []byte{}) {
		t.Fatalf("Expected echo random id to be an empty byte slice since it is a request, but got %s\n", e)
	}
}

func TestMessageX_FindNodeResponse(t *testing.T) {
	c1 := generateContact("b4945c02ddd3d4484ed7200107b46f65f5300305")
	c2 := generateContact("dc03f8f281c7118225901c8655f788cd84e3f449")
	c3 := generateContact("9d079f19f9edca7f8b2f5ce58624b55ffec2c4f3")

	fnr := messages.FindNodeResponse{
		SenderID:     "8bc8082329609092bf86dea25cf7784cd708cc5d",
		EchoRandomID: "28f787e3b60f99fb29b14266c40b536d6037307e",
		Payload: []gokad.Contact{
			c1,
			c2,
			c3,
		},
		RandomID: "8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a",
	}

	sender, _ := gokad.From("8bc8082329609092bf86dea25cf7784cd708cc5d")
	echo, _ := gokad.From("28f787e3b60f99fb29b14266c40b536d6037307e")
	random, _ := gokad.From("8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a")
	payload := make([]byte, 0)
	payload = append(payload, c1.Serialize()...)
	payload = append(payload, c2.Serialize()...)
	payload = append(payload, c3.Serialize()...)

	b, _ := fnr.Bytes()
	msg := messages.Message(b)

	if senderID, _ := msg.SenderID(); !reflect.DeepEqual(senderID, sender) {
		t.Fatalf("Expected senderID to equal %s, but got %s\n", sender, senderID)
	}

	if p, _ := msg.Payload(); !reflect.DeepEqual(p, payload) {
		t.Fatalf("Expected payload to equal %s, but got %s\n", payload, p)
	}

	if r, _ := msg.RandomID(); !reflect.DeepEqual(r, []byte(random)) {
		t.Fatalf("Expected random id to equal %s, but got %s\n", random, r)
	}

	if e, _ := msg.EchoRandomID(); !reflect.DeepEqual(e, []byte(echo)) {
		t.Fatalf("Expected echo random id to be %s, but got %s\n", echo, e)
	}

}

func generateContact(id string) gokad.Contact {
	x, _ := gokad.From(id)
	return gokad.Contact{
		ID:   x,
		IP:   net.ParseIP("127.0.0.1"),
		Port: 5050,
	}
}
