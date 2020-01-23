package buffers

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"reflect"
	"testing"
)

func TestPingReplyBuffer(t *testing.T) {
	prb := NewPingReplyBuffer()
	prb.Open()
	defer prb.Close()

	fnr := messages.FindNodeResponse{
		SenderID:     gokad.GenerateRandomID().String(),
		EchoRandomID: gokad.GenerateRandomID().String(),
		Payload:      []gokad.Contact{},
		RandomID:     gokad.GenerateRandomID().String(),
	}

	key := fnr.EchoRandomID

	writer := prb.NewWriter()
	b, _ := fnr.Bytes()
	writer.Write(b)

	reader := prb.NewReader(key)
	var expected messages.FindNodeResponse
	reader.Read(&expected)

	if !reflect.DeepEqual(fnr, expected) {
		t.Fatalf("Expected fnr and expected to be equal, but got %v", expected)
	}
}

func TestPingReplyBuffer_NotFound(t *testing.T) {
	prb := NewPingReplyBuffer()
	prb.Open()
	defer prb.Close()

	fnr := messages.FindNodeResponse{
		SenderID:     gokad.GenerateRandomID().String(),
		EchoRandomID: gokad.GenerateRandomID().String(),
		Payload:      []gokad.Contact{},
		RandomID:     gokad.GenerateRandomID().String(),
	}

	key := gokad.GenerateRandomID().String()

	writer := prb.NewWriter()
	b, _ := fnr.Bytes()
	writer.Write(b)

	reader := prb.NewReader(key)
	var expected messages.FindNodeResponse
	_, err := reader.Read(&expected)

	if err == nil {
		t.Fatalf("Expected err to be %s, but got nil", PingReplyNotFoundErr)
	}

}
