package buffers

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"reflect"
	"testing"
	"time"
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
	// set the expiry to something shorter
	prb.expiry = time.Second * 1
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
	reader.SetDeadline(time.Millisecond * 500)
	_, err := reader.Read(&expected)

	if err == nil {
		t.Fatalf("Expected err to be %s, but got nil", PingReplyNotFoundErr)
	}

}

func TestPingReplyBuffer_WithTimeout(t *testing.T) {
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
	reader.SetDeadline(time.Second * 2)
	if _, err := reader.Read(&expected); err == nil {
		t.Fatalf("Expected %s, but got nil", TimeoutErr)
	}
}

func TestPingReplyBuffer_First(t *testing.T) {
	prb := NewPingReplyBuffer()
	prb.Open()
	defer prb.Close()

	pr := messages.PingResponse{
		SenderID:     gokad.GenerateRandomID().String(),
		EchoRandomID: gokad.GenerateRandomID().String(),
		RandomID:     gokad.GenerateRandomID().String(),
	}

	writer := prb.NewWriter()
	b, _ := pr.Bytes()
	writer.Write(b)

	var prx messages.PingResponse
	prb.First(&prx)

	if !reflect.DeepEqual(pr, prx) {
		t.Fatalf("Expected them to be equal")
	}
}
