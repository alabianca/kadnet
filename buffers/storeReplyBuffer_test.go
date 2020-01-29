package buffers

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"reflect"
	"testing"
	"time"
)

func TestStoreReplyBuffer_Read(t *testing.T) {
	buf := NewStoreReplyBuffer()
	buf.Open()
	defer buf.Close()

	msg := messages.StoreResponse{
		SenderID:     gokad.GenerateRandomID().String(),
		EchoRandomID: gokad.GenerateRandomID().String(),
		RandomID:     gokad.GenerateRandomID().String(),
	}

	b, _ := msg.Bytes()

	writer := buf.NewWriter()
	writer.Write(b)

	var res messages.StoreResponse
	reader := buf.NewReader(msg.SenderID)

	if _, err := reader.Read(&res); err != nil {
		t.Fatalf("Expected err to be nil, but got %s\n", err)
	}

	if !reflect.DeepEqual(msg, res) {
		t.Fatalf("Expected both messages to equal")
	}
}

func TestStoreReplyBuffer_ReadTimeout(t *testing.T) {
	buf := NewStoreReplyBuffer()
	buf.Open()
	defer buf.Close()

	msg := messages.StoreResponse{
		SenderID:     gokad.GenerateRandomID().String(),
		EchoRandomID: gokad.GenerateRandomID().String(),
		RandomID:     gokad.GenerateRandomID().String(),
	}

	b, _ := msg.Bytes()

	writer := buf.NewWriter()
	writer.Write(b)

	var res messages.StoreResponse
	reader := buf.NewReader(gokad.GenerateRandomID().String())
	reader.SetDeadline(time.Millisecond * 500)

	if _, err := reader.Read(&res); err == nil {
		t.Fatalf("Expected %s , but got nil", TimeoutErr)
	}
}
