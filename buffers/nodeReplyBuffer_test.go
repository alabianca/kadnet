package buffers

import (
	"context"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/messages"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestNodeReplyBuffer_Open(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	if nrb.IsOpen() {
		t.Fatalf("Expected node reply buffer to be closed initially")
	}

	nrb.Open()
	if !nrb.IsOpen() {
		t.Fatalf("Expected node reply buffer to be open after .Open")
	}

	nrb.Close()
	if nrb.IsOpen() {
		t.Fatalf("Expected node reply buffer to be closed after .Close")
	}
}

func TestNodeReplyBuffer_ReadWriteOnClosedBuffer(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	reader := nrb.NewReader("akjdflalfdj")
	_, readErr := reader.Read(&messages.FindNodeRequest{})
	if readErr.Error() != ClosedBufferErr {
		t.Fatalf("Expected read %s Error, but got %s\n", ClosedBufferErr, readErr.Error())
	}

	writer := nrb.NewWriter()
	_, writeErr := writer.Write([]byte{})
	if writeErr.Error() != ClosedBufferErr {
		t.Fatalf("Expected write %s Error, but got %s\n", ClosedBufferErr, writeErr.Error())
	}

}

func TestNodeReplyBuffer_Read(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	// open the buffer for reading and writing
	nrb.Open()
	defer nrb.Close()
	fnr := messages.FindNodeResponse{
		SenderID:     "8bc8082329609092bf86dea25cf7784cd708cc5d",
		EchoRandomID: "28f787e3b60f99fb29b14266c40b536d6037307e",
		Payload:      []gokad.Contact{generateContact("8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a")},
		RandomID:     "dc03f8f281c7118225901c8655f788cd84e3f449",
	}

	msg, _ := fnr.Bytes()
	writer := nrb.NewWriter()
	n, err := writer.Write(msg)
	if n != len(msg) {
		t.Fatalf("Expected %d to be written, but got %d\n", len(msg), n)
	}

	if err != nil {
		t.Fatalf("Expected write error to be nil, but got %s\n", err)
	}

	fnrOut := &messages.FindNodeResponse{}
	reader := nrb.NewReader("8bc8082329609092bf86dea25cf7784cd708cc5d" + "28f787e3b60f99fb29b14266c40b536d6037307e")
	_, rerr := reader.Read(fnrOut)
	if rerr != nil {
		t.Fatalf("Expected read error to be nil, but got %s\n", rerr)
	}

	if !reflect.DeepEqual(fnrOut, &fnr) {
		t.Fatalf("Expected fnrOur to equal fnr")
	}

}

func TestNodeReplyBuffer_AsyncRead(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	// open the buffer for reading and writing
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	nrb.Open()
	defer nrb.Close()
	fnr := messages.FindNodeResponse{
		SenderID:     "8bc8082329609092bf86dea25cf7784cd708cc5d",
		EchoRandomID: "28f787e3b60f99fb29b14266c40b536d6037307e",
		Payload:      []gokad.Contact{generateContact("8f2d6ae2378dda228d3bd39c41a4b6f6f538a41a")},
		RandomID:     "dc03f8f281c7118225901c8655f788cd84e3f449",
	}

	fnrOut := &messages.FindNodeResponse{}
	go func(c context.CancelFunc) {
		reader := nrb.NewReader("8bc8082329609092bf86dea25cf7784cd708cc5d" + "28f787e3b60f99fb29b14266c40b536d6037307e")
		_, err := reader.Read(fnrOut)
		if err != nil {
			t.Fatalf("Expected read error to be nil, but got %s\n", err)
		}

		cancel()
	}(cancel)

	time.Sleep(time.Millisecond * 200) // let some time pass
	msg, _ := fnr.Bytes()
	writer := nrb.NewWriter()
	writer.Write(msg)

	<-ctx.Done()
	if !reflect.DeepEqual(fnrOut, &fnr) {
		t.Fatalf("Expected fnrOur to equal fnr")
	}
}

func TestNodeReplyBuffer_Close(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	x := runtime.NumGoroutine()
	nrb.Open()
	nrb.Close()

	if nrb.IsOpen() {
		t.Fatalf("Expected buffer to be closed after .Close")
	}

	num := runtime.NumGoroutine()
	if num != x {
		t.Fatalf("Expected all go routines to be done after .Close, but %d are still running", num)
	}
}

func TestNodeReplyBufferReadDeadline(t *testing.T) {
	nrb := NewNodeReplyBuffer()
	nrb.Open()
	defer nrb.Close()

	reader := nrb.NewReader("nonexistingid")
	reader.SetDeadline(time.Millisecond * 200)

	var fnr messages.FindNodeResponse
	n, err := reader.Read(&fnr)
	if n > 0 {
		t.Fatalf("Expected n to be %d, but got %d\n", 0, n)
	}

	if err.Error() != TimeoutErr {
		t.Fatalf("Expected the read to timeout but got %s\n", err)
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
