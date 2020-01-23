package kadmux

import (
	"fmt"
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"io"

)

type RpcHandlerFunc func(conn kadconn.KadWriter, req *request.Request)

func (r RpcHandlerFunc) Handle(conn kadconn.KadWriter, req *request.Request) {
	r(conn, req)
}

type RpcHandler interface {
	Handle(conn kadconn.KadWriter, req *request.Request)
}

// On any request that is sent to use we also expect to receive a
// PingResponse after we responded.
// This middleware creates the PingReplyMessage we expect to receive
// based on the request's EchoRandomId. We then store
// the expected PingReplyMessage in the provided buffer.
func ExpectPingReply(buffer buffers.Buffer) func(next RpcHandler) RpcHandler {
	return func(next RpcHandler) RpcHandler {
		fn := func(conn kadconn.KadWriter, req *request.Request) {
			mux, _ := req.Body.MultiplexKey()
			if mux == messages.PingRes {
				next.Handle(conn, req)
				return
			}
			
			writer := buffer.NewWriter()
			sid, _ := req.Body.SenderID()
			rid, _ := req.Body.RandomID()
			pingReply := messages.PingResponse{
				SenderID:     sid.String(),
				EchoRandomID: gokad.ID(rid).String(),
				RandomID:     gokad.GenerateRandomID().String(),
			}

			b, _ := pingReply.Bytes()
			writer.Write(b)

			next.Handle(conn, req)
		}

		return RpcHandlerFunc(fn)
	}
}

func Logging(w io.Writer) func(next RpcHandler) RpcHandler {
	return func(next RpcHandler) RpcHandler {
		fn := func(conn kadconn.KadWriter, req *request.Request) {
			mux, _ := req.Body.MultiplexKey()
			sid, _ := req.Body.SenderID()
			rid, _ := req.Body.RandomID()
			eid, _ := req.Body.EchoRandomID()

			fmt.Fprintf(
				w,
				"(%s): Sender: (%s) RandomID: (%s) EchoRandomID: (%s)\n",
				messageType(mux), sid.String(), gokad.ID(rid).String(), gokad.ID(eid).String())

			next.Handle(conn, req)

		}

		return RpcHandlerFunc(fn)
	}
}

func messageType(t messages.MessageType) string {
	switch t {
	case messages.FindNodeReq:
		return "FindNodeRequest"
	case messages.FindNodeRes:
		return "FindNodeResponse"
	case messages.FindValueReq:
		return "FindValueRequest"
	case messages.FindValueRes:
		return "FindValueResponse"
	case messages.PingRes:
		return "PingResponse"
	case messages.PingReq:
		return "PingRequest"
	case messages.StoreReq:
		return "StoreRequest"
	case messages.StoreRes:
		return "StoreResponse"

	default:
		return "Not Found"
	}
}
