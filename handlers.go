package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/buffers"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/kadmux"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"log"
)

func onFindNode(proxy *dhtProxy) kadmux.RpcHandlerFunc {
	return func(conn kadconn.KadWriter, req *request.Request) {
		randomId, _ := req.Body.RandomID()
		payload, _ := req.Body.Payload()

		contacts := proxy.findNode(payload)

		res := messages.FindNodeResponse{
			SenderID:     proxy.dht.ID.String(),
			EchoRandomID: gokad.ID(randomId).String(),
			Payload:      contacts,
			RandomID:     gokad.GenerateRandomID().String(),
		}

		bts, err := res.Bytes()
		if err != nil {
			log.Printf("Error %s\n", err)
			return
		}

		conn.Write(bts, req.Address())
	}
}

func onPingReplyImplicit(proxy *dhtProxy, buffer buffers.Buffer) kadmux.RpcHandlerFunc {
	return func(conn kadconn.KadWriter, req *request.Request) {
		// find the pingReplyMessage in the buffer
		id, err := req.Body.EchoRandomID()
		if err != nil {
			return
		}
		sid, err := req.Body.SenderID()
		if err != nil {
			return
		}

		key := gokad.ID(id).String()
		reader := buffer.NewReader(key)

		var prm messages.PingResponse
		if _, err := reader.Read(&prm); err != nil {
			return
		}

		if prm.EchoRandomID != key || prm.SenderID != sid.String() {
			return
		}

		// they match. Let's attempt to insert contact to our dht
		proxy.insert(req.Contact)


	}
}

func onPingRequest(myID gokad.ID) kadmux.RpcHandlerFunc {
	return func(conn kadconn.KadWriter, req *request.Request) {
		rid, err := req.Body.RandomID()
		if err != nil {
			return
		}

		res := messages.Explicit()
		res.RandomID = gokad.GenerateRandomID().String()
		res.EchoRandomID = gokad.ID(rid).String()
		res.SenderID = myID.String()

		b, err := res.Bytes()
		if err != nil {
			return
		}

		conn.Write(b, req.Address())
	}
}