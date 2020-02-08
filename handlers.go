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

		key := sid.String() + gokad.ID(id).String()
		reader := buffer.NewReader(key)

		var prm messages.PingResponse
		if _, err := reader.Read(&prm); err != nil {
			return
		}

		if prm.SenderID + prm.EchoRandomID != key {
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

func onStoreRequest(myID gokad.ID, proxy *dhtProxy) kadmux.RpcHandlerFunc {
	return func(conn kadconn.KadWriter, req *request.Request) {
		var storeReq messages.StoreRequest
		messages.ToKademliaMessage(req.Body, &storeReq)

		if storeReq.SenderID == "" {
			return
		}

		key := storeReq.Payload.Key
		ip := storeReq.Payload.Value.Host
		port := storeReq.Payload.Value.Port

		proxy.store(key, ip, port)

		res := messages.StoreResponse{
			SenderID:     myID.String(),
			EchoRandomID: storeReq.RandomID,
			RandomID:     gokad.GenerateRandomID().String(),
		}

		b, err := res.Bytes()
		if err != nil {
			return
		}

		conn.Write(b, req.Address())

	}
}

func onFindValue(proxy *dhtProxy) kadmux.RpcHandlerFunc {
	return func(conn kadconn.KadWriter, req *request.Request) {
		randomId, _ := req.Body.RandomID()
		payload, _ := req.Body.Payload()

		contacts, val := proxy.findValue(gokad.ID(payload))

		var fvr *messages.FindValueResponse
		if len(contacts) > 0 {
			fvr = messages.FindValueResponseNOK()
			fvr.Payload.Contacts = contacts
		} else {
			fvr = messages.FindValueResponseOK()
			fvr.Payload.Contacts = []gokad.Contact{
				{
					ID: gokad.GenerateRandomID(), // just generate a random id here. We are not using it
					IP:   val.Host,
					Port: val.Port,
				},
			}
			fvr.Payload.Key = gokad.ID(payload).String()
		}

		fvr.EchoRandomID = gokad.ID(randomId).String()
		fvr.RandomID = gokad.GenerateRandomID().String()
		fvr.SenderID = proxy.dht.ID.String()

		b, err := fvr.Bytes()
		if err != nil {
			log.Printf("Error onFindValue %s\n", err)
			return
		}

		conn.Write(b, req.Address())
	}
}
