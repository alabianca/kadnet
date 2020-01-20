package kadnet

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/kadconn"
	"github.com/alabianca/kadnet/kadmux"
	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"log"
)

func onFindNode(proxy *dhtProxy) kadmux.RpcHandler {
	return func(conn kadconn.KadWriter, req *request.Request) {
		mux, _ := req.Body.MultiplexKey()
		senderID, _ := req.Body.SenderID()
		randomId, _ := req.Body.RandomID()
		payload, _ := req.Body.Payload()

		contacts := proxy.findNode(payload)

		res := messages.FindNodeResponse{
			SenderID:     proxy.dht.ID.String(),
			EchoRandomID: gokad.ID(randomId).String(),
			Payload:      contacts,
			RandomID:     gokad.GenerateRandomID().String(),
		}
		log.Printf("%d: (%s)  RandomID: (%s) Payload: (%s) Size: (%d)\n",
			mux,
			senderID.String(),
			gokad.ID(randomId).String(),
			gokad.ID(payload).String(),
			len(req.Body))

		bts, err := res.Bytes()
		if err != nil {
			log.Printf("Error %s\n", err)
			return
		}

		conn.Write(bts, req.Address())
	}
}
