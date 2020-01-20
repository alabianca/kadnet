package kadmux

import (
	"github.com/alabianca/gokad"
	"github.com/alabianca/kadnet/kadconn"

	"github.com/alabianca/kadnet/messages"
	"github.com/alabianca/kadnet/request"
	"net"
)

const maxMsgBuffer = 100

type readResult struct {
	message messages.Message
	remote  net.Addr
	err     error
}

type ReceiverThread struct {
	fanoutReply   chan<- messages.Message
	fanoutRequest chan<- *request.Request
	conn          kadconn.KadReader
}

func NewReceiverThread(res chan<- messages.Message, req chan<- *request.Request, conn kadconn.KadReader) *ReceiverThread {
	return &ReceiverThread{
		fanoutReply:   res,
		fanoutRequest: req,
		conn:          conn,
	}
}

func (r *ReceiverThread) Run(exit <-chan chan error) {
	receivedMsgs := make([]*readResult, 0)
	var readDone chan readResult // non-nil channel means we are currently doing IO
	for {

		var startRead chan bool
		// only start reading again if currently not reading
		if readDone == nil && len(receivedMsgs) < maxMsgBuffer {
			startRead = make(chan bool, 1)
			startRead <- true
		}

		// decide where to send the message
		// if it is a response we send it along the response fanout channel
		// if it is a valid request we send it along the request fanout channel
		var fanoutRequest chan<- *request.Request
		var fanoutResponse chan<- messages.Message
		var nextMessage messages.Message
		var nextRequest *request.Request
		if len(receivedMsgs) > 0 {
			next := receivedMsgs[0]
			nextMessage = next.message
			key, _ := nextMessage.MultiplexKey()
			sender, _ := nextMessage.SenderID()
			if udpAddr, err := net.ResolveUDPAddr("udp", next.remote.String()); err == nil && !messages.IsResponse(key) {
				fanoutRequest = r.fanoutRequest
				contact := gokad.Contact{
					ID:   sender,
					IP:   udpAddr.IP,
					Port: udpAddr.Port,
				}
				nextRequest = request.New(contact, nextMessage)
			} else if err != nil {
				receivedMsgs = receivedMsgs[1:]
			}

			if messages.IsResponse(key) {
				fanoutResponse = r.fanoutReply
			}
		}

		select {
		case out := <-exit:
			out <- nil
			return

		case result := <-readDone:
			// set to nil so we know we should start reading again in the next cycle
			readDone = nil
			if result.err == nil {
				// if no error buffer the message. Otherwise just drop it
				receivedMsgs = append(receivedMsgs, &result)
			}

		case <-startRead:
			readDone = make(chan readResult, 1)
			go func() {
				msg, raddr, err := r.conn.Next()
				readDone <- readResult{msg, raddr, err}
			}()

		case fanoutRequest <- nextRequest:
			receivedMsgs = receivedMsgs[1:]

		case fanoutResponse <- nextMessage:
			receivedMsgs = receivedMsgs[1:]
		}

	}
}
