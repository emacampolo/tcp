package stream_test

import (
	"log"
	"net"
	"time"

	"github.com/emacampolo/tcp/stream"
	"github.com/moov-io/iso8583"
	"github.com/moov-io/iso8583/field"
)

// This example demonstrates how to use the stream package to send and receive ISO8583 messages.
// The Stream will act as a client signing on to a fake VISA server.
// The flow will be as follows:
//  1. Client sends a sign on request to the server.
//  2. Server responds with a sign on response and after a fixed delay sends an authorization request.
//  3. Sometime in the middle, the server sends a network message.
//  4. Client responds with an authorization response.
//
// It will use a third party ISO8583 library.
func ExampleStream() {
	server, err := newISO8583Server()
	if err != nil {
		log.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	// This handler is an implementation of a stream.Handler that will be called
	// when a message is received from the server.
	// It only changes the MTI of the message to a purchase response and sends it back to the server.
	handler := func(sender stream.Sender, message stream.Message) {
		payload := message.Payload.(*iso8583.Message)
		payload.MTI("0210")
		if err := sender.Send(stream.Message{ID: message.ID, Payload: payload}); err != nil {
			log.Fatalf("error sendying to message: %v", err)
		}

		log.Println("sent authorization response")
	}

	networkHandler := func(sender stream.Sender, message stream.Message) {
		// We should receive the heartbeat message here.
		payload := message.Payload.(*iso8583.Message)
		mti, err := payload.GetMTI()
		if err != nil {
			log.Fatalf("error getting MTI: %v", err)
		}

		if mti != "0800" {
			log.Fatalf("expected heartbeat message, got %v", payload)
		}

		code, err := payload.GetString(70)
		if err != nil {
			log.Fatalf("error getting field 70: %v", err)
		}

		if code != "073" {
			log.Fatalf("expected network code 073, got %v", code)
		}

		log.Println("received heartbeat message")
	}

	// Create a new connection to the server.
	netConn, err := net.Dial("tcp", server.Addr)
	if err != nil {
		log.Fatalf("error dialing server: %v", err)
	}

	// Create a new Stream from the net.Conn and the handler and default options.
	conn, err := stream.New(netConn, iso8583EncodeDecoder{}, iso8583MarshalUnmarshal{}, handler, networkHandler)
	if err != nil {
		log.Fatalf("error creating stream: %v", err)
	}

	// Start the Stream.
	conn.Start()
	defer conn.Stop()

	// Create a new ISO8583 message with the sign on request MTI.
	message := iso8583.NewMessage(testSpec)
	id := getSTAN()
	err = message.Marshal(baseFields{
		MTI:         field.NewStringValue("0800"),
		STAN:        field.NewStringValue(id),
		NetworkCode: field.NewStringValue("071"),
	})

	if err != nil {
		log.Fatalf("error marshaling message: %v", err)
	}

	// Request the sign on request to the server and wait for a response.
	response, err := conn.Request(stream.Message{ID: id, Payload: message})
	if err != nil {
		log.Fatalf("error sending message: %v", err)
	}

	responseMessage := response.Payload.(*iso8583.Message)
	responseMTI, err := responseMessage.GetMTI()
	if err != nil {
		log.Fatalf("error getting response MTI: %v", err)
	}

	if responseMTI != "0810" {
		log.Fatalf("expected response MTI to be 0810, got %s", responseMTI)
	}

	// After the sign on response is received, we wait for the server to send a purchase request.
	time.Sleep(200 * time.Millisecond)

	// Output: received purchase response with STAN: 123456
}
