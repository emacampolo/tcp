package acceptor_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emacampolo/tcp/stream"
	"github.com/emacampolo/tcp/stream/acceptor"
)

// encodeDecoder implements a stream.EncodeDecoder that uses a new line as a delimiter for messages.
type encodeDecoder struct{}

func (ed encodeDecoder) Encode(writer io.Writer, message []byte) error {
	_, err := writer.Write(message)
	return err
}

func (ed encodeDecoder) Decode(reader io.Reader) ([]byte, error) {
	return readLine(reader)
}

// readLine reads a line from the reader until it encounters a new line character.
// The new line is not included in the returned message.
// We should not read more bytes than required to find the new line since the reader is a stream.
// If we read more bytes than required, the next call to Decode will read from the same stream and will read the bytes that we read here.
// This will cause the message to be corrupted.
func readLine(reader io.Reader) ([]byte, error) {
	var message []byte
	for {
		byte := make([]byte, 1)
		_, err := reader.Read(byte)
		if err != nil {
			return nil, err
		}

		if byte[0] == '\n' {
			break
		}

		message = append(message, byte[0])
	}
	return message, nil
}

// marshalUnmarshal implements a stream.MarshalUnmarshaler that assume that the stream.Message payload is a string.
type marshalUnmarshal struct{}

func (marshalUnmarshal) Marshal(message stream.Message) ([]byte, error) {
	return json.Marshal(message)
}

func (marshalUnmarshal) Unmarshal(data []byte) (stream.Message, error) {
	var message stream.Message
	if err := json.Unmarshal(data, &message); err != nil {
		return stream.Message{}, err
	}

	return message, nil
}

var alwaysPanicNetworkHandler = func(sender stream.Sender, message stream.Message) {
	panic("always fail")
}

func TestAcceptor_Accept_MultipleMessageOverSingleConnection(t *testing.T) {
	handler := func(sender stream.Sender, message stream.Message) {
		message.Payload = "pong"
		if err := sender.Send(message); err != nil {
			t.Fatal(err)
		}
	}

	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		encodeDecoder{},
		marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler)

	if err != nil {
		t.Fatal(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", acceptor.Port()), 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	for i := 0; i < 100; i++ {
		if err := encoder.Encode(stream.Message{Payload: "ping"}); err != nil {
			t.Fatal(err)
		}

		var message stream.Message
		if err := decoder.Decode(&message); err != nil {
			t.Fatal(err)
		}

		if message.Payload != "pong" {
			t.Fatalf("expected pong, got %s", message.Payload)
		}
	}

	acceptor.Close()
}

func TestAcceptor_Accept_MultipleMessagesOverMultipleConnections(t *testing.T) {
	var totalMessages atomic.Int64
	handler := func(sender stream.Sender, message stream.Message) {
		message.Payload = "pong"
		if err := sender.Send(message); err != nil {
			t.Fatal(err)
		}
		totalMessages.Add(1)
	}

	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		encodeDecoder{},
		marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler)

	if err != nil {
		t.Fatal(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	// Create 10 connections and send 100 messages each.
	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", acceptor.Port()), 500*time.Millisecond)
			if err != nil {
				t.Error(err)
				return
			}

			encoder := json.NewEncoder(conn)
			decoder := json.NewDecoder(conn)

			for i := 0; i < 100; i++ {
				if err := encoder.Encode(stream.Message{Payload: "ping"}); err != nil {
					t.Error(err)
					return
				}

				var message stream.Message
				if err := decoder.Decode(&message); err != nil {
					t.Error(err)
					return
				}

				if message.Payload != "pong" {
					t.Errorf("expected pong, got %s", message.Payload)
					return
				}
			}
		}()
	}

	wg.Wait()
	acceptor.Close()
	if totalMessages.Load() != 1000 {
		t.Fatalf("expected 1000 messages, got %d", totalMessages.Load())
	}
}

func TestAcceptor_Accept_ReplyAfterCallingClose(t *testing.T) {
	started := make(chan struct{})
	handler := func(sender stream.Sender, message stream.Message) {
		close(started)
		time.Sleep(500 * time.Millisecond)
		// wait for the acceptor to be closed before responding.
		message.Payload = "pong"
		if err := sender.Send(message); err != nil {
			t.Fatal(err)
		}
	}

	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		encodeDecoder{},
		marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler)

	if err != nil {
		t.Fatal(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", acceptor.Port()), 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(stream.Message{Payload: "ping"}); err != nil {
		t.Fatal(err)
	}

	<-started
	done := make(chan struct{})
	go func() {
		acceptor.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("acceptor did not close in time")
	}

	var message stream.Message
	if err := decoder.Decode(&message); err != nil {
		t.Fatal(err)
	}

	if message.Payload != "pong" {
		t.Fatalf("expected pong, got %s", message.Payload)
	}

	// If send is called after close, it should return an error.
	err = encoder.Encode(stream.Message{Payload: "ping"})
	if errors.Is(err, stream.ErrStopped) {
		t.Fatal("expected ErrClosed error")
	}
}

func TestAcceptor_WithOnConnectFunc(t *testing.T) {
	handler := func(sender stream.Sender, message stream.Message) {
		message.Payload = "pong"
		if err := sender.Send(message); err != nil {
			t.Fatal(err)
		}
	}

	var onConnectCalled atomic.Bool
	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		encodeDecoder{},
		marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler,
		acceptor.WithOnConnectFunc(func(conn net.Conn) {
			onConnectCalled.Store(true)
		}))

	if err != nil {
		t.Fatal(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", acceptor.Port()), 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(stream.Message{Payload: "ping"}); err != nil {
		t.Fatal(err)
	}

	var message stream.Message
	if err := decoder.Decode(&message); err != nil {
		t.Fatal(err)
	}

	if message.Payload != "pong" {
		t.Fatalf("expected pong, got %s", message.Payload)
	}

	conn.Close()
	acceptor.Close()

	if !onConnectCalled.Load() {
		t.Fatal("expected onConnect to be called")
	}
}

func TestAcceptor_WithOnCloseFunc(t *testing.T) {
	handler := func(sender stream.Sender, message stream.Message) {
		message.Payload = "pong"
		if err := sender.Send(message); err != nil {
			t.Fatal(err)
		}
	}

	var onCloseCalled atomic.Bool
	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		encodeDecoder{},
		marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler,
		acceptor.WithOnCloseFunc(func(conn net.Conn) {
			onCloseCalled.Store(true)
		}))

	if err != nil {
		t.Fatal(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", acceptor.Port()), 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(stream.Message{Payload: "ping"}); err != nil {
		t.Fatal(err)
	}

	var message stream.Message
	if err := decoder.Decode(&message); err != nil {
		t.Fatal(err)
	}

	if message.Payload != "pong" {
		t.Fatalf("expected pong, got %s", message.Payload)
	}

	conn.Close()
	acceptor.Close()

	if !onCloseCalled.Load() {
		t.Fatal("expected onClose to be called")
	}
}
