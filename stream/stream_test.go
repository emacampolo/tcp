package stream_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emacampolo/tcp/stream"
	"github.com/emacampolo/tcp/stream/internal"
)

type testServer struct {
	Addr   string
	Server *internal.TestServer

	mutex         sync.Mutex
	receivedPings int
}

func (t *testServer) IncrementPings() {
	t.mutex.Lock()
	t.receivedPings++
	t.mutex.Unlock()
}

func (t *testServer) ReceivedPings() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.receivedPings
}

func (t *testServer) Shutdown() {
	t.Server.Shutdown()
}

func (t *testServer) Handler() internal.Handler {
	return func(ctx context.Context, conn net.Conn) {
		defer conn.Close()
		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			var message stream.Message
			if err := decoder.Decode(&message); err != nil {
				if err == io.EOF {
					return
				}

				return
			}

			go func() {
				switch message.Payload {
				case "ping":
					t.IncrementPings()
					encoder.Encode(stream.Message{ID: message.ID, Payload: "pong", IsNetwork: false, IsResponse: true})
				case "sign_on":
					encoder.Encode(stream.Message{ID: message.ID, Payload: "signed_on", IsNetwork: false, IsResponse: true})

					for i := 0; i < 1000; i++ {
						randomString := make([]byte, 5)
						rand.Read(randomString)
						// ID starts from 100 to avoid collision with other messages.
						encoder.Encode(stream.Message{ID: strconv.Itoa(100 + i), Payload: hex.EncodeToString(randomString), IsNetwork: false, IsResponse: false})
					}
				case "sign_off":
					encoder.Encode(stream.Message{ID: message.ID, Payload: "signed_off", IsNetwork: false, IsResponse: true})
					// Wait for the client to decode the ack before closing the connection since
					// the read loop is still running and will try to read from the connection and
					// will get an EOF error before the client has a chance to decode the ack.
					time.Sleep(100 * time.Millisecond)
				case "delay":
					time.Sleep(500 * time.Millisecond)
					encoder.Encode(stream.Message{ID: message.ID, Payload: "delayed", IsNetwork: false, IsResponse: true})
				case "same_id":
					// Send a message to the client with the same ID as the request.
					encoder.Encode(stream.Message{ID: message.ID, Payload: "new_same_id", IsNetwork: true, IsResponse: false})
					// and then delay the reply.
					time.Sleep(200 * time.Millisecond)
					encoder.Encode(stream.Message{ID: message.ID, Payload: "same_id_response", IsNetwork: false, IsResponse: true})
				case "trigger_close":
					encoder.Encode(stream.Message{ID: message.ID, Payload: "closing", IsNetwork: false, IsResponse: true})
					time.Sleep(50 * time.Millisecond)
					conn.Close()
				default:
					log.Fatalf("server: received unknown message: %v", message.Payload)
				}
			}()
		}
	}
}

func newTestServerWithAddr(addr string) (*testServer, error) {
	var testServer testServer
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	server := internal.NewTestServer(listener, testServer.Handler())
	testServer.Server = server
	testServer.Addr = listener.Addr().String()

	go server.Listen()

	return &testServer, nil
}

func newTestServer() (*testServer, error) {
	return newTestServerWithAddr("127.0.0.1:0")
}

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

var alwaysPanicHandler = func(sender stream.Sender, message stream.Message) {
	panic("always fail")
}

var alwaysPanicNetworkHandler = func(sender stream.Sender, message stream.Message) {
	panic("always fail")
}

type netConnAlwaysWriteError struct {
	net.Conn
}

func (*netConnAlwaysWriteError) Write(p []byte) (n int, err error) {
	return 0, errors.New("always fail")
}

type netConnDummy struct {
	net.Conn
}

func TestNew(t *testing.T) {
	t.Run("encode decoder is nil", func(t *testing.T) {
		_, err := stream.New(netConnDummy{}, nil, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler)
		if err == nil || err.Error() != "encode decoder is nil" {
			t.Fatalf("expected error to be 'encode decoder is nil' but got: %v", err)
		}
	})
	t.Run("marshal unmarshal is nil", func(t *testing.T) {
		_, err := stream.New(netConnDummy{}, encodeDecoder{}, nil, alwaysPanicHandler, alwaysPanicNetworkHandler)
		if err == nil || err.Error() != "marshal unmarshaler is nil" {
			t.Fatalf("expected error to be 'marshal unmarshal is nil' but got: %v", err)
		}
	})
	t.Run("handler is nil", func(t *testing.T) {
		_, err := stream.New(netConnDummy{}, encodeDecoder{}, marshalUnmarshal{}, nil, alwaysPanicNetworkHandler)
		if err == nil || err.Error() != "handler is nil" {
			t.Fatalf("expected error to be 'response handler is nil' but got: %v", err)
		}
	})
	t.Run("network handler is nil", func(t *testing.T) {
		_, err := stream.New(netConnDummy{}, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, nil)
		if err == nil || err.Error() != "network handler is nil" {
			t.Fatalf("expected error to be 'network handler is nil' but got: %v", err)
		}
	})
	t.Run("conn is nil", func(t *testing.T) {
		_, err := stream.New(nil, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler)
		if err == nil || err.Error() != "conn is nil" {
			t.Fatalf("expected error to be 'conn is nil' but got: %v", err)
		}
	})
}

func TestStream(t *testing.T) {
	t.Run("request messages to server and receives responses", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()

		result, err := conn.Request(stream.Message{ID: "1", Payload: "ping"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "pong" {
			t.Fatalf("unexpected result: %v", result)
		}

		conn.Stop()
	})
	t.Run("request a sign on message to server and wait for all messages to be handled", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		var count atomic.Int64
		handler := func(sender stream.Sender, message stream.Message) {
			// Simulate a long running task.
			count.Add(1)
			time.Sleep(1 * time.Second)
		}

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := stream.WithErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			handler,
			alwaysPanicNetworkHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()

		result, err := conn.Request(stream.Message{ID: "1", Payload: "sign_on"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_on" {
			t.Fatalf("result: %v, expected signed_on", result)
		}

		time.Sleep(1 * time.Second)

		result, err = conn.Request(stream.Message{ID: "1", Payload: "sign_off"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_off" {
			t.Fatalf("result: %v, expected signed_off", result)
		}

		conn.Stop()

		if count.Load() != 1000 {
			t.Fatalf("expected 1000 messages to be handled, got %v", count.Load())
		}
	})
	t.Run("return ErrStopped when Stop was called", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := stream.WithErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		conn.Stop()

		_, err = conn.Request(stream.Message{ID: "1", Payload: "sign_on"})
		if !errors.Is(err, stream.ErrStopped) {
			t.Fatalf("expected ErrStopped, got %v", err)
		}
	})
	t.Run("return ErrRequestTimeout when response was not received during request timeout", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := []stream.Option{
			stream.WithRequestTimeout(100 * time.Millisecond),
			stream.WithErrorHandler(func(err error) {
				if !errors.Is(err, io.EOF) {
					t.Fatalf("unexpected error: %v", err)
				}
			}),
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler,
			options...,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		_, err = conn.Request(stream.Message{ID: "1", Payload: "delay"})
		if !errors.Is(err, stream.ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}
	})
	t.Run("return error when the Message does not have an ID", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := []stream.Option{
			stream.WithRequestTimeout(100 * time.Millisecond),
			stream.WithErrorHandler(func(err error) {
				if !errors.Is(err, io.EOF) {
					t.Fatalf("unexpected error: %v", err)
				}
			}),
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler,
			options...,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		_, err = conn.Request(stream.Message{Payload: "delay"})
		if err == nil || err.Error() != "message ID is empty" {
			t.Fatalf("expected error with \"message ID is empty\", got %q", err.Error())
		}
	})
	t.Run("pending requests should complete after Stop was called", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()

		var wg sync.WaitGroup
		var errs []error
		var mu sync.Mutex

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				id := fmt.Sprintf("%v", i+1)
				result, err := conn.Request(stream.Message{ID: id, Payload: "delay"})
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("error sending message: %v", err))
					mu.Unlock()
					return
				}

				if result.Payload.(string) != "delayed" {
					mu.Lock()
					errs = append(errs, fmt.Errorf("result: %v, expected \"delayed\"", result))
					mu.Unlock()
					return
				}
			}(i)
		}

		// Let's wait all messages to be sent
		time.Sleep(100 * time.Millisecond)
		conn.Stop()
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}
	})
	t.Run("responses received asynchronously", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		var (
			results []stream.Message
			wg      sync.WaitGroup
			mu      sync.Mutex
			errs    []error
		)

		wg.Add(1)
		go func() {
			defer wg.Done()

			result, err := conn.Request(stream.Message{ID: "1", Payload: "delay"})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("error sending message 1: %v", err))
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			// This message will be sent after the first one
			time.Sleep(100 * time.Millisecond)

			result, err := conn.Request(stream.Message{ID: "2", Payload: "ping"})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("error sending message 2: %v", err))
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()

		// Let's wait all messages to be sent
		time.Sleep(200 * time.Millisecond)
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		if len(results) != 2 {
			t.Fatalf("unexpected results: %v", results)
		}

		// We expect that response for the second message was received first.
		if results[0].ID != "2" {
			t.Fatalf("expected result with ID \"2\", got %q", results[0].ID)
		}

		if results[1].ID != "1" {
			t.Fatalf("expected result with ID \"1\", got %q", results[1].ID)
		}
	})
	t.Run("automatically sends ping messages after write timeout interval", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		pingHandler := func(requester stream.Requester) {
			_, err := requester.Request(stream.Message{ID: "1", Payload: "ping"})
			if err != nil {
				if errors.Is(err, stream.ErrStopped) {
					return
				}

				t.Fatalf("error sending message: %v", err)
			}
		}

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := stream.WithWriteTimeout(25*time.Millisecond, pingHandler)

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		// We expect that ping interval in 50ms has not passed yet
		// and server has not being pinged yet.
		if server.ReceivedPings() != 0 {
			t.Fatalf("expected 0 received pings, got %v", server.receivedPings)
		}

		time.Sleep(200 * time.Millisecond)

		// On average, we expect that server has been pinged 7 times.
		// We use 5 as a threshold to avoid flaky tests.
		if server.ReceivedPings() < 5 {
			t.Fatalf("expected at least 3 received ping, got %v", server.receivedPings)
		}
	})
	t.Run("automatically sends ping messages after read timeout interval", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		pingHandler := func(requester stream.Requester) {
			_, err := requester.Request(stream.Message{ID: "1", Payload: "ping"})
			if err != nil {
				if errors.Is(err, stream.ErrStopped) {
					return
				}

				t.Fatalf("error sending message: %v", err)
			}
		}

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := stream.WithReadTimeout(25*time.Millisecond, pingHandler)

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		// We expect that ping interval in 50ms has not passed yet
		// and server has not being pinged yet.
		if server.ReceivedPings() != 0 {
			t.Fatalf("expected 0 received pings, got %v", server.receivedPings)
		}

		time.Sleep(200 * time.Millisecond)

		// On average, we expect that server has been pinged 7 times.
		// We use 5 as a threshold to avoid flaky tests.
		if server.ReceivedPings() < 5 {
			t.Fatalf("expected at least 3 received ping, got %v", server.receivedPings)
		}
	})
	t.Run("ignores forgotten messages", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		var expectedErr atomic.Value
		options := []stream.Option{
			stream.WithRequestTimeout(100 * time.Millisecond),
			stream.WithErrorHandler(func(err error) {
				expectedErr.Store(err)
			}),
		}

		handler := func(sender stream.Sender, message stream.Message) {
			t.Fatalf("unexpected message: %v", message)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			handler,
			alwaysPanicNetworkHandler,
			options...,
		)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		_, err = conn.Request(stream.Message{ID: "1", Payload: "delay"})
		if !errors.Is(err, stream.ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}

		// Give time to the server to send the delayed message.
		time.Sleep(1 * time.Second)

		if !errors.Is(expectedErr.Load().(error), stream.ErrUnexpectedResponse) {
			t.Fatalf("expected ErrUnexpectedResponse, got %v", expectedErr)
		}
	})
	t.Run("it handles incoming messages with same id not as reply but as incoming message", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		var receivedMessage atomic.Value
		handler := func(sender stream.Sender, message stream.Message) {
			receivedMessage.Store(message)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			handler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		request, err := conn.Request(stream.Message{ID: "1", Payload: "same_id"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if request.ID != "1" {
			t.Fatalf("expected request with ID \"1\", got %q", request.ID)
		}

		if request.Payload.(string) != "same_id_response" {
			t.Fatalf("expected request with payload \"same_id_response\", got %q", request.Payload)
		}

		if receivedMessage.Load().(stream.Message).ID != "1" {
			t.Fatalf("expected received message with ID \"1\", got %q", receivedMessage.Load())
		}

		if receivedMessage.Load().(stream.Message).Payload.(string) != "new_same_id" {
			t.Fatalf("expected received message with payload \"new_same_id\", got %q", receivedMessage.Load())
		}
	})
	t.Run("pending requests get ErrStopped if server closed the stream", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()
		defer conn.Stop()

		done := make(chan struct{})
		errCh := make(chan error, 1)
		go func() {
			defer close(done)

			_, err := conn.Request(stream.Message{ID: "1", Payload: "delay"})
			errCh <- err
		}()

		time.Sleep(50 * time.Millisecond)

		// Trigger the server to close the stream.
		_, err = conn.Request(stream.Message{ID: "2", Payload: "trigger_close"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		<-done
		err = <-errCh
		if !errors.Is(err, stream.ErrStopped) {
			t.Fatalf("expected ErrStopped, got %v", err)
		}
	})
	t.Run("calling Request on a stopped Stream should return ErrStopped", func(t *testing.T) {
		conn, err := stream.New(
			netConnDummy{},
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		_, err = conn.Request(stream.Message{ID: "1", Payload: "ping"})
		if !errors.Is(err, stream.ErrStopped) {
			t.Fatalf("expected ErrStopped, got %v", err)
		}
	})
	t.Run("write on a closed connection should call the error handler", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			&netConnAlwaysWriteError{netConn},
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()

		_, err = conn.Request(stream.Message{ID: "1", Payload: "ping"})
		if !errors.Is(err, stream.ErrStopped) {
			t.Fatalf("expected ErrStopped, got %v", err)
		}
	})
	t.Run("request multiples messages and stop at random type and should not leak nor block indefinitely", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := stream.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			alwaysPanicNetworkHandler)

		if err != nil {
			t.Fatalf("error creating stream: %v", err)
		}

		conn.Start()

		errCh := make(chan error, 1)
		var processedMessages int
		for i := 0; i < 100; i++ {
			if i == 50 {
				go func() {
					errCh <- conn.Stop()
				}()
			}

			_, err := conn.Request(stream.Message{ID: strconv.Itoa(i), Payload: "ping"})
			if err == nil {
				processedMessages++
				continue
			}

			if !errors.Is(err, stream.ErrStopped) {
				t.Fatalf("error sending message: %v", err)
			}
		}

		if err := <-errCh; err != nil {
			t.Fatalf("error stopping stream: %v", err)
		}

		if processedMessages <= 50 {
			t.Fatalf("expected at least 500 processed messages, got %d", processedMessages)
		}
	})
}

func BenchmarkRequest100(b *testing.B) { benchmarkRequest(100, b) }

func BenchmarkRequest1000(b *testing.B) { benchmarkRequest(1000, b) }

func BenchmarkRequest10000(b *testing.B) { benchmarkRequest(10000, b) }

func BenchmarkRequest100000(b *testing.B) { benchmarkRequest(100000, b) }

func benchmarkRequest(m int, b *testing.B) {
	server, err := newTestServer()
	if err != nil {
		b.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	netConn, err := net.Dial("tcp", server.Addr)
	if err != nil {
		b.Fatalf("error dialing test server: %v", err)
	}

	options := []stream.Option{
		stream.WithErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				b.Fatalf("unexpected error: %v", err)
			}
		}),
		stream.WithRequestTimeout(10 * time.Second),
	}

	conn, err := stream.New(
		netConn,
		encodeDecoder{},
		marshalUnmarshal{},
		alwaysPanicHandler,
		alwaysPanicNetworkHandler,
		options...,
	)

	if err != nil {
		b.Fatalf("error creating stream: %v", err)
	}

	conn.Start()
	defer conn.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		processMessages(b, m, conn)
	}
}

// send/receive m messages
func processMessages(b *testing.B, m int, conn *stream.Stream) {
	var wg sync.WaitGroup
	var gerr error

	for i := 0; i < m; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			result, err := conn.Request(stream.Message{ID: strconv.Itoa(i), Payload: "ping"})
			if err != nil {
				gerr = err
				return
			}

			if result.Payload.(string) != "pong" {
				gerr = err
				return
			}
		}(i)
	}

	wg.Wait()
	if gerr != nil {
		b.Fatal("sending message: ", gerr)
	}
}
