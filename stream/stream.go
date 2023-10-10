package stream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Sender is used to send messages that don't expect a response to the remote host.
type Sender interface {
	// Send sends the given message to the remote host.
	// Usually the Message does not have an ID.
	// It returns an ErrStopped error if the Stream is stopped.
	Send(message Message) error
}

// Handler handles incoming messages from the remote host that are not responses to requests nor network messages.
// This means that Message.IsResponse and Message.IsNetwork are both false.
// This invariant is guaranteed by the implementation of the Unmarshaler interface.
type Handler func(sender Sender, message Message)

// NetworkHandler handles incoming messages from the remote host that are network messages.
// A network message is usually a message that is not related to the business logic of the application.
// eg: a heartbeat message.
type NetworkHandler func(sender Sender, message Message)

// Requester is used to send requests to the remote host and wait for a response.
// The message must have an ID which is used to match the response with the request.
type Requester interface {
	// Request sends a request to the remote host and waits for a response.
	// It returns an ErrStopped error if the Stream is stopped.
	Request(message Message) (Message, error)
}

// WriteTimeoutFunc is a function that is called when the Stream is idle waiting to write to the underlying connection.
// A common use case is to send a heartbeat message to the remote destination.
// The function is called in a separate goroutine.
type WriteTimeoutFunc func(requester Requester)

// ReadTimeoutFunc is a function that is called when the Stream is idle waiting for a message from the remote host
// for a period of time.
// The function is called in a separate goroutine.
type ReadTimeoutFunc func(requester Requester)

var (
	// ErrStopped is returned when the Stream is stopped and the operation can't be completed.
	ErrStopped = errors.New("stream stopped")
	// ErrRequestTimeout is returned when the request is not completed within the specified timeout.
	ErrRequestTimeout = errors.New("request timeout")
	// ErrUnexpectedResponse is returned when the response is not expected.
	ErrUnexpectedResponse = errors.New("unexpected response")
)

// EncodeDecoder is responsible for encoding and decoding the byte representation of a Message.
// This interface it is used in conjunction with the MarshalUnmarshaler interface.
type EncodeDecoder interface {
	// Encode encodes the message into a writer. It is safe call Write more than once to write the entire message.
	Encode(writer io.Writer, message []byte) error
	// Decode decodes the message from a reader. It is responsible for reading an entire message.
	// It shouldn't read more that necessary.
	Decode(reader io.Reader) (message []byte, err error)
}

// MarshalUnmarshaler is responsible for marshaling and unmarshalling the messages.
type MarshalUnmarshaler interface {
	// Marshal marshals the message into a byte array.
	Marshal(Message) ([]byte, error)
	// Unmarshal unmarshalls the message from a byte array.
	Unmarshal([]byte) (Message, error)
}

// Message represents a message being transferred between hosts.
type Message struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string

	// Payload is the message payload.
	Payload any

	// IsResponse is true if the message is a response to a request.
	IsResponse bool

	// IsNetwork is true if the message is network message such as a heartbeat or a ping.
	IsNetwork bool

	// IsReject is true if the message is a reject message. This is used by the card network
	// to notify the issuer that the transaction response was rejected.
	IsReject bool
}

// ErrorHandler is called in a goroutine with the errors that can't be returned to the caller.
type ErrorHandler func(err error)

// New returns an un-started Stream.
func New(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler,
	handler Handler, networkHandler NetworkHandler, opts ...Option) (*Stream, error) {
	if conn == nil {
		return nil, errors.New("conn is nil")
	}

	if encodeDecoder == nil {
		return nil, errors.New("encode decoder is nil")
	}

	if marshalUnmarshaler == nil {
		return nil, errors.New("marshal unmarshaler is nil")
	}

	if handler == nil {
		return nil, errors.New("handler is nil")
	}

	if networkHandler == nil {
		return nil, errors.New("network handler is nil")
	}

	var options options
	for _, opt := range DefaultOptions {
		if err := opt(&options); err != nil {
			return nil, fmt.Errorf("failed to apply default option: %w", err)
		}
	}

	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	return newStream(conn, encodeDecoder, marshalUnmarshaler, handler, networkHandler, &options), nil
}

// Stream represents a bidirectional communication channel between hosts.
// It is worth mentioning that we don't use the term "client" and "server". Instead, we use the terms "host" and "remote host"
// since once the tcp connection is established, messages can be sent in both directions.
// It requires a net.Conn which can be created using net.Dial or net.Listen.
// Its main purpose is to send and receive messages.
//   - To receive messages that are sent by the remote host, you need to implement the Handler and NetworkHandler interfaces.
//   - To send messages to the remote host, you can either use the Request or Send method.
//     The difference between the two is that the Request resembles to the Request-Response pattern in that it expects
//     a response from the remote host.
//     The Send function sends a message to the remote host as a response to a request.
//     Both ensures that the message is written to the socket before returning the control to the caller.
//
// When you are done with the Stream, you need to call Stop to ensure that all the resources are released properly
// and all the requests are completed.
// It is safe to call methods of the connection from multiple goroutines.
type Stream struct {
	conn               net.Conn
	encodeDecoder      EncodeDecoder
	marshalUnmarshaler MarshalUnmarshaler
	handler            Handler
	networkHandler     NetworkHandler
	options            *options

	// outgoingChannel is used to send requests to the remote host.
	outgoingChannel chan request
	// incomingChannel is used to receive messages from the remote host.
	incomingChannel chan []byte
	// done is closed when the Stream is stopped and all messages are handled.
	done chan struct{}

	pendingResponsesMutex sync.Mutex // guards pendingResponses map.
	pendingResponses      map[string]response

	runningMutex sync.Mutex // guards messagesWg and running.
	messagesWg   sync.WaitGroup
	running      bool

	sender    Sender
	requester Requester
}

func newStream(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler,
	handler Handler, networkHandler NetworkHandler, options *options) *Stream {

	stream := Stream{
		conn:               conn,
		encodeDecoder:      encodeDecoder,
		marshalUnmarshaler: marshalUnmarshaler,
		handler:            handler,
		networkHandler:     networkHandler,
		options:            options,

		outgoingChannel:  make(chan request),
		incomingChannel:  make(chan []byte),
		done:             make(chan struct{}),
		pendingResponses: make(map[string]response),
	}

	stream.sender = &sender{stream: &stream}
	stream.requester = &requester{stream: &stream}

	return &stream
}

// request represents a request to the remote host.
type request struct {
	message   []byte
	requestID string
	replyCh   chan Message
	errCh     chan error
}

// response represents a response from the remote host.
type response struct {
	replyCh chan Message
	errCh   chan error
}

type sender struct {
	stream *Stream
}

// Send implements the Sender interface.
// It must be called in the handleResponse goroutine to ensure that the mutex and the wait group are properly handled.
func (r *sender) Send(message Message) error {
	messageBytes, err := r.stream.marshalUnmarshaler.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	var buff bytes.Buffer
	if err := r.stream.encodeDecoder.Encode(&buff, messageBytes); err != nil {
		return err
	}

	req := request{
		message: buff.Bytes(),
		errCh:   make(chan error, 1),
	}

	r.stream.outgoingChannel <- req
	err = <-req.errCh

	return err
}

type requester struct {
	stream *Stream
}

// Request implements the Requester interface.
func (r *requester) Request(message Message) (Message, error) {
	r.stream.runningMutex.Lock()
	if !r.stream.running {
		r.stream.runningMutex.Unlock()
		return Message{}, ErrStopped
	}

	r.stream.messagesWg.Add(1)
	r.stream.runningMutex.Unlock()

	defer r.stream.messagesWg.Done()

	if message.ID == "" {
		return Message{}, errors.New("message ID is empty")
	}

	messageBytes, err := r.stream.marshalUnmarshaler.Marshal(message)
	if err != nil {
		return Message{}, fmt.Errorf("marshaling message: %w", err)
	}

	var buff bytes.Buffer
	if err := r.stream.encodeDecoder.Encode(&buff, messageBytes); err != nil {
		return Message{}, err
	}

	req := request{
		message:   buff.Bytes(),
		requestID: message.ID,
		replyCh:   make(chan Message),
		errCh:     make(chan error, 1),
	}

	r.stream.outgoingChannel <- req

	var responseMessage Message

	timeout := time.NewTimer(r.stream.options.requestTimeoutDuration)
	defer timeout.Stop()

	select {
	case responseMessage = <-req.replyCh:
	case err = <-req.errCh:
	case <-timeout.C:
		err = ErrRequestTimeout
	}

	r.stream.pendingResponsesMutex.Lock()
	delete(r.stream.pendingResponses, req.requestID)
	r.stream.pendingResponsesMutex.Unlock()

	return responseMessage, err
}

// Start starts the Stream by reading and writing messages to the underlying connection.
func (s *Stream) Start() {
	s.runningMutex.Lock()
	if s.running {
		s.runningMutex.Unlock()
		return
	}

	s.running = true
	s.runningMutex.Unlock()

	go s.writeLoop()
	go s.readLoop()
	go s.handleLoop()
}

// writeLoop read requests from the outgoingChannel and writes them to the underlying connection.
func (s *Stream) writeLoop() {
	var err error

	for err == nil {
		timeoutTimer := time.NewTimer(s.options.writeTimeoutDuration)

		select {
		case req := <-s.outgoingChannel:
			if req.replyCh != nil {
				s.pendingResponsesMutex.Lock()
				s.pendingResponses[req.requestID] = response{
					replyCh: req.replyCh,
					errCh:   req.errCh,
				}
				s.pendingResponsesMutex.Unlock()
			}

			_, err = s.conn.Write(req.message)
			if err != nil {
				s.handleError(err)
				break
			}

			// For messages using Send we just
			// return nil to errCh as caller is waiting for error.
			// Messages sent by Request waits for responses to be received to their replyCh channel.
			if req.replyCh == nil {
				req.errCh <- nil
			}
		case <-timeoutTimer.C:
			if s.options.writeTimeoutFunc != nil {
				go s.options.writeTimeoutFunc(s.requester)
			}
		case <-s.done:
			timeoutTimer.Stop()
			return
		}

		timeoutTimer.Stop()
	}

	s.handleConnectionError(err)
}

// readLoop reads data from the socket and runs a goroutine to handle the message.
// It reads data from the socket until the Stream is stopped.
func (s *Stream) readLoop() {
	var err error
	var messageBytes []byte

	for {
		messageBytes, err = s.encodeDecoder.Decode(s.conn)
		if err != nil {
			s.handleError(err)
			break
		}

		// Send the message to the incomingChannel to be handled.
		// If the stream is stopped, the message will be ignored and the loop will be stopped.
		select {
		case s.incomingChannel <- messageBytes:
		case <-s.done:
			return
		}
	}

	s.handleConnectionError(err)
}

// handleLoop handles the incoming messages that are read from the socket by the readLoop.
// The messages may correspond to the responses to the requests sent by the Request method
// or, they may be messages sent by the remote host to the local host without a previous request.
func (s *Stream) handleLoop() {
	for {
		timeoutTimer := time.NewTimer(s.options.readTimeoutDuration)
		select {
		case rawBytes := <-s.incomingChannel:
			go s.handleResponse(rawBytes)
		case <-timeoutTimer.C:
			if s.options.readTimeoutFunc != nil {
				go s.options.readTimeoutFunc(s.requester)
			}
		case <-s.done:
			timeoutTimer.Stop()
			return
		}

		timeoutTimer.Stop()
	}
}

func (s *Stream) handleResponse(rawBytes []byte) {
	message, err := s.marshalUnmarshaler.Unmarshal(rawBytes)
	if err != nil {
		s.handleError(err)
		return
	}

	// If the message is a response to a request, we should send its channel.
	// If it is not found in the pendingResponses map, it means that the message is unknown or the request timed out.
	// In that case, we should just ignore the response but call the handlerError function with a sentinel error.
	if message.IsResponse {
		s.pendingResponsesMutex.Lock()
		response, found := s.pendingResponses[message.ID]
		s.pendingResponsesMutex.Unlock()
		if found {
			response.replyCh <- message
		} else {
			s.handleError(ErrUnexpectedResponse)
		}
		return
	}

	// If a message is sent by the remote host without a previous request,
	// we should increment the wait group to prevent returning from Stop before the message is handled.
	s.runningMutex.Lock()
	if !s.running {
		s.runningMutex.Unlock()
		return
	}
	s.messagesWg.Add(1)
	s.runningMutex.Unlock()
	if message.IsNetwork {
		s.networkHandler(s.sender, message)
	} else {
		s.handler(s.sender, message)
	}
	s.messagesWg.Done()
}

func (s *Stream) handleError(err error) {
	if s.options.errorHandler == nil {
		return
	}

	s.runningMutex.Lock()
	if !s.running {
		s.runningMutex.Unlock()
		return
	}
	s.runningMutex.Unlock()

	go s.options.errorHandler(err)
}

// handlerConnectionError is called when we get an error when reading or writing to the underlying connection.
func (s *Stream) handleConnectionError(err error) {
	s.runningMutex.Lock()
	if err == nil || !s.running {
		s.runningMutex.Unlock()
		return
	}

	s.running = false
	s.runningMutex.Unlock()

	done := make(chan struct{})
	defer close(done)

	s.pendingResponsesMutex.Lock()
	for _, resp := range s.pendingResponses {
		resp.errCh <- ErrStopped
	}
	s.pendingResponsesMutex.Unlock()

	go func() {
		for {
			select {
			case req := <-s.outgoingChannel:
				req.errCh <- ErrStopped
			case <-done:
				return
			}
		}
	}()

	_ = s.stop()
}

func (s *Stream) stop() error {
	s.messagesWg.Wait()
	close(s.done)
	return s.conn.Close()
}

// Stop stops the Stream and waits for all messages to be handled.
// Then it closes the underlying connection.
func (s *Stream) Stop() error {
	s.runningMutex.Lock()
	if !s.running {
		s.runningMutex.Unlock()
		return nil
	}

	s.running = false
	s.runningMutex.Unlock()

	return s.stop()
}

// Done returns a channel that is closed when the Stream is stopped and all messages are handled.
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// Request sends message and waits for the response as you would expect from the request-response pattern.
// It returns a ErrRequestTimeout error if the response is not received within the timeout.
// If the Stream is stopped, it returns ErrStopped.
func (s *Stream) Request(message Message) (Message, error) {
	return s.requester.Request(message)
}
