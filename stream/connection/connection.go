package connection

import (
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"github.com/emacampolo/tcp/stream"
)

// OnConnectFunc is called synchronously when a connection is established.
type OnConnectFunc func(conn net.Conn)

// OnCloseFunc is called synchronously when a connection is closed.
type OnCloseFunc func(conn net.Conn)

// Connection is a TCP client that connects to a remote host and delegates the handling of the connection to a stream.Stream.
type Connection struct {
	address            string
	encodeDecoder      stream.EncodeDecoder
	marshalUnmarshaler stream.MarshalUnmarshaler
	handler            stream.Handler
	networkHandler     stream.NetworkHandler
	options            *options

	runningMutex sync.Mutex
	running      bool
	conn         net.Conn
	stream       *stream.Stream
}

// New creates a new Connection.
func New(address string,
	encodeDecoder stream.EncodeDecoder,
	marshalUnmarshaler stream.MarshalUnmarshaler,
	handler stream.Handler, networkHandler stream.NetworkHandler, opts ...Option) (*Connection, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	var options options
	for _, opt := range opts {
		opt(&options)
	}

	return &Connection{
		address:            address,
		encodeDecoder:      encodeDecoder,
		marshalUnmarshaler: marshalUnmarshaler,
		handler:            handler,
		networkHandler:     networkHandler,
		options:            &options,
	}, nil
}

// Connect connects to the remote host and handles the connection using a stream.Stream.
func (c *Connection) Connect() error {
	c.runningMutex.Lock()
	defer c.runningMutex.Unlock()

	if c.running {
		return errors.New("connection is already running")
	}

	dialer := net.Dialer{Timeout: c.options.dialTimeout}

	var conn net.Conn
	var err error

	if c.options.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&dialer, "tcp", c.address, c.options.tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", c.address)
	}

	if err != nil {
		return err
	}

	if c.options.onConnectFunc != nil {
		c.options.onConnectFunc(conn)
	}

	stream, err := stream.New(conn, c.encodeDecoder, c.marshalUnmarshaler, c.handler, c.networkHandler, c.options.streamOptions...)
	if err != nil {
		return err
	}

	stream.Start()

	c.conn = conn
	c.stream = stream
	c.running = true

	return nil
}

// Request sends a request to the remote host and waits for a response.
// It returns an error if the connection is not running.
func (c *Connection) Request(message stream.Message) (stream.Message, error) {
	c.runningMutex.Lock()
	if !c.running {
		c.runningMutex.Unlock()
		return stream.Message{}, errors.New("connection is not running")
	}
	c.runningMutex.Unlock()

	return c.stream.Request(message)
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.runningMutex.Lock()
	defer func() {
		c.conn = nil
		c.stream = nil
		c.running = false
		c.runningMutex.Unlock()
	}()

	if !c.running {
		return errors.New("connection is not running")
	}

	if c.options.onCloseFunc != nil {
		c.options.onCloseFunc(c.conn)
	}

	return c.stream.Stop()
}
