package acceptor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/emacampolo/tcp/stream"
)

// OnConnectFunc is called synchronously when a connection is established.
type OnConnectFunc func(conn net.Conn)

// OnCloseFunc is called synchronously when a connection is closed.
type OnCloseFunc func(conn net.Conn)

// Acceptor is a TCP server that accepts new connections and delegates the handling of the connection to a stream.Stream.
type Acceptor struct {
	address            string
	encodeDecoder      stream.EncodeDecoder
	marshalUnmarshaler stream.MarshalUnmarshaler
	handler            stream.Handler
	networkHandler     stream.NetworkHandler
	options            *options

	listener  net.Listener
	port      int
	ctx       context.Context
	ctxCancel context.CancelFunc

	running chan struct{}
	wg      sync.WaitGroup

	mu      sync.Mutex
	closing bool
}

// New creates a new Acceptor.
func New(
	address string,
	encodeDecoder stream.EncodeDecoder,
	marshalUnmarshaler stream.MarshalUnmarshaler,
	handler stream.Handler, networkHandler stream.NetworkHandler,
	opt ...Option) (*Acceptor, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	var options options
	for _, opts := range opt {
		opts(&options)
	}

	return &Acceptor{
		address:            address,
		encodeDecoder:      encodeDecoder,
		marshalUnmarshaler: marshalUnmarshaler,
		handler:            handler,
		networkHandler:     networkHandler,
		options:            &options,
		running:            make(chan struct{}),
	}, nil
}

// Accept starts accepting connections.
func (a *Acceptor) Accept() error {
	listener, err := net.Listen("tcp", a.address)
	if err != nil {
		return err
	}

	a.port = listener.Addr().(*net.TCPAddr).Port
	a.ctx, a.ctxCancel = context.WithCancel(context.Background())
	a.listener = listener

	// Signal that the application is ready to enqueue SYN messages.
	close(a.running)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		if a.options.onConnectFunc != nil {
			a.options.onConnectFunc(conn)
		}

		a.mu.Lock()
		if a.closing {
			a.mu.Unlock()
			return conn.Close()
		}

		a.wg.Add(1)
		a.mu.Unlock()

		go func(netConn net.Conn) {
			defer func() {
				if a.options.onCloseFunc != nil {
					a.options.onCloseFunc(netConn)
				}
				a.wg.Done()
			}()

			stream, err := stream.New(netConn, a.encodeDecoder, a.marshalUnmarshaler, a.handler, a.networkHandler, a.options.streamOptions...)
			if err != nil {
				panic(fmt.Sprintf("failed to create stream: %v", err))
			}

			stream.Start()
			select {
			case <-a.ctx.Done():
				_ = stream.Stop()
			case <-stream.Done():
			}
		}(conn)
	}
}

// Port returns the port that the Acceptor is listening on.
func (a *Acceptor) Port() int {
	return a.port
}

// Running returns a channel that is closed when the Acceptor is ready to accept connections.
func (a *Acceptor) Running() <-chan struct{} {
	return a.running
}

// Close stops accepting new connections and closes all existing connections. It is idempotent.
func (a *Acceptor) Close() {
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		return
	}
	a.closing = true
	a.mu.Unlock()

	_ = a.listener.Close()
	a.ctxCancel()
	a.wg.Wait()
}
