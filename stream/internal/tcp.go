package internal

import (
	"context"
	"net"
	"sync"
)

// The Handler type allows clients to process incoming tcp connections.
// The provided context is canceled on Shutdown.
type Handler func(ctx context.Context, conn net.Conn)

// A TestServer defines parameters for running an TCP server in tests.
type TestServer struct {
	handler Handler

	mu      sync.Mutex
	wg      sync.WaitGroup
	closing bool

	listener  net.Listener
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func NewTestServer(listener net.Listener, handler Handler) *TestServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &TestServer{
		handler:   handler,
		listener:  listener,
		ctx:       ctx,
		ctxCancel: cancel,
	}
}

// Listen accepts incoming connections on the listener l, creating a new service goroutine for each message.
// The service goroutines read requests with a Decoder, then forward the request to the Handler and then
// write the response back to the client using an Encoder.
func (s *TestServer) Listen() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.wg.Add(1)
		s.mu.Unlock()

		go func(c net.Conn) {
			s.handler(s.ctx, c)
			s.wg.Done()
			_ = c.Close()
		}(conn)
	}
}

func (s *TestServer) Shutdown() {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	s.mu.Unlock()

	_ = s.listener.Close()

	// Canceling context.
	s.ctxCancel()

	// Wait for active connections to close.
	s.wg.Wait()
}
