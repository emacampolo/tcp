package acceptor

import "github.com/emacampolo/tcp/stream"

type options struct {
	onConnectFunc OnConnectFunc
	onCloseFunc   OnCloseFunc

	streamOptions []stream.Option
}

// Option is a function that configures a Connection.
type Option func(*options)

// WithOnConnectFunc sets a function to be called when a connection is established.
func WithOnConnectFunc(onConnectFunc OnConnectFunc) Option {
	return func(o *options) {
		o.onConnectFunc = onConnectFunc
	}
}

// WithOnCloseFunc sets a function to be called when a connection is closed.
func WithOnCloseFunc(onCloseFunc OnCloseFunc) Option {
	return func(o *options) {
		o.onCloseFunc = onCloseFunc
	}
}

// WithStreamOptions sets the stream options.
func WithStreamOptions(streamOptions ...stream.Option) Option {
	return func(o *options) {
		o.streamOptions = streamOptions
	}
}
