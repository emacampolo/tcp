package stream

import (
	"fmt"
	"time"
)

type options struct {
	writeTimeoutDuration   time.Duration
	writeTimeoutFunc       WriteTimeoutFunc
	readTimeoutDuration    time.Duration
	readTimeoutFunc        ReadTimeoutFunc
	requestTimeoutDuration time.Duration

	errorHandler ErrorHandler
}

// Option is a function that configures a Stream.
type Option func(*options) error

var DefaultOptions = []Option{
	WithWriteTimeout(5*time.Second, nil),
	WithReadTimeout(1*time.Minute, nil),
	WithRequestTimeout(1 * time.Second),
}

// WithWriteTimeout sets a duration after which the Stream is considered idle
// and the idle function is called.
// The duration must be greater than 0.
func WithWriteTimeout(duration time.Duration, writeTimeoutFunc WriteTimeoutFunc) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("write timeout duration must be greater than 0")
		}

		o.writeTimeoutDuration = duration
		o.writeTimeoutFunc = writeTimeoutFunc
		return nil
	}
}

// WithReadTimeout sets a duration for which the Stream will wait for a message to be received.
// If duration is reached, the read timeout function is called.
// The duration must be greater than 0.
func WithReadTimeout(duration time.Duration, readTimeoutFunc ReadTimeoutFunc) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("read timeout duration must be greater than 0")
		}

		o.readTimeoutDuration = duration
		o.readTimeoutFunc = readTimeoutFunc
		return nil
	}
}

// WithRequestTimeout sets a duration for which the Stream will wait for a message to be sent.
// The duration must be greater than 0.
func WithRequestTimeout(duration time.Duration) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("request timeout duration must be greater than 0")
		}

		o.requestTimeoutDuration = duration
		return nil
	}
}

// WithErrorHandler sets a function to be called when an error occurs while trying
// to unmarshal a message or when an error occurs while trying to write a message to the Stream.
// The error handler is called with the error that occurred.
func WithErrorHandler(errorHandler ErrorHandler) Option {
	return func(o *options) error {
		o.errorHandler = errorHandler
		return nil
	}
}
