# TCP packages

## `tcp/stream` package

This package is heavily inspired by https://github.com/moov-io/iso8583-connection.
The key differences are:

- The messaging protocol is not tied to ISO8583 or the usage of [moov-io/iso8583](https://github.com/moov-io/iso8583).
- It provides a higher level abstraction for sending and receiving messages over a connection.
- It ensures that when Stop is called, the function will only return once all messages sent using `Request`
  or any `stream.Message` forwarded to the `stream.Handler` or `stream.NetworkHandler` has finished being handled.

It is also decoupled from the underlying transport layer, so it can be used with any net.Conn implementation
whether it is created using `net.Dial` or `net.Listen`.

For an example of how to use this package, see a [testable example](./stream/example_test.go).

### `stream/acceptor` package

This package provides a thin layer on top of the `stream` package to provide a higher level abstraction for
accepting connections and handling messages.

### `stream/connection` package

This package provides a thin layer on top of the `stream` package to provide a higher level abstraction for
connecting to a remote host and handling messages.
It exposes part of the `stream` API to allow the user to send messages. 