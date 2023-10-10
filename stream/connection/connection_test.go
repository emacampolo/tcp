package connection_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emacampolo/tcp/stream"
	"github.com/emacampolo/tcp/stream/connection"
	"github.com/emacampolo/tcp/stream/internal"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	Addr   string
	Server *internal.TestServer
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
					encoder.Encode(stream.Message{ID: message.ID, Payload: "pong", IsNetwork: false, IsResponse: true})
				default:
					log.Fatalf("server: received unknown message: %v", message.Payload)
				}
			}()
		}
	}
}

func newTestServerWithListener(listener net.Listener) (*testServer, error) {
	var testServer testServer
	server := internal.NewTestServer(listener, testServer.Handler())
	testServer.Server = server
	testServer.Addr = listener.Addr().String()

	go server.Listen()

	return &testServer, nil
}

func newTestServer() (*testServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return newTestServerWithListener(listener)
}

func newTestTLSServer(tlsConfig *tls.Config) (*testServer, error) {
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		return nil, err
	}
	return newTestServerWithListener(listener)
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

func TestConnection_Connect(t *testing.T) {
	server, err := newTestServer()
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	conn, err := connection.New(server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler)
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}

	defer conn.Close()

	if err := conn.Connect(); err != nil {
		t.Fatalf("error connecting to server: %v", err)
	}

	message, err := conn.Request(stream.Message{ID: "1", Payload: "ping"})
	if err != nil {
		t.Fatalf("error sending request: %v", err)
	}

	if message.Payload != "pong" {
		t.Fatalf("expected message payload to be 'pong', got %v", message.Payload)
	}
}

func TestConnection_WithOnConnectFunc(t *testing.T) {
	server, err := newTestServer()
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	var onConnectCalled atomic.Bool
	conn, err := connection.New(server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler, connection.WithOnConnectFunc(func(conn net.Conn) {
		onConnectCalled.Store(true)
	}))
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}

	defer conn.Close()

	if err := conn.Connect(); err != nil {
		t.Fatalf("error connecting to server: %v", err)
	}

	if !onConnectCalled.Load() {
		t.Fatal("expected onConnect to be called")
	}
}

func TestConnection_WithOnCloseFunc(t *testing.T) {
	server, err := newTestServer()
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	var onDisconnectCalled atomic.Bool
	conn, err := connection.New(server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler, connection.WithOnCloseFunc(func(conn net.Conn) {
		onDisconnectCalled.Store(true)
	}))
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}

	defer conn.Close()

	if err := conn.Connect(); err != nil {
		t.Fatalf("error connecting to server: %v", err)
	}

	conn.Close()

	if !onDisconnectCalled.Load() {
		t.Fatal("expected onDisconnect to be called")
	}
}

func TestConnection_WithDialTimeout(t *testing.T) {
	server, err := newTestServer()
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	conn, err := connection.New(server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler,
		connection.WithDialTimeout(1*time.Nanosecond))
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}

	defer conn.Close()

	err = conn.Connect()

	var expectedError net.Error
	require.ErrorAs(t, err, &expectedError)
	require.True(t, expectedError.Timeout())
}

// testCert is a PEM-encoded TLS cert with SAN IPs
// "127.0.0.1" and "[::1]", expiring at Jan 29 16:00:00 2084 GMT.
// generated from src/crypto/tls:
// go run generate_cert.go  --rsa-bits 1024 --host 127.0.0.1,::1,example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var testCert = []byte(`-----BEGIN CERTIFICATE-----
MIICEzCCAXygAwIBAgIQMIMChMLGrR+QvmQvpwAU6zANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMCAXDTcwMDEwMTAwMDAwMFoYDzIwODQwMTI5MTYw
MDAwWjASMRAwDgYDVQQKEwdBY21lIENvMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCB
iQKBgQDuLnQAI3mDgey3VBzWnB2L39JUU4txjeVE6myuDqkM/uGlfjb9SjY1bIw4
iA5sBBZzHi3z0h1YV8QPuxEbi4nW91IJm2gsvvZhIrCHS3l6afab4pZBl2+XsDul
rKBxKKtD1rGxlG4LjncdabFn9gvLZad2bSysqz/qTAUStTvqJQIDAQABo2gwZjAO
BgNVHQ8BAf8EBAMCAqQwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUw
AwEB/zAuBgNVHREEJzAlggtleGFtcGxlLmNvbYcEfwAAAYcQAAAAAAAAAAAAAAAA
AAAAATANBgkqhkiG9w0BAQsFAAOBgQCEcetwO59EWk7WiJsG4x8SY+UIAA+flUI9
tyC4lNhbcF2Idq9greZwbYCqTTTr2XiRNSMLCOjKyI7ukPoPjo16ocHj+P3vZGfs
h1fIw3cSS2OolhloGw/XM6RWPWtPAlGykKLciQrBru5NAPvCMsb/I1DAceTiotQM
fblo6RBxUQ==
-----END CERTIFICATE-----`)

// testKey is the private key for testCert.
var testKey = []byte(testingKey(`-----BEGIN RSA TESTING KEY-----
MIICXgIBAAKBgQDuLnQAI3mDgey3VBzWnB2L39JUU4txjeVE6myuDqkM/uGlfjb9
SjY1bIw4iA5sBBZzHi3z0h1YV8QPuxEbi4nW91IJm2gsvvZhIrCHS3l6afab4pZB
l2+XsDulrKBxKKtD1rGxlG4LjncdabFn9gvLZad2bSysqz/qTAUStTvqJQIDAQAB
AoGAGRzwwir7XvBOAy5tM/uV6e+Zf6anZzus1s1Y1ClbjbE6HXbnWWF/wbZGOpet
3Zm4vD6MXc7jpTLryzTQIvVdfQbRc6+MUVeLKwZatTXtdZrhu+Jk7hx0nTPy8Jcb
uJqFk541aEw+mMogY/xEcfbWd6IOkp+4xqjlFLBEDytgbIECQQDvH/E6nk+hgN4H
qzzVtxxr397vWrjrIgPbJpQvBsafG7b0dA4AFjwVbFLmQcj2PprIMmPcQrooz8vp
jy4SHEg1AkEA/v13/5M47K9vCxmb8QeD/asydfsgS5TeuNi8DoUBEmiSJwma7FXY
fFUtxuvL7XvjwjN5B30pNEbc6Iuyt7y4MQJBAIt21su4b3sjXNueLKH85Q+phy2U
fQtuUE9txblTu14q3N7gHRZB4ZMhFYyDy8CKrN2cPg/Fvyt0Xlp/DoCzjA0CQQDU
y2ptGsuSmgUtWj3NM9xuwYPm+Z/F84K6+ARYiZ6PYj013sovGKUFfYAqVXVlxtIX
qyUBnu3X9ps8ZfjLZO7BAkEAlT4R5Yl6cGhaJQYZHOde3JEMhNRcVFMO8dJDaFeo
f9Oeos0UUothgiDktdQHxdNEwLjQf7lJJBzV+5OtwswCWA==
-----END RSA TESTING KEY-----`))

func testingKey(s string) string { return strings.ReplaceAll(s, "TESTING KEY", "PRIVATE KEY") }

func TestConnection_WithTLSConfig(t *testing.T) {
	cert, err := tls.X509KeyPair(testCert, testKey)
	if err != nil {
		t.Fatalf("error creating tls certificate: %v", err)
	}

	tlsConfig := tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	server, err := newTestTLSServer(&tlsConfig)
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	certificate, err := x509.ParseCertificate(tlsConfig.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	certpool := x509.NewCertPool()
	certpool.AddCert(certificate)

	conn, err := connection.New(server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicNetworkHandler,
		connection.WithTLSConfig(&tls.Config{
			RootCAs: certpool,
		}))
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}

	defer conn.Close()

	if err := conn.Connect(); err != nil {
		t.Fatalf("error connecting to server: %v", err)
	}
}
