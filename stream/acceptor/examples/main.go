package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/emacampolo/tcp/stream"
	"github.com/emacampolo/tcp/stream/acceptor"
)

type encodeDecoder struct{}

func (ed *encodeDecoder) Encode(writer io.Writer, message []byte) error {
	_, err := writer.Write(message)
	return err
}

func (ed *encodeDecoder) Decode(reader io.Reader) ([]byte, error) {
	return bufio.NewReader(reader).ReadBytes('\n')
}

type marshalUnmarshal struct{}

func (mu *marshalUnmarshal) Marshal(message stream.Message) ([]byte, error) {
	return []byte(message.Payload.(string) + "\n"), nil
}

func (mu *marshalUnmarshal) Unmarshal(data []byte) (stream.Message, error) {
	return stream.Message{Payload: data}, nil
}

var alwaysPanicNetworkHandler = func(sender stream.Sender, message stream.Message) {
	panic("always fail")
}

func main() {
	handler := func(sender stream.Sender, message stream.Message) {
		response := bytes.TrimSpace(message.Payload.([]byte))
		message.Payload = fmt.Sprintf("Received message: %q", string(response))
		if err := sender.Send(message); err != nil {
			panic(err)
		}
	}

	acceptor, err := acceptor.New(
		"127.0.0.1:0",
		&encodeDecoder{},
		&marshalUnmarshal{},
		handler,
		alwaysPanicNetworkHandler)

	if err != nil {
		panic(err)
	}

	go acceptor.Accept()
	<-acceptor.Running()

	fmt.Printf("👋 Listening to 127.0.0.1:%d for connections\n", acceptor.Port())

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
	fmt.Printf("☠️  Shutting down")

	acceptor.Close()
}
