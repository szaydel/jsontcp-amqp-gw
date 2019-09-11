package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type AMQPServer struct {
	uri          string
	exchangeName string
	exchangeType string
	routingKey   string
	reliable     bool
}

const bufferSize int = 128 * 1024

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		buf.Grow(bufferSize * 2)
		return buf
	},
}

func enableKeepAlive(conn net.Conn) error {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("Bad conn type: %T", conn)
	}
	if err := tcp.SetKeepAlive(true); err != nil {
		return err
	}
	if err := tcp.SetKeepAlivePeriod(50 * time.Second); err != nil {
		return err
	}
	return nil
}

func listen(addr string, port int, lineChan chan *bytes.Buffer) {
	bind := fmt.Sprintf("%s:%d", addr, port)
	log.Printf("Listening on %s", bind)
	l, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatalf("Error listening: %v", err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("Error accepting: %v", err)
		}
		log.Printf("New connection from %s", conn.RemoteAddr())
		if err := enableKeepAlive(conn); err != nil {
			log.Fatalf("Error enabling keepalive: %v", err)
		}
		go handleLog(conn, lineChan)
	}
}

func handleLog(conn net.Conn, lineChan chan *bytes.Buffer) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, bufferSize)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		buf := scanner.Bytes()
		outbuf := bufPool.Get().(*bytes.Buffer)
		outbuf.Write(buf)
		lineChan <- outbuf
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
	log.Printf("Connection from %s closed", conn.RemoteAddr())
}

func receive(lineChan chan *bytes.Buffer, serverInfo AMQPServer) error {
	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	log.Printf("dialing %q", serverInfo.uri)
	connection, err := amqp.Dial(serverInfo.uri)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", serverInfo.exchangeType, serverInfo.exchangeName)
	if err := channel.ExchangeDeclare(
		serverInfo.exchangeName, // name
		serverInfo.exchangeType, // type
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // noWait
		nil,                     // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	var b *bytes.Buffer
	for {
		b = <-lineChan
		//log.Printf("Publishing %s", string(b.Bytes()))
		if err = channel.Publish(
			serverInfo.exchangeName, // publish to an exchange
			serverInfo.routingKey,   // routing to 0 or more queues
			false,                   // mandatory
			false,                   // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            b.Bytes(),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
			},
		); err != nil {
			lineChan <- b
			return fmt.Errorf("Exchange Publish: %s", err)
		}

		if b.Cap() <= 1024*1024 {
			b.Reset()
			bufPool.Put(b)
		}
	}
	return nil
}

func main() {
	var port int
	var addr string
	var serverInfo AMQPServer
	flag.StringVar(&serverInfo.uri, "uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	flag.StringVar(&serverInfo.exchangeName, "exchange", "test-exchange", "Durable AMQP exchange name")
	flag.StringVar(&serverInfo.exchangeType, "exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	flag.StringVar(&serverInfo.routingKey, "key", "test-key", "AMQP routing key")

	flag.StringVar(&addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&port, "port", 9000, "Port to listen on")
	flag.Parse()

	log.Printf("Publishing to %+v", serverInfo)
	lineChan := make(chan *bytes.Buffer, 1000)
	go listen(addr, port, lineChan)

	for {
		err := receive(lineChan, serverInfo)
		if err != nil {
			log.Printf("Error sending to amqp: %v", err)
		}
		time.Sleep(5 * time.Second)
	}

}
