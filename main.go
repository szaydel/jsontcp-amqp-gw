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

type AMQPServer struct {
	uri          string
	exchangeName string
	exchangeType string
	routingKey   string
	reliable     bool
	connection   *amqp.Connection
	channel      *amqp.Channel
	connected    bool
}

func (s AMQPServer) String() string {
	return fmt.Sprintf("uri=%s exchange=%s routingKey=%s", s.uri, s.exchangeName, s.routingKey)
}
func (s *AMQPServer) Connect() error {
	// This function dials, connects, declares,
	log.Printf("dialing %q", s.uri)
	connection, err := amqp.Dial(s.uri)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", s.exchangeType, s.exchangeName)
	if err := channel.ExchangeDeclare(
		s.exchangeName, // name
		s.exchangeType, // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		connection.Close()
		return fmt.Errorf("Exchange Declare: %s", err)
	}
	s.connection = connection
	s.channel = channel
	return nil
}

func (s *AMQPServer) ConnectWithRetries() {
	for {
		err := s.Connect()
		if err == nil {
			return
		}
		log.Printf("Error connecting to AMQP: %s", err)
		time.Sleep(10 * time.Second)
	}
}

func (s *AMQPServer) ConnectIfNeeded() {
	if s.connection == nil {
		s.ConnectWithRetries()
	}
}
func (s *AMQPServer) Close() {
	if s.connection != nil {
		s.connection.Close()
		s.connection = nil
	}
}
func (s *AMQPServer) Reconnect() {
	s.Close()
	s.ConnectWithRetries()
}

func (s *AMQPServer) Publish(rec []byte) error {
	//log.Printf("Publishing %s", string(rec))
	return s.channel.Publish(
		s.exchangeName, // publish to an exchange
		s.routingKey,   // routing to 0 or more queues
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			Body:            rec,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	)
}

func (s *AMQPServer) PublishWithRetries(rec []byte) {
	for {
		s.ConnectIfNeeded()
		err := s.Publish(rec)
		if err == nil {
			return
		}
		log.Printf("Exchange Publish: %s", err)
		s.Reconnect()
	}
}

func receive(lineChan chan *bytes.Buffer, serverConn AMQPServer) error {
	var b *bytes.Buffer

	for b = range lineChan {
		serverConn.PublishWithRetries(b.Bytes())
		if b.Cap() <= 1024*1024 {
			b.Reset()
			bufPool.Put(b)
		}
	}
	serverConn.Close()
	return nil
}

func main() {
	var port int
	var addr string
	var serverConn AMQPServer
	flag.StringVar(&serverConn.uri, "uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	flag.StringVar(&serverConn.exchangeName, "exchange", "test-exchange", "Durable AMQP exchange name")
	flag.StringVar(&serverConn.exchangeType, "exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	flag.StringVar(&serverConn.routingKey, "key", "test-key", "AMQP routing key")

	flag.StringVar(&addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&port, "port", 9000, "Port to listen on")
	flag.Parse()

	log.Printf("Publishing to %s", serverConn)
	lineChan := make(chan *bytes.Buffer, 1000)
	go listen(addr, port, lineChan)

	for {
		err := receive(lineChan, serverConn)
		if err != nil {
			log.Printf("Error sending to amqp: %v", err)
		}
		time.Sleep(5 * time.Second)
	}

}
