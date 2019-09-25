package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	heartbeat    time.Duration
	reliable     bool
	confirm      bool
	connection   *amqp.Connection
	channel      *amqp.Channel

	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
}

func (s AMQPServer) String() string {
	return fmt.Sprintf("uri=%s exchange=%s routingKey=%s", s.uri, s.exchangeName, s.routingKey)
}
func (s *AMQPServer) Connect() error {
	// This function dials, connects, declares,
	log.Printf("dialing %q", s.uri)
	connection, err := amqp.DialConfig(s.uri,
		amqp.Config{
			Heartbeat: s.heartbeat, // broker will likely be lower
		},
	)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return fmt.Errorf("Error getting channel: %s", err)
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

	s.notifyConnClose = make(chan *amqp.Error)
	s.notifyChanClose = make(chan *amqp.Error)

	connection.NotifyClose(s.notifyConnClose)
	channel.NotifyClose(s.notifyChanClose)
	if s.confirm {
		s.notifyConfirm = make(chan amqp.Confirmation, 1)
		channel.NotifyPublish(s.notifyConfirm)
		channel.Confirm(false) // false here implies noWait = false
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
	if s.connection == nil {
		return
	}
	if err := s.connection.Close(); err != nil {
		log.Printf("Error closing connection to AMQP: %v", err)
		return
	}
	s.connection = nil
}

func (s *AMQPServer) Reconnect() {
	log.Printf("Reconnecting to AMQP server")
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
		log.Printf("Begin Exchange publish %d bytes", len(rec))
		if err := s.Publish(rec); err != nil {
			log.Printf("Error publishing %d bytes to Exchange: %v", len(rec), err)
		}
		if !s.confirm {
			return
		}
		// select here is only for cases where reliable delivery is used.
		select {
		case confirm := <-s.notifyConfirm:
			if confirm.Ack {
				log.Printf("Confirmed Exchange publish %d bytes %v", len(rec), confirm)
				return
			}
		case <-time.After(1 * time.Second):
			// this just delays the loop by 1 second on retries
		}
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
	return nil // never reached
}

func main() {
	var port int
	var addr string
	var serverConn AMQPServer
	flag.StringVar(&serverConn.uri, "uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	flag.StringVar(&serverConn.exchangeName, "exchange", "test-exchange", "Durable AMQP exchange name")
	flag.StringVar(&serverConn.exchangeType, "exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	flag.StringVar(&serverConn.routingKey, "key", "test-key", "AMQP routing key")
	flag.DurationVar(&serverConn.heartbeat, "heartbeat interval", 60*time.Second, "Time in seconds to set heartbeat timeout to")
	flag.BoolVar(&serverConn.confirm, "confirm", false, "Should each message be confirmed?")
	flag.StringVar(&addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&port, "port", 9000, "Port to listen on")
	flag.Parse()

	// Setup signals and shutdown channel
	var (
		stop    chan struct{}  // Channel to stop/close server and exit
		sighup  chan struct{}  // Channel to trigger reconnect on SIGHUP
		signals chan os.Signal // OS signals arrive on this channel
	)
	signals = make(chan os.Signal, 1)
	stop = make(chan struct{})
	sighup = make(chan struct{})
	signal.Notify(signals)
	go signalWatch(signals, stop, sighup)

	log.Printf("Publishing to %s", serverConn)
	lineChan := make(chan *bytes.Buffer, 1000)
	go listen(addr, port, lineChan)

	// Setup handling of events like close notifications from broker or signals.
	go eventHandler(&serverConn, stop, sighup)

	for {
		err := receive(lineChan, serverConn)
		if err != nil {
			log.Printf("Error sending to AMQP: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func eventHandler(serverConn *AMQPServer, stop, sighup chan struct{}) {
	for {
		select {
		case <-stop:
			log.Printf("Received shutdown signal; gateway shutting down...")
			serverConn.Close() // This should flush any buffers
			os.Exit(0)
		case <-sighup:
			log.Printf("Received SIGHUP signal; asking server to reconnect")
			serverConn.Reconnect()
		case msg := <-serverConn.notifyChanClose:
			log.Printf("Channel Close Notification: %v", msg)
		case msg := <-serverConn.notifyConnClose:
			log.Printf("Connection Close Notification: %v", msg)
			serverConn.Reconnect()
		default:
			time.Sleep(2 * time.Second) // At most shutdown should take 2 secs
		}
	}
}

func signalWatch(signals chan os.Signal, stop, sighup chan struct{}) {
	for {
		s := <-signals
		switch s {
		case syscall.SIGTERM, syscall.SIGINT:
			stop <- struct{}{}
		case syscall.SIGHUP:
			sighup <- struct{}{}
		default:
			log.Printf("Caught signal: %v\n", s)
		}
	}
}
