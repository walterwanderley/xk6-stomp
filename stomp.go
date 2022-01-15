package stomp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-stomp/stomp/v3"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/stomp".
func init() {
	modules.Register("k6/x/stomp", new(Stomp))
}

// Stomp is the k6 extension for a Stomp client.
type Stomp struct{}

const (
	defaultProtocol = "tcp"
	defaultTimeout  = "10s"
)

type Options struct {
	Addr     string
	Protocol string
	Timeout  string
	TLS      bool
	Headers  map[string]string

	User string
	Pass string

	MessageSendTimeout string
	ReceiptTimeout     string
}

// Client is the Stomp conn wrapper.
type Client struct {
	conn *stomp.Conn
	ctx  context.Context
}

// XClient represents the Client constructor (i.e. `new stomp.Client()`) and
// returns a new Stomp client object.
func (s *Stomp) XClient(ctxPtr *context.Context, opts *Options) interface{} {
	rt := common.GetRuntime(*ctxPtr)

	if opts.Protocol == "" {
		opts.Protocol = defaultProtocol
	}
	if opts.Timeout == "" {
		opts.Timeout = defaultTimeout
	}
	timeout, err := time.ParseDuration(opts.Timeout)
	if err != nil {
		common.Throw(rt, err)
		return err
	}

	var netConn io.ReadWriteCloser
	if opts.TLS {
		netConn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout},
			opts.Protocol, opts.Addr, nil)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
	} else {
		netConn, err = net.DialTimeout(opts.Protocol, opts.Addr, timeout)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
	}
	connOpts := make([]func(*stomp.Conn) error, 0)
	if opts.User != "" || opts.Pass != "" {
		connOpts = append(connOpts, stomp.ConnOpt.Login(opts.User, opts.Pass))
	}
	for k, v := range opts.Headers {
		connOpts = append(connOpts, stomp.ConnOpt.Header(k, v))
	}
	if opts.MessageSendTimeout != "" {
		timeout, err := time.ParseDuration(opts.MessageSendTimeout)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
		connOpts = append(connOpts, stomp.ConnOpt.MsgSendTimeout(timeout))
	}
	if opts.ReceiptTimeout != "" {
		timeout, err := time.ParseDuration(opts.ReceiptTimeout)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
		connOpts = append(connOpts, stomp.ConnOpt.RcvReceiptTimeout(timeout))
	}

	stompConn, err := stomp.Connect(netConn, connOpts...)
	if err != nil {
		common.Throw(rt, err)
		return err
	}

	return common.Bind(rt, &Client{conn: stompConn, ctx: *ctxPtr}, ctxPtr)
}

// Disconnect will disconnect from the STOMP server.
func (c *Client) Disconnect() error {
	return c.conn.Disconnect()
}

// Send sends a message to the STOMP server.
func (c *Client) Send(destination, contentType string, body []byte) error {
	return c.conn.Send(destination, contentType, body)
}

// Subscribe creates a subscription on the STOMP server.
func (c *Client) Subscribe(destination string, ackMode string) (*Subscription, error) {
	var mode stomp.AckMode
	switch ackMode {
	case "client":
		mode = stomp.AckClient
	case "client-individual":
		mode = stomp.AckClientIndividual
	case "", "auto":
		mode = stomp.AckAuto
	default:
		return nil, fmt.Errorf("ackMode should be 'auto', 'client' or 'client-individual'")
	}
	sub, err := c.conn.Subscribe(destination, mode)
	if err != nil {
		return nil, err
	}
	return &Subscription{sub, c.ctx}, nil
}

// Ack acknowledges a message received from the STOMP server.
func (c *Client) Ack(m *Message) error {
	return c.conn.Ack(m.Message)
}

// Nack indicates to the server that a message was not received
// by the client.
func (c *Client) Nack(m *Message) error {
	return c.conn.Nack(m.Message)
}

// Server returns the STOMP server identification.
func (c *Client) Server() string {
	return c.conn.Server()
}

// Session returns the session identifier.
func (c *Client) Session() string {
	return c.conn.Session()
}

// Begin is used to start a transaction.
func (c *Client) Begin() *Transaction {
	return &Transaction{c.conn.Begin()}
}

// BeginWithError is used to start a transaction, but also returns the error.
func (c *Client) BeginWithError() (*Transaction, error) {
	tx, err := c.conn.BeginWithError()
	return &Transaction{tx}, err
}
