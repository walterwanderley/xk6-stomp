package stomp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/stats"
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
	Path     string
	Timeout  string
	TLS      bool
	Headers  map[string]string

	User string
	Pass string

	MessageSendTimeout string
	ReceiptTimeout     string

	Heartbeat struct {
		Incoming  string
		Outcoming string
	}
}

// Client is the Stomp conn wrapper.
type Client struct {
	conn *stomp.Conn
}

type SendOptions struct {
	Headers map[string]string
	Receipt bool
}

// Listener is a callback function to execute when the subscription reads a message
type Listener func(*Message)

type SubscribeOptions struct {
	Ack      string
	Headers  map[string]string
	Id       string
	Listener Listener
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

	netConn, err := openNetConn(opts)
	if err != nil {
		common.Throw(rt, err)
		return err
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
	if opts.Heartbeat.Incoming != "" && opts.Heartbeat.Outcoming != "" {
		sendTimeout, err := time.ParseDuration(opts.Heartbeat.Outcoming)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
		receiveTimeout, err := time.ParseDuration(opts.Heartbeat.Incoming)
		if err != nil {
			common.Throw(rt, err)
			return err
		}
		connOpts = append(connOpts, stomp.ConnOpt.HeartBeat(sendTimeout, receiveTimeout))
	}

	stompConn, err := stomp.Connect(netConn, connOpts...)
	if err != nil {
		common.Throw(rt, err)
		return err
	}

	return common.Bind(rt, &Client{conn: stompConn}, ctxPtr)
}

func openNetConn(opts *Options) (io.ReadWriteCloser, error) {
	timeout, err := time.ParseDuration(opts.Timeout)
	if err != nil {
		return nil, err
	}
	switch {
	case opts.Protocol == "ws" || opts.Protocol == "wss":
		return openWSConn(opts, timeout)
	case opts.TLS:
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout},
			opts.Protocol, opts.Addr, nil)
	default:
		return net.DialTimeout(opts.Protocol, opts.Addr, timeout)
	}
}

// Disconnect will disconnect from the STOMP server.
func (c *Client) Disconnect() error {
	return c.conn.Disconnect()
}

// Send sends a message to the STOMP server.
func (c *Client) Send(ctx context.Context, destination, contentType string, body []byte, opts *SendOptions) (err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		reportStats(ctx, sendMessageTiming, nil, now, stats.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(ctx, sendMessageErrors, nil, now, 1)
		} else {
			reportStats(ctx, dataSent, nil, now, float64(len(body)))
			reportStats(ctx, sendMessage, nil, now, 1)
		}
	}()
	if opts == nil {
		opts = new(SendOptions)
	}
	var sendOpts []func(*frame.Frame) error
	if opts.Receipt {
		sendOpts = append(sendOpts, stomp.SendOpt.Receipt)
	}
	for k, v := range opts.Headers {
		sendOpts = append(sendOpts, stomp.SendOpt.Header(k, v))
	}

	err = c.conn.Send(destination, contentType, body, sendOpts...)
	return
}

// Subscribe creates a subscription on the STOMP server.
func (c *Client) Subscribe(ctx context.Context, destination string, opts *SubscribeOptions) (*Subscription, error) {
	if opts == nil {
		opts = new(SubscribeOptions)
	}
	var mode stomp.AckMode
	switch opts.Ack {
	case "client":
		mode = stomp.AckClient
	case "client-individual":
		mode = stomp.AckClientIndividual
	case "", "auto":
		mode = stomp.AckAuto
	default:
		return nil, fmt.Errorf("ackMode should be 'auto', 'client' or 'client-individual'")
	}
	var subOpts []func(*frame.Frame) error
	for k, v := range opts.Headers {
		subOpts = append(subOpts, stomp.SubscribeOpt.Header(k, v))
	}
	if opts.Id != "" {
		subOpts = append(subOpts, stomp.SubscribeOpt.Id(opts.Id))
	}
	sub, err := c.conn.Subscribe(destination, mode, subOpts...)
	if err != nil {
		return nil, err
	}
	return NewSubscription(ctx, sub, opts.Listener), nil
}

// Ack acknowledges a message received from the STOMP server.
func (c *Client) Ack(ctx context.Context, m *Message) error {
	now := time.Now()
	err := c.conn.Ack(m.Message)
	if err != nil {
		reportStats(ctx, ackMessageErrors, nil, now, 1)
	} else {
		reportStats(ctx, ackMessage, nil, now, 1)
	}
	return err
}

// Nack indicates to the server that a message was not received
// by the client.
func (c *Client) Nack(ctx context.Context, m *Message) error {
	now := time.Now()
	err := c.conn.Nack(m.Message)
	if err != nil {
		reportStats(ctx, nackMessageErrors, nil, now, 1)
	} else {
		reportStats(ctx, nackMessage, nil, now, 1)
	}
	return err
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
func (c *Client) Begin(ctx context.Context) *Transaction {
	return &Transaction{Transaction: c.conn.Begin(), ctx: ctx}
}

// BeginWithError is used to start a transaction, but also returns the error.
func (c *Client) BeginWithError(ctx context.Context) (*Transaction, error) {
	tx, err := c.conn.BeginWithError()
	return &Transaction{Transaction: tx, ctx: ctx}, err
}
