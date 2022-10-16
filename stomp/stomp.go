package stomp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/dop251/goja"
	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

const (
	defaultProtocol = "tcp"
	defaultTimeout  = "10s"
)

type (
	RootModule struct{}

	// Stomp is the k6 extension for a Stomp client.
	Stomp struct {
		vu modules.VU
		*Client
	}
)

type Options struct {
	Addr     string
	Protocol string
	Path     string
	Timeout  string
	TLS      bool
	Headers  map[string]string
	Host     string

	User string
	Pass string

	MessageSendTimeout string
	ReceiptTimeout     string

	Heartbeat struct {
		Incoming string
		Outgoing string
	}

	ReadBufferSize      int
	ReadChannelCapacity int

	WriteBufferSize      int
	WriteChannelCapacity int

	Verbose bool
}

// Client is the Stomp conn wrapper.
type Client struct {
	conn    *stomp.Conn
	vu      modules.VU
	metrics stompMetrics
}

type SendOptions struct {
	Headers map[string]string
	Receipt bool
}

// Listener is a callback function to execute when the subscription reads a message
type Listener func(*Message) error

type ListenerError func(goja.Value) (goja.Value, error)

type SubscribeOptions struct {
	Ack      string
	Headers  map[string]string
	Id       string
	Listener Listener
	Error    ListenerError
}

func New() *RootModule {
	return &RootModule{}
}

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	m, err := registerMetrics(vu)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}
	return &Stomp{vu: vu, Client: &Client{vu: vu, metrics: m}}
}

func (s *Stomp) Exports() modules.Exports {
	return modules.Exports{Default: s.Client}
}

// Connect to a stomp server
func (c *Client) Connect(opts *Options) *Client {
	rt := c.vu.Runtime()
	if c.conn != nil {
		common.Throw(rt, fmt.Errorf("already connected"))
	}

	if opts.Protocol == "" {
		opts.Protocol = defaultProtocol
	}
	if opts.Timeout == "" {
		opts.Timeout = defaultTimeout
	}

	netConn, err := openNetConn(opts, c)
	if err != nil {
		common.Throw(rt, err)
	}
	connOpts := make([]func(*stomp.Conn) error, 0)
	if opts.User != "" || opts.Pass != "" {
		connOpts = append(connOpts, stomp.ConnOpt.Login(opts.User, opts.Pass))
	}
	for k, v := range opts.Headers {
		connOpts = append(connOpts, stomp.ConnOpt.Header(k, v))
	}
	if opts.Host != "" {
		connOpts = append(connOpts, stomp.ConnOpt.Host(opts.Host))
	}
	if opts.MessageSendTimeout != "" {
		timeout, err := time.ParseDuration(opts.MessageSendTimeout)
		if err != nil {
			common.Throw(rt, err)
		}
		connOpts = append(connOpts, stomp.ConnOpt.MsgSendTimeout(timeout))
	}
	if opts.ReceiptTimeout != "" {
		timeout, err := time.ParseDuration(opts.ReceiptTimeout)
		if err != nil {
			common.Throw(rt, err)
		}
		connOpts = append(connOpts, stomp.ConnOpt.RcvReceiptTimeout(timeout))
	}
	if opts.ReadBufferSize > 0 {
		connOpts = append(connOpts, stomp.ConnOpt.ReadBufferSize(opts.ReadBufferSize))
	}
	if opts.ReadChannelCapacity > 0 {
		connOpts = append(connOpts, stomp.ConnOpt.ReadChannelCapacity(opts.ReadChannelCapacity))
	}
	if opts.WriteBufferSize > 0 {
		connOpts = append(connOpts, stomp.ConnOpt.WriteBufferSize(opts.WriteBufferSize))
	}
	if opts.WriteChannelCapacity > 0 {
		connOpts = append(connOpts, stomp.ConnOpt.WriteChannelCapacity(opts.WriteChannelCapacity))
	}
	if opts.Heartbeat.Incoming != "" || opts.Heartbeat.Outgoing != "" {
		sendTimeout, receiveTimeout := time.Minute, time.Minute
		if opts.Heartbeat.Outgoing != "" {
			sendTimeout, err = time.ParseDuration(opts.Heartbeat.Outgoing)
			if err != nil {
				common.Throw(rt, err)
			}
		}
		if opts.Heartbeat.Incoming != "" {
			receiveTimeout, err = time.ParseDuration(opts.Heartbeat.Incoming)
			if err != nil {
				common.Throw(rt, err)
			}
		}
		connOpts = append(connOpts, stomp.ConnOpt.HeartBeat(sendTimeout, receiveTimeout))
	}

	c.conn, err = stomp.Connect(netConn, connOpts...)
	if err != nil {
		common.Throw(rt, err)
	}

	return c
}

func openNetConn(opts *Options, c *Client) (io.ReadWriteCloser, error) {
	timeout, err := time.ParseDuration(opts.Timeout)
	if err != nil {
		return nil, err
	}
	var rwc io.ReadWriteCloser
	switch {
	case opts.Protocol == "ws" || opts.Protocol == "wss":
		rwc, err = openWSConn(opts, timeout)
	case opts.TLS:
		rwc, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout},
			opts.Protocol, opts.Addr, nil)
	default:
		rwc, err = net.DialTimeout(opts.Protocol, opts.Addr, timeout)
	}
	rwc = &StatsReadWriteClose{rwc, c}

	if opts.Verbose {
		return &VerboseReadWriteClose{rwc}, err
	}
	return rwc, err
}

// Disconnect will disconnect from the STOMP server.
func (c *Client) Disconnect() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Disconnect()
}

// Send sends a message to the STOMP server.
func (c *Client) Send(destination, contentType string, body []byte, opts *SendOptions) (err error) {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		c.reportStats(c.metrics.sendMessageTiming, nil, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			c.reportStats(c.metrics.sendMessageErrors, nil, now, 1)
		} else {
			c.reportStats(c.metrics.sendMessage, nil, now, 1)
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
	if err != nil {
		common.Throw(c.vu.Runtime(), err)
	}
	return
}

// Subscribe creates a subscription on the STOMP server.
func (c *Client) Subscribe(destination string, opts *SubscribeOptions) (*Subscription, error) {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
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
		common.Throw(c.vu.Runtime(), err)
	}
	return NewSubscription(c, sub, opts.Listener, opts.Error), nil
}

// Ack acknowledges a message received from the STOMP server.
func (c *Client) Ack(m *Message) error {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	if m == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("nil message"))
	}
	now := time.Now()
	if m.Header.Get(frame.Id) == "" {
		m.Header.Set(frame.Id, m.Header.Get(frame.Ack))
	}
	if m.Header.Get(frame.MessageId) == "" {
		m.Header.Set(frame.MessageId, m.Header.Get(frame.Ack))
	}
	err := c.conn.Ack(m.Message)
	if err != nil {
		c.reportStats(c.metrics.ackMessageErrors, nil, now, 1)
		common.Throw(c.vu.Runtime(), err)
	} else {
		c.reportStats(c.metrics.ackMessage, nil, now, 1)
	}
	return err
}

// Nack indicates to the server that a message was not received
// by the client.
func (c *Client) Nack(m *Message) error {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	if m == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("nil message"))
	}
	now := time.Now()
	if m.Header.Get(frame.Id) == "" {
		m.Header.Set(frame.Id, m.Header.Get(frame.Ack))
	}
	if m.Header.Get(frame.MessageId) == "" {
		m.Header.Set(frame.MessageId, m.Header.Get(frame.Ack))
	}
	err := c.conn.Nack(m.Message)
	if err != nil {
		c.reportStats(c.metrics.nackMessageErrors, nil, now, 1)
		common.Throw(c.vu.Runtime(), err)
	} else {
		c.reportStats(c.metrics.nackMessage, nil, now, 1)
	}
	return err
}

// Server returns the STOMP server identification.
func (c *Client) Server() string {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	return c.conn.Server()
}

// Session returns the session identifier.
func (c *Client) Session() string {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	return c.conn.Session()
}

// Begin is used to start a transaction.
func (c *Client) Begin() *Transaction {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	return &Transaction{Transaction: c.conn.Begin(), client: c}
}

// BeginWithError is used to start a transaction, but also returns the error.
func (c *Client) BeginWithError(ctx context.Context) (*Transaction, error) {
	if c == nil || c.conn == nil {
		common.Throw(c.vu.Runtime(), fmt.Errorf("not connected"))
	}
	tx, err := c.conn.BeginWithError()
	if err != nil {
		common.Throw(c.vu.Runtime(), err)
	}
	return &Transaction{Transaction: tx, client: c}, err
}

func (c *Client) reportStats(metric *metrics.Metric, tags map[string]string, now time.Time, value float64) {
	state := c.vu.State()
	if state == nil {
		return
	}

	metrics.PushIfNotDone(c.vu.Context(), state.Samples, metrics.Sample{
		Time:   now,
		Metric: metric,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  value,
	})
}
