package stomp

import (
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/metrics"
)

type Transaction struct {
	*stomp.Transaction
	client *Client
}

func (tx *Transaction) Send(destination, contentType string, body []byte, opts *SendOptions) (err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		tx.client.reportStats(tx.client.metrics.sendMessageTiming, nil, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			tx.client.reportStats(tx.client.metrics.sendMessageErrors, nil, now, 1)
		} else {
			tx.client.reportStats(tx.client.metrics.sendMessage, nil, now, 1)
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
	err = tx.Transaction.Send(destination, contentType, body, sendOpts...)
	return
}

func (tx *Transaction) Ack(m *Message) error {
	now := time.Now()
	if m.Header.Get(frame.Id) == "" {
		m.Header.Set(frame.Id, m.Header.Get(frame.Ack))
	}
	if m.Header.Get(frame.MessageId) == "" {
		m.Header.Set(frame.MessageId, m.Header.Get(frame.Ack))
	}
	err := tx.Transaction.Ack(m.Message)
	if err != nil {
		tx.client.reportStats(tx.client.metrics.ackMessageErrors, nil, now, 1)
	} else {
		tx.client.reportStats(tx.client.metrics.ackMessage, nil, now, 1)
	}
	return err
}

func (tx *Transaction) Nack(m *Message) error {
	now := time.Now()
	if m.Header.Get(frame.Id) == "" {
		m.Header.Set(frame.Id, m.Header.Get(frame.Ack))
	}
	if m.Header.Get(frame.MessageId) == "" {
		m.Header.Set(frame.MessageId, m.Header.Get(frame.Ack))
	}
	err := tx.Transaction.Nack(m.Message)
	if err != nil {
		tx.client.reportStats(tx.client.metrics.nackMessageErrors, nil, now, 1)
	} else {
		tx.client.reportStats(tx.client.metrics.nackMessage, nil, now, 1)
	}
	return err
}
