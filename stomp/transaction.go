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

		tags := map[string]string{
			METRIC_TAG_QUEUE: destination,
		}

		tx.client.reportStats(tx.client.metrics.sendMessageTiming, tags, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			tx.client.reportStats(tx.client.metrics.sendMessageErrors, tags, now, 1)
		} else {
			tx.client.reportStats(tx.client.metrics.sendMessage, tags, now, 1)
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

	tags := map[string]string{}
	if m.Message != nil {
		tags[METRIC_TAG_QUEUE] = m.Message.Destination
	}

	err := tx.Transaction.Ack(m.Message)
	if err != nil {
		tx.client.reportStats(tx.client.metrics.ackMessageErrors, tags, now, 1)
	} else {
		tx.client.reportStats(tx.client.metrics.ackMessage, tags, now, 1)
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

	tags := map[string]string{}
	if m.Message != nil {
		tags[METRIC_TAG_QUEUE] = m.Message.Destination
	}

	err := tx.Transaction.Nack(m.Message)
	if err != nil {
		tx.client.reportStats(tx.client.metrics.nackMessageErrors, tags, now, 1)
	} else {
		tx.client.reportStats(tx.client.metrics.nackMessage, tags, now, 1)
	}
	return err
}
