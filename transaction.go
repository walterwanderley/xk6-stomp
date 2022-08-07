package stomp

import (
	"go.k6.io/k6/metrics"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/js/modules"
)

type Transaction struct {
	*stomp.Transaction
	vu modules.VU
}

func (tx *Transaction) Send(destination, contentType string, body []byte, opts *SendOptions) (err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		reportStats(tx.vu, sendMessageTiming, nil, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(tx.vu, sendMessageErrors, nil, now, 1)
		} else {
			reportStats(tx.vu, dataSent, nil, now, float64(len(body)))
			reportStats(tx.vu, sendMessage, nil, now, 1)
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
		reportStats(tx.vu, ackMessageErrors, nil, now, 1)
	} else {
		reportStats(tx.vu, ackMessage, nil, now, 1)
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
		reportStats(tx.vu, nackMessageErrors, nil, now, 1)
	} else {
		reportStats(tx.vu, nackMessage, nil, now, 1)
	}
	return err
}
