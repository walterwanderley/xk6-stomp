package stomp

import (
	"context"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/stats"
)

type Transaction struct {
	*stomp.Transaction
	ctx context.Context
}

func (tx *Transaction) Send(destination, contentType string, body []byte, opts *SendOptions) (err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		reportStats(tx.ctx, sendMessageTiming, nil, now, stats.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(tx.ctx, sendMessageErrors, nil, now, 1)
		} else {
			reportStats(tx.ctx, dataSent, nil, now, float64(len(body)))
			reportStats(tx.ctx, sendMessage, nil, now, 1)
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

func (tx *Transaction) Ack(msg *Message) error {
	now := time.Now()
	err := tx.Transaction.Ack(msg.Message)
	if err != nil {
		reportStats(tx.ctx, ackMessageErrors, nil, now, 1)
	} else {
		reportStats(tx.ctx, ackMessage, nil, now, 1)
	}
	return err
}

func (tx *Transaction) Nack(msg *Message) error {
	now := time.Now()
	err := tx.Transaction.Nack(msg.Message)
	if err != nil {
		reportStats(tx.ctx, nackMessageErrors, nil, now, 1)
	} else {
		reportStats(tx.ctx, nackMessage, nil, now, 1)
	}
	return err
}
