package stomp

import (
	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type Transaction struct {
	*stomp.Transaction
}

func (tx *Transaction) Send(destination, contentType string, body []byte, opts *SendOptions) error {
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
	return tx.Transaction.Send(destination, contentType, body, sendOpts...)
}

func (tx *Transaction) Ack(msg *Message) error {
	return tx.Transaction.Ack(msg.Message)
}

func (tx *Transaction) Nack(msg *Message) error {
	return tx.Transaction.Nack(msg.Message)
}
