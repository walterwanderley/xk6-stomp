package stomp

import "github.com/go-stomp/stomp/v3"

type Transaction struct {
	*stomp.Transaction
}

func (tx *Transaction) Ack(msg *Message) error {
	return tx.Transaction.Ack(msg.Message)
}

func (tx *Transaction) Nack(msg *Message) error {
	return tx.Transaction.Nack(msg.Message)
}
