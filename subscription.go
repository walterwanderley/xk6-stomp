package stomp

import (
	"context"

	"github.com/go-stomp/stomp/v3"
)

type Subscription struct {
	*stomp.Subscription
	ctx context.Context
}

func NewSubscription(sc *stomp.Subscription, ctx context.Context, listener Listener) *Subscription {
	var s Subscription
	s.Subscription = sc
	s.ctx = ctx
	if listener != nil {
		go func() {
			for {
				msg, err := s.Read()
				if err != nil {
					return
				}
				listener(msg)
			}
		}()
	}
	return &s
}

func (s *Subscription) Read() (*Message, error) {
	msg, err := s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	return &Message{Message: msg, ctx: s.ctx}, nil
}
