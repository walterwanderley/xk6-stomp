package stomp

import (
	"context"
	"time"

	"github.com/go-stomp/stomp/v3"
	"go.k6.io/k6/stats"
)

type Subscription struct {
	*stomp.Subscription
	ctx context.Context
}

func NewSubscription(ctx context.Context, sc *stomp.Subscription, listener Listener) *Subscription {
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

func (s *Subscription) Read() (msg *Message, err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		reportStats(s.ctx, readMessageTiming, nil, now, stats.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(s.ctx, readMessageErrors, nil, now, 1)
		} else {
			reportStats(s.ctx, dataReceived, nil, now, float64(len(msg.Body)))
			reportStats(s.ctx, readMessage, nil, now, 1)
		}
	}()
	var stompMessage *stomp.Message
	stompMessage, err = s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	return &Message{Message: stompMessage, ctx: s.ctx}, nil
}
