package stomp

import (
	"context"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/stats"
)

type Subscription struct {
	*stomp.Subscription
	ctx      context.Context
	listener Listener
	done     chan bool
}

func NewSubscription(ctx context.Context, sc *stomp.Subscription, listener Listener) *Subscription {
	s := Subscription{
		ctx:          ctx,
		Subscription: sc,
		listener:     listener,
		done:         make(chan bool, 1),
	}
	if listener != nil {
		go func() {
			defer func() {
				_ = recover()
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.done:
					return
				default:
					if !s.Active() {
						return
					}
					msg, err := s.Read()
					if err != nil {
						return
					}
					select {
					case <-s.done:
						return
					default:
						listener(msg)
					}
				}
			}
		}()
	}
	return &s
}

func (s *Subscription) Unsubscribe(opts ...func(*frame.Frame) error) error {
	if s.Active() {
		if s.listener != nil {
			s.done <- true
		}
		return s.Subscription.Unsubscribe(opts...)
	}
	return nil
}

func (s *Subscription) Read() (msg *Message, err error) {
	startedAt := time.Now()
	defer func() {
		now := time.Now()
		reportStats(s.ctx, readMessageTiming, nil, now, stats.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(s.ctx, readMessageErrors, nil, now, 1)
		} else {
			if msg != nil {
				reportStats(s.ctx, dataReceived, nil, now, float64(len(msg.Body)))
			}
			reportStats(s.ctx, readMessage, nil, now, 1)
		}
	}()
	var stompMessage *stomp.Message
	stompMessage, err = s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	msg = &Message{Message: stompMessage, ctx: s.ctx}
	return
}
