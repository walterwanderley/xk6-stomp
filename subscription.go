package stomp

import (
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/stats"
)

type Subscription struct {
	*stomp.Subscription
	vu       modules.VU
	listener Listener
	done     chan bool
}

func NewSubscription(vu modules.VU, sc *stomp.Subscription, listener Listener) *Subscription {
	s := Subscription{
		vu:           vu,
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
				case <-vu.Context().Done():
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
		reportStats(s.vu, readMessageTiming, nil, now, stats.D(now.Sub(startedAt)))
		if err != nil {
			reportStats(s.vu, readMessageErrors, nil, now, 1)
		} else {
			reportStats(s.vu, readMessage, nil, now, 1)
		}
	}()
	var stompMessage *stomp.Message
	stompMessage, err = s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	msg = &Message{Message: stompMessage, vu: s.vu}
	return
}
