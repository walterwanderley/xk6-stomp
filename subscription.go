package stomp

import (
	"log"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/metrics"
)

type Subscription struct {
	*stomp.Subscription
	client   *Client
	listener Listener
	done     chan bool
}

func NewSubscription(client *Client, sc *stomp.Subscription, listener Listener) *Subscription {
	s := Subscription{
		client:       client,
		Subscription: sc,
		listener:     listener,
		done:         make(chan bool, 1),
	}
	if listener != nil {
		go func() {
			defer func() {
				e := recover()
				if e != nil {
					log.Println("Recover FROM:", e)
				}
			}()
			for {
				select {
				case <-client.vu.Context().Done():
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
		s.client.reportStats(s.client.metrics.readMessageTiming, nil, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			s.client.reportStats(s.client.metrics.readMessageErrors, nil, now, 1)
		} else {
			s.client.reportStats(s.client.metrics.readMessage, nil, now, 1)
		}
	}()
	var stompMessage *stomp.Message
	stompMessage, err = s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	msg = &Message{Message: stompMessage, vu: s.client.vu}
	return
}
