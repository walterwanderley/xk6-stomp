package stomp

import (
	"log"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

type Subscription struct {
	*stomp.Subscription
	client   *Client
	listener Listener
	done     chan bool
}

func NewSubscription(client *Client, sc *stomp.Subscription, listener Listener, abortOnFail bool) *Subscription {
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
					s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
					if err, ok := e.(error); ok {
						err = common.UnwrapGojaInterruptedError(err)
						log.Printf("[xk6-stomp] listener %q panic err: %v\n", sc.Destination(), err.Error())
					} else {
						log.Printf("[xk6-stomp] listener %q panic: %v\n", sc.Destination(), e)
					}
					if abortOnFail {
						panic(e)
					}
				}
			}()
			for {
				startedAt := time.Now()
				select {
				case <-client.vu.Context().Done():
					return
				case <-s.done:
					return
				case stompMessage, ok := <-sc.C:
					if !s.Active() || s.client.vu.State() == nil {
						return
					}
					if !ok {
						continue
					}

					s.client.reportStats(s.client.metrics.readMessageTiming, nil, time.Now(), metrics.D(time.Since(startedAt)))
					if stompMessage.Err != nil {
						s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
						continue
					}
					msg := Message{Message: stompMessage, vu: s.client.vu}
					if err := listener(&msg); err != nil {
						s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
						gojaErr := common.UnwrapGojaInterruptedError(err)
						log.Printf("[xk6-stomp] listener %q err: %s\n", sc.Destination(), gojaErr.Error())
						common.Throw(s.client.vu.Runtime(), gojaErr)
					}
					s.client.reportStats(s.client.metrics.readMessage, nil, time.Now(), 1)
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
