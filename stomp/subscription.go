package stomp

import (
	"time"

	"github.com/dop251/goja"
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

func NewSubscription(client *Client, sc *stomp.Subscription, listener Listener, listenerError ListenerError) *Subscription {
	s := Subscription{
		client:       client,
		Subscription: sc,
		listener:     listener,
		done:         make(chan bool, 1),
	}
	if listener != nil {
		go func() {
			for {
				startedAt := time.Now()
				select {
				case <-client.vu.Context().Done():
					return
				case <-s.done:
					return
				case stompMessage, ok := <-sc.C:
					if client.vu.State() == nil {
						return
					}
					if !ok || !s.Active() {
						handleListenerError(client.vu.Runtime(), listenerError, stomp.ErrCompletedSubscription)
						continue
					}

					client.reportStats(client.metrics.readMessageTiming, nil, time.Now(), metrics.D(time.Since(startedAt)))
					if stompMessage.Err != nil {
						s.client.reportStats(client.metrics.readMessageErrors, nil, time.Now(), 1)
						handleListenerError(client.vu.Runtime(), listenerError, stompMessage.Err)
						continue
					}
					msg := Message{Message: stompMessage, vu: s.client.vu}
					if err := listener(&msg); err != nil {
						client.reportStats(client.metrics.readMessageErrors, nil, time.Now(), 1)
						gojaErr := common.UnwrapGojaInterruptedError(err)
						handleListenerError(client.vu.Runtime(), listenerError, gojaErr)
					}
					client.reportStats(client.metrics.readMessage, nil, time.Now(), 1)
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
		common.Throw(s.client.vu.Runtime(), err)
	}
	msg = &Message{Message: stompMessage, vu: s.client.vu}
	return
}

func handleListenerError(rt *goja.Runtime, listenerErr ListenerError, err error) {
	if listenerErr == nil {
		common.Throw(rt, err)
	}
	o := rt.NewObject()
	if err := o.DefineDataProperty("error", rt.ToValue(err.Error()), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE); err != nil {
		common.Throw(rt, err)
	}

	listenerErr(o)
}
