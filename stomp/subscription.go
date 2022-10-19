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
		go s.handle(listenerError)
	}
	return &s
}

func (s *Subscription) handle(listenerError ListenerError) {
	for {
		startedAt := time.Now()
		select {
		case <-s.client.ctx.Done():
			return
		case <-s.client.vu.Context().Done():
			return
		case <-s.done:
			return
		case stompMessage, ok := <-s.C:
			if !ok || !s.Active() {
				s.handleListenerError(listenerError, stomp.ErrCompletedSubscription)
				return
			}

			if s.client == nil || s.client.ctx.Err() != nil || s.client.vu.Context().Err() != nil || s.client.vu.State() == nil || stompMessage.Conn == nil {
				return
			}

			s.client.reportStats(s.client.metrics.readMessageTiming, nil, time.Now(), metrics.D(time.Since(startedAt)))
			if stompMessage.Err != nil {
				s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
				s.handleListenerError(listenerError, stompMessage.Err)
				return
			}
			msg := Message{Message: stompMessage, vu: s.client.vu}
			err := s.listener(&msg)
			if err != nil {
				s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
				gojaErr := common.UnwrapGojaInterruptedError(err)
				s.handleListenerError(listenerError, gojaErr)
			}

			s.client.reportStats(s.client.metrics.readMessage, nil, time.Now(), 1)
		}
	}
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

func (s *Subscription) handleListenerError(listenerErr ListenerError, err error) {
	if s.client == nil || s.client.ctx.Err() != nil || s.client.vu.Context().Err() != nil || s.client.vu.State() == nil {
		return
	}

	rt := s.client.vu.Runtime()
	if rt == nil {
		return
	}
	if listenerErr == nil {
		common.Throw(rt, err)
	}
	o := rt.NewObject()
	if err := o.DefineDataProperty("error", rt.ToValue(err.Error()), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE); err != nil {
		common.Throw(rt, err)
	}

	listenerErr(o)
}
