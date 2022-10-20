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
	client        *Client
	listener      Listener
	listenerError ListenerError
	done          chan bool
}

func NewSubscription(client *Client, sc *stomp.Subscription, listener Listener, listenerError ListenerError) *Subscription {
	s := Subscription{
		client:        client,
		Subscription:  sc,
		listener:      listener,
		listenerError: listenerError,
		done:          make(chan bool, 1),
	}
	if listener != nil {
		runOnLoop := s.client.vu.RegisterCallback()
		go s.handle(runOnLoop)
	}
	return &s
}

func (s *Subscription) Continue() error {
	if s.listener == nil {
		return nil
	}
	if !s.Active() {
		return stomp.ErrCompletedSubscription
	}
	runOnLoop := s.client.vu.RegisterCallback()
	go s.handle(runOnLoop)
	return nil
}

func (s *Subscription) handle(runOnLoop func(func() error)) {
	noop := func() error { return nil }
	startedAt := time.Now()
	select {
	case stompMessage, ok := <-s.C:
		if !ok || !s.Active() {
			runOnLoop(s.handleListenerError(stomp.ErrCompletedSubscription))
			return
		}

		if s.client == nil || s.client.ctx.Err() != nil || s.client.vu.Context().Err() != nil || s.client.vu.State() == nil || stompMessage.Conn == nil {
			runOnLoop(noop)
			return
		}

		s.client.reportStats(s.client.metrics.readMessageTiming, nil, time.Now(), metrics.D(time.Since(startedAt)))
		if stompMessage.Err != nil {
			s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
			runOnLoop(s.handleListenerError(stompMessage.Err))
			return
		}
		msg := Message{Message: stompMessage, Subscription: s, vu: s.client.vu}
		runOnLoop(func() error {
			err := s.listener(&msg)
			if err != nil {
				s.client.reportStats(s.client.metrics.readMessageErrors, nil, time.Now(), 1)
			}
			return err
		})
		s.client.reportStats(s.client.metrics.readMessage, nil, time.Now(), 1)
	case <-s.client.ctx.Done():
		runOnLoop(noop)
		return
	case <-s.client.vu.Context().Done():
		runOnLoop(noop)
		return
	case <-s.done:
		runOnLoop(noop)
		return
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

func (s *Subscription) handleListenerError(err error) func() error {
	return func() error {
		if s.client == nil || s.client.ctx.Err() != nil || s.client.vu.Context().Err() != nil || s.client.vu.State() == nil {
			return nil
		}

		rt := s.client.vu.Runtime()
		if rt == nil {
			return nil
		}
		if s.listenerError == nil {
			common.Throw(rt, err)
		}
		o := rt.NewObject()
		if err := o.DefineDataProperty("error", rt.ToValue(err.Error()), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE); err != nil {
			common.Throw(rt, err)
		}

		s.listenerError(o)
		return nil
	}
}
