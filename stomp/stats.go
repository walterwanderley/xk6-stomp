package stomp

import (
	"errors"
	"io"
	"time"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type stompMetrics struct {
	dataSent     *metrics.Metric
	dataReceived *metrics.Metric

	sendMessage       *metrics.Metric
	sendMessageTiming *metrics.Metric
	sendMessageErrors *metrics.Metric

	readMessage       *metrics.Metric
	readMessageTiming *metrics.Metric
	readMessageErrors *metrics.Metric

	ackMessage       *metrics.Metric
	ackMessageErrors *metrics.Metric

	nackMessage       *metrics.Metric
	nackMessageErrors *metrics.Metric
}

func registerMetrics(vu modules.VU) (stompMetrics, error) {
	var err error
	var sm stompMetrics
	registry := vu.InitEnv().Registry

	if sm.dataSent, err = registry.NewMetric(metrics.DataSentName, metrics.Counter, metrics.Data); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.dataReceived, err = registry.NewMetric(metrics.DataReceivedName, metrics.Counter, metrics.Data); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessage, err = registry.NewMetric("stomp_send_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessageTiming, err = registry.NewMetric("stomp_send_time", metrics.Trend, metrics.Time); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessageErrors, err = registry.NewMetric("stomp_send_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessage, err = registry.NewMetric("stomp_read_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessageTiming, err = registry.NewMetric("stomp_read_time", metrics.Trend, metrics.Time); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessageErrors, err = registry.NewMetric("stomp_read_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.ackMessage, err = registry.NewMetric("stomp_ack_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.ackMessageErrors, err = registry.NewMetric("stomp_ack_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.nackMessage, err = registry.NewMetric("stomp_nack_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.nackMessageErrors, err = registry.NewMetric("stomp_nack_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	return sm, nil
}

type StatsReadWriteClose struct {
	io.ReadWriteCloser
	client *Client
}

func (s *StatsReadWriteClose) Read(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Read(p)
	s.client.reportStats(s.client.metrics.dataReceived, nil, time.Now(), float64(n))
	return n, err
}

func (s *StatsReadWriteClose) Write(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Write(p)
	s.client.reportStats(s.client.metrics.dataSent, nil, time.Now(), float64(n))
	return n, err
}
