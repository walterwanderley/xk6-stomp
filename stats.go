package stomp

import (
	"io"
	"time"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

var (
	registry    = metrics.NewRegistry()
	dataSent, _ = registry.NewMetric(metrics.DataSentName, metrics.Counter, metrics.Data)

	dataReceived, _ = registry.NewMetric(metrics.DataReceivedName, metrics.Counter, metrics.Data)

	sendMessage, _       = registry.NewMetric("stomp_send_count", metrics.Counter)
	sendMessageTiming, _ = registry.NewMetric("stomp_send_time", metrics.Trend, metrics.Time)
	sendMessageErrors, _ = registry.NewMetric("stomp_send_error_count", metrics.Counter)

	readMessage, _       = registry.NewMetric("stomp_read_count", metrics.Counter)
	readMessageTiming, _ = registry.NewMetric("stomp_read_time", metrics.Trend, metrics.Time)
	readMessageErrors, _ = registry.NewMetric("stomp._read_error_count", metrics.Counter)

	ackMessage, _       = registry.NewMetric("stomp_ack_count", metrics.Counter)
	ackMessageErrors, _ = registry.NewMetric("stomp_ack_error_count", metrics.Counter)

	nackMessage, _       = registry.NewMetric("stomp_nack_count", metrics.Counter)
	nackMessageErrors, _ = registry.NewMetric("stomp_nack_error_count", metrics.Counter)
)

func reportStats(vu modules.VU, metric *metrics.Metric, tags map[string]string, now time.Time, value float64) {
	state := vu.State()
	if state == nil {
		return
	}

	metrics.PushIfNotDone(vu.Context(), state.Samples, metrics.Sample{
		Time:   now,
		Metric: metric,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  value,
	})
}

type StatsReadWriteClose struct {
	io.ReadWriteCloser
	vu modules.VU
}

func (s *StatsReadWriteClose) Read(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Read(p)
	reportStats(s.vu, dataReceived, nil, time.Now(), float64(n))
	return n, err
}

func (s *StatsReadWriteClose) Write(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Write(p)
	reportStats(s.vu, dataSent, nil, time.Now(), float64(n))
	return n, err
}
