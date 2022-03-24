package stomp

import (
	"io"
	"time"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib/metrics"
	"go.k6.io/k6/stats"
)

var (
	dataSent     = stats.New(metrics.DataSentName, stats.Counter, stats.Data)
	dataReceived = stats.New(metrics.DataReceivedName, stats.Counter, stats.Data)

	sendMessage       = stats.New("stomp_send_count", stats.Counter)
	sendMessageTiming = stats.New("stomp_send_time", stats.Trend, stats.Time)
	sendMessageErrors = stats.New("stomp_send_error_count", stats.Counter)

	readMessage       = stats.New("stomp_read_count", stats.Counter)
	readMessageTiming = stats.New("stomp_read_time", stats.Trend, stats.Time)
	readMessageErrors = stats.New("stomp._read_error_count", stats.Counter)

	ackMessage       = stats.New("stomp_ack_count", stats.Counter)
	ackMessageErrors = stats.New("stomp_ack_error_count", stats.Counter)

	nackMessage       = stats.New("stomp_nack_count", stats.Counter)
	nackMessageErrors = stats.New("stomp_nack_error_count", stats.Counter)
)

func reportStats(vu modules.VU, metric *stats.Metric, tags map[string]string, now time.Time, value float64) {
	state := vu.State()
	if state == nil {
		return
	}

	stats.PushIfNotDone(vu.Context(), state.Samples, stats.Sample{
		Time:   now,
		Metric: metric,
		Tags:   stats.IntoSampleTags(&tags),
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
