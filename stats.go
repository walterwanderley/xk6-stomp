package stomp

import (
	"context"
	"fmt"
	"time"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/metrics"
	"go.k6.io/k6/stats"
)

var (
	dataSent     = stats.New(metrics.DataSentName, stats.Counter, stats.Data)
	dataReceived = stats.New(metrics.DataReceivedName, stats.Counter, stats.Data)

	sendMessage       = stats.New("stomp.send.count", stats.Counter)
	sendMessageTiming = stats.New("stomp.send.time", stats.Trend, stats.Time)
	sendMessageErrors = stats.New("stomp.send.error.count", stats.Counter)

	readMessage       = stats.New("stomp.read.count", stats.Counter)
	readMessageTiming = stats.New("stomp.read.time", stats.Trend, stats.Time)
	readMessageErrors = stats.New("stomp.read.error.count", stats.Counter)

	ackMessage       = stats.New("stomp.ack.count", stats.Counter)
	ackMessageErrors = stats.New("stomp.ack.error.count", stats.Counter)

	nackMessage       = stats.New("stomp.nack.count", stats.Counter)
	nackMessageErrors = stats.New("stomp.nack.error.count", stats.Counter)
)

func reportStats(ctx context.Context, metric *stats.Metric, tags map[string]string, now time.Time, value float64) {
	state := lib.GetState(ctx)
	if state == nil {
		fmt.Println("cann't get state")
		return
	}

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: metric,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  value,
	})
}
