//go:build vlink_perf

package network

// This file is excluded from normal SDK builds. The performance build overlay
// calls StartQAMetrics and WrapQAMetricsLogger from database initialization.
import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm/logger"
)

type QAHTTPMetric struct {
	Requests          int64 `json:"requests"`
	TransportErrors   int64 `json:"transportErrors"`
	RequestBodyBytes  int64 `json:"requestBodyBytes"`
	ResponseBodyBytes int64 `json:"responseBodyBytes"`
}
type QAMetricSnapshot struct {
	StartedAt        int64                   `json:"startedAt"`
	CapturedAt       int64                   `json:"capturedAt"`
	HTTP             map[string]QAHTTPMetric `json:"http"`
	SQLTraceCalls    int64                   `json:"sqlTraceCalls"`
	SQLDurationNanos int64                   `json:"sqlDurationNanos"`
	SQLFailures      int64                   `json:"sqlFailures"`
}

var qaMetrics = struct {
	sync.Mutex
	data QAMetricSnapshot
}{data: QAMetricSnapshot{HTTP: map[string]QAHTTPMetric{}}}
var qaRoute = regexp.MustCompile(`^/[a-z_]+/[a-z_][a-z0-9_]*$`)

func init() { http.DefaultTransport = qaMetricsTransport{base: http.DefaultTransport} }
func qaRecordHTTP(route string, update func(*QAHTTPMetric)) {
	route = strings.TrimPrefix(route, "/api")
	if !qaRoute.MatchString(route) {
		route = "other"
	}
	qaMetrics.Lock()
	value := qaMetrics.data.HTTP[route]
	update(&value)
	qaMetrics.data.HTTP[route] = value
	qaMetrics.Unlock()
}
func ReadQAMetrics() QAMetricSnapshot {
	qaMetrics.Lock()
	defer qaMetrics.Unlock()
	result := qaMetrics.data
	result.CapturedAt = time.Now().UnixMilli()
	result.HTTP = make(map[string]QAHTTPMetric, len(qaMetrics.data.HTTP))
	for route, value := range qaMetrics.data.HTTP {
		result.HTTP[route] = value
	}
	return result
}

// Start once for a single-account, single-process QA run. Files contain counters
// only. Body bytes are observed HTTP body bytes, not TLS/IP traffic or headers.
func StartQAMetrics(directory string) func() {
	qaMetrics.Lock()
	qaMetrics.data = QAMetricSnapshot{StartedAt: time.Now().UnixMilli(), HTTP: map[string]QAHTTPMetric{}}
	qaMetrics.Unlock()
	done := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	flush := func() {
		data, err := json.Marshal(ReadQAMetrics())
		if err == nil {
			_ = os.WriteFile(filepath.Join(directory, "vlink-qa-metrics.json"), data, 0600)
		}
	}
	go func() {
		defer close(finished)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flush()
			case <-done:
				return
			}
		}
	}()
	return func() { once.Do(func() { close(done); <-finished; flush() }) }
}

type qaMetricsBody struct {
	io.ReadCloser
	bytes     atomic.Int64
	closeOnce sync.Once
	report    func(int64)
}

func (b *qaMetricsBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}
func (b *qaMetricsBody) Close() error {
	err := b.ReadCloser.Close()
	b.closeOnce.Do(func() { b.report(b.bytes.Load()) })
	return err
}

type qaMetricsTransport struct{ base http.RoundTripper }

func (t qaMetricsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	route := req.URL.Path
	qaRecordHTTP(route, func(v *QAHTTPMetric) { v.Requests++ })
	request := req.Clone(req.Context())
	if request.Body != nil {
		request.Body = &qaMetricsBody{ReadCloser: request.Body, report: func(n int64) { qaRecordHTTP(route, func(v *QAHTTPMetric) { v.RequestBodyBytes += n }) }}
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		qaRecordHTTP(route, func(v *QAHTTPMetric) { v.TransportErrors++ })
		return response, err
	}
	response.Body = &qaMetricsBody{ReadCloser: response.Body, report: func(n int64) { qaRecordHTTP(route, func(v *QAHTTPMetric) { v.ResponseBodyBytes += n }) }}
	return response, nil
}

type qaMetricsLogger struct{ logger.Interface }

func WrapQAMetricsLogger(base logger.Interface) logger.Interface { return qaMetricsLogger{base} }
func (l qaMetricsLogger) LogMode(level logger.LogLevel) logger.Interface {
	return qaMetricsLogger{l.Interface.LogMode(level)}
}
func (l qaMetricsLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	qaMetrics.Lock()
	qaMetrics.data.SQLTraceCalls++
	qaMetrics.data.SQLDurationNanos += time.Since(begin).Nanoseconds()
	if err != nil {
		qaMetrics.data.SQLFailures++
	}
	qaMetrics.Unlock()
	l.Interface.Trace(ctx, begin, fc, err)
}
