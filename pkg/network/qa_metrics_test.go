//go:build vlink_perf

package network

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm/logger"
)

func TestQAMetricsPreserveHTTPAndExcludePayload(t *testing.T) {
	dir := t.TempDir()
	stop := StartQAMetrics(dir)
	defer stop()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "private-request" || r.Header.Get("token") != "private-token" {
			t.Error("request changed")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("private-response"))
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/msg/sync_messages_v2?secret=private-query", bytes.NewBufferString("private-request"))
	req.Header.Set("token", "private-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(data) != "private-response" || resp.StatusCode != http.StatusCreated {
		t.Fatal("response changed", err)
	}
	WrapQAMetricsLogger(logger.Default.LogMode(logger.Silent)).Trace(context.Background(), time.Now(), func() (string, int64) { t.Fatal("SQL payload was unnecessarily materialized"); return "private-sql", 1 }, nil)
	snapshot := ReadQAMetrics()
	route := snapshot.HTTP["/msg/sync_messages_v2"]
	if route.Requests != 1 || route.RequestBodyBytes != int64(len("private-request")) || route.ResponseBodyBytes != int64(len("private-response")) || snapshot.SQLTraceCalls != 1 {
		t.Fatalf("wrong counters: %+v %+v", snapshot, route)
	}
	stop()
	saved, err := os.ReadFile(filepath.Join(dir, "vlink-qa-metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(saved), "private-") {
		t.Fatal("private payload leaked")
	}
}
