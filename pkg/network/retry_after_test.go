package network

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/tools/errs"
)

func TestApiPostRetainsAdmissionAndBusinessError(t *testing.T) {
	for _, body := range []string{`{"errCode":8,"errMsg":"busy","errDlt":"retry later"}`, "proxy busy", ""} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "fixture", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "admission")
			started := time.Now()
			err := ApiPost(ctx, "/profiles", struct{}{}, nil)
			var admission *RetryAfterError
			if !errors.As(err, &admission) || admission.RetryAt.Before(started.Add(60*time.Second)) {
				t.Fatalf("lost Retry-After: %v", err)
			}
			if _, ok := err.(errs.CodeError); !ok {
				t.Fatal("lost direct bridge CodeError contract")
			}
			if strings.HasPrefix(body, "{") && (admission.Code() != 8 || admission.Msg() != "busy" || admission.Detail() != "retry later") {
				t.Fatalf("changed business error: %v", err)
			}
		})
	}
}

func TestRetryAfterDeadlineFormats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		value string
		want  time.Time
	}{
		{"60", now.Add(time.Minute)},
		{now.Add(time.Minute).Format(http.TimeFormat), now.Add(time.Minute)},
		{"invalid", time.Time{}},
		{"-1", time.Time{}},
		{now.Add(-time.Minute).Format(http.TimeFormat), time.Time{}},
	} {
		response := &http.Response{Status: "429 Too Many Requests", Header: make(http.Header)}
		response.Header.Set("Retry-After", tc.value)
		err := withRetryAfter(nil, response, now).(*RetryAfterError)
		if !err.RetryAt.Equal(tc.want) {
			t.Errorf("%q: got %v want %v", tc.value, err.RetryAt, tc.want)
		}
	}
}
