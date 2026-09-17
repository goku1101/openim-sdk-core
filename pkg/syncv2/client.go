// Package syncv2 implements the project-owned conversation synchronization protocol.
package syncv2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/tools/log"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/tools/errs"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const PolicyVersion = "conversation-sync-v2.2"
const maxRequestBytes = 64 * 1024

var diagnosticReason = regexp.MustCompile(`SYNC_V2_[A-Z_]+`)

// The server bounds protobuf responses to 3 MiB. JSON escaping can expand strings
// sixfold; bound the wire response as well, including its API envelope.
const maxResponseBytes = 20 * 1024 * 1024

// APIError preserves the server's code and reason for precise recovery decisions.
// Error never includes tokens, request payloads or message response bodies.
type APIError struct {
	ErrCode int
	ErrMsg  string
	ErrDlt  string
}

func (e *APIError) Error() string { return fmt.Sprintf("sync v2: %d %s", e.ErrCode, e.ErrMsg) }

// Unwrap exposes the SDK callback's coded error without leaking server details.
// The outer APIError remains available for Sync V2 recovery decisions.
func (e *APIError) Unwrap() error { return errs.NewCodeError(e.ErrCode, e.ErrMsg) }

type HTTPError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string { return fmt.Sprintf("sync v2 HTTP status %d", e.StatusCode) }

type Client struct {
	baseURL          string
	token            string
	http             *http.Client
	clusterAdmission atomic.Bool
	requestSlot      chan struct{}
}

func NewClient(baseURL, token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 70 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), token: token, http: httpClient, requestSlot: make(chan struct{}, 1)}
}

func (c *Client) invoke(ctx context.Context, operationID, route string, req, resp proto.Message) (json.RawMessage, error) {
	if !c.clusterAdmission.Load() {
		return c.invokeOnce(ctx, operationID, route, req, resp)
	}
	// One client belongs to one authenticated runtime. Share admission across
	// startup, foreground history and durable retries, not just each worker pool.
	select {
	case c.requestSlot <- struct{}{}:
		defer func() { <-c.requestSlot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := c.invokeOnce(ctx, operationID, route, req, resp)
		if err == nil || attempt == 3 || !isAdmissionRetry(err) {
			return raw, err
		}
		if err = waitAdmissionRetry(ctx, err, attempt); err != nil {
			return nil, err
		}
	}
}

func (c *Client) invokeOnce(ctx context.Context, operationID, route string, req, resp proto.Message) (json.RawMessage, error) {
	if operationID == "" {
		return nil, fmt.Errorf("sync v2: operationID is required")
	}
	// The pinned API gateway binds generated Go structs with encoding/json.
	// protojson would quote int64 sequences, which that binding rejects.
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("sync v2: encode request: %w", err)
	}
	if len(body) > maxRequestBytes {
		return nil, &APIError{ErrCode: 8, ErrMsg: "SYNC_V2_REQUEST_BYTES"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+route, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("token", c.token)
	request.Header.Set("operationID", operationID)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %w", route, &HTTPError{StatusCode: response.StatusCode, RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now())})
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("sync v2: response exceeds byte limit")
	}
	var envelope struct {
		ErrCode *int            `json:"errCode"`
		ErrMsg  string          `json:"errMsg"`
		ErrDlt  string          `json:"errDlt"`
		Data    json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("sync v2: invalid response envelope")
	}
	if envelope.ErrCode == nil {
		return nil, fmt.Errorf("sync v2: response is missing errCode")
	}
	if *envelope.ErrCode != 0 {
		fields := []any{"operationID", operationID, "route", route, "diagnosticReason", diagnosticReason.FindString(envelope.ErrDlt)}
		if pull, ok := req.(*pb.PullRequest); ok {
			fields = append(fields, "notifications", pull.Notifications, "self", pull.Self, "sequenceCount", len(pull.Seqs))
			if len(pull.Seqs) > 0 {
				low, high := pull.Seqs[0], pull.Seqs[0]
				for _, seq := range pull.Seqs {
					low = min(low, seq)
					high = max(high, seq)
				}
				fields = append(fields, "minRequestedSeq", low, "maxRequestedSeq", high)
			}
		}
		log.ZWarn(ctx, "sync v2 API request failed", &APIError{ErrCode: *envelope.ErrCode, ErrMsg: envelope.ErrMsg}, fields...)
		return nil, fmt.Errorf("%s: %w", route, &APIError{ErrCode: *envelope.ErrCode, ErrMsg: envelope.ErrMsg, ErrDlt: envelope.ErrDlt})
	}
	if len(envelope.Data) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		// apiresp omits data for ReleaseResponse because it has no public fields.
		if _, ok := resp.(*pb.ReleaseResponse); ok {
			return nil, nil
		}
		return nil, fmt.Errorf("sync v2: response is missing data")
	}
	// Unknown additive fields do not alter the semantics of the negotiated policy.
	if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(envelope.Data, resp); err != nil {
		return nil, fmt.Errorf("sync v2: invalid response data")
	}
	return envelope.Data, nil
}

func (c *Client) Capabilities(ctx context.Context, op string, req *pb.UserRequest) (*pb.Capabilities, error) {
	resp := new(pb.Capabilities)
	raw, err := c.invoke(ctx, op, "/conversation/sync_capabilities", req, resp)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	declaration := bytes.TrimSpace(fields["conversationSyncV2"])
	if !bytes.Equal(declaration, []byte("true")) && !bytes.Equal(declaration, []byte("false")) {
		return nil, fmt.Errorf("sync v2: missing or invalid capability declaration")
	}
	if resp.ConversationSyncV2 && (resp.PolicyVersion != PolicyVersion || resp.MaxBatchSize <= 0 || resp.MaxPageSize <= 0 || resp.SnapshotTTLSeconds <= 0 || resp.MaxRequestBytes <= 0) {
		return nil, fmt.Errorf("sync v2: incompatible capability contract")
	}
	c.clusterAdmission.Store(resp.ConversationSyncV2 && resp.ClusterReadAdmission)
	return resp, nil
}

func (c *Client) Pull(ctx context.Context, op string, req *pb.PullRequest) (*pb.PullResponse, error) {
	resp := new(pb.PullResponse)
	_, err := c.invoke(ctx, op, "/msg/sync_messages_v2", req, resp)
	return resp, err
}

func (c *Client) Snapshot(ctx context.Context, op string, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	resp := new(pb.SnapshotResponse)
	_, err := c.invoke(ctx, op, "/conversation/sync_snapshot", req, resp)
	return resp, err
}

func (c *Client) States(ctx context.Context, op string, req *pb.StateRequest) (*pb.StateResponse, error) {
	resp := new(pb.StateResponse)
	_, err := c.invoke(ctx, op, "/msg/sync_seqs_v2", req, resp)
	return resp, err
}

func (c *Client) Release(ctx context.Context, op string, req *pb.ReleaseRequest) error {
	_, err := c.invoke(ctx, op, "/conversation/sync_snapshot_release", req, new(pb.ReleaseResponse))
	return err
}
