package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestScanBaselineSurvivesProtocolDecoding(t *testing.T) {
	p := new(pb.ScanResponse)
	if err := protojson.Unmarshal([]byte(`{"baselineCursor":"opaque-base","live":true,"scope":"chat"}`), p); err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	q := new(pb.ScanResponse)
	if err := proto.Unmarshal(data, q); err != nil {
		t.Fatal(err)
	}
	if q.BaselineCursor != "opaque-base" {
		t.Fatal("baseline lost")
	}
}

func TestScanPageContractRejectsUnsafeContinuation(t *testing.T) {
	for _, name := range []string{"unchanged-cursor", "baseline-changed", "duplicate-ID", "wrong-scope", "expired", "empty-baseline"} {
		t.Run(name, func(t *testing.T) {
			task := &model_struct.ConversationSyncV2ScanScope{Started: true, Cursor: "old", Baseline: "base"}
			p := &pb.ScanResponse{Live: true, Scope: "chat", PolicyVersion: PolicyVersion, VersionID: "v", ExpiresAt: time.Now().Add(time.Hour).Unix(), BaselineCursor: "base", Cursor: "new", HasMore: true, Scanned: 2, ConversationIDs: []string{"a", "b"}}
			switch name {
			case "unchanged-cursor":
				p.Cursor = "old"
			case "baseline-changed":
				p.BaselineCursor = "other"
			case "duplicate-ID":
				p.ConversationIDs = []string{"a", "a"}
			case "wrong-scope":
				p.Scope = "notifications"
			case "expired":
				p.ExpiresAt = 1
			case "empty-baseline":
				p.BaselineCursor = ""
			}
			if validateScanPage(p, "chat", task, 100) == nil {
				t.Fatal("accepted unsafe page")
			}
		})
	}
}

type scanContractStore struct {
	CoordinatorStore
	cp *model_struct.LocalConversationSyncV2
}

func (s *scanContractStore) GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error) {
	return s.cp, nil
}
func (s *scanContractStore) SaveConversationSyncV2(_ context.Context, p *model_struct.LocalConversationSyncV2) error {
	s.cp = p
	return nil
}
func TestScanFallbackOnlyForMissingBaselineOrUnsupported(t *testing.T) {
	for _, mode := range []string{"missing", "unsupported", "busy"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "busy" {
					w.WriteHeader(503)
					return
				}
				if mode == "unsupported" {
					fmt.Fprint(w, `{"errCode":12,"errMsg":"SYNC_V2_SCAN_UNSUPPORTED"}`)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.ScanResponse{Live: true, Scope: "notifications", IncludeSelf: true, VersionID: "v", PolicyVersion: PolicyVersion, ExpiresAt: time.Now().Add(time.Hour).Unix()}})
			}))
			defer server.Close()
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", PolicyVersion: PolicyVersion, SyncDomainID: "domain", RecoveryVersion: RecoveryVersion, ScanTask: &model_struct.ConversationSyncV2ScanTask{PageSize: 100, SelfDone: true}}
			c := NewClient(server.URL, "", server.Client())
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := c.synchronizeScan(ctx, "op", "u", &scanContractStore{cp: cp}, CoordinatorHooks{}, &pb.Capabilities{MaxScanPageSize: 100, SyncDomainID: "domain", IncrementalRecoveryVersion: RecoveryVersion}, cp)
			if (mode != "busy") != errors.Is(err, errScanBaselineRequired) {
				t.Fatalf("unexpected fallback: %v", err)
			}
			if mode == "busy" && err == nil {
				t.Fatal("busy accepted")
			}
		})
	}
}
func TestSynchronizationRejectsConcurrentRound(t *testing.T) {
	c := NewClient("", "", nil)
	c.synchronizing.Store(true)
	_, err := c.Synchronize(context.Background(), "op", "u", nil, CoordinatorHooks{})
	if !errors.Is(err, ErrCatchupPending) {
		t.Fatal(err)
	}
}
