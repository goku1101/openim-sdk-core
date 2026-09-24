package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type snapshotSaveStore struct {
	CoordinatorStore
	saves  int
	failAt int
	cancel context.CancelFunc
}

func (s *snapshotSaveStore) SaveConversationSyncV2(ctx context.Context, cp *model_struct.LocalConversationSyncV2) error {
	s.saves++
	if s.saves == s.failAt {
		if s.cancel != nil {
			s.cancel()
		}
		return errors.New("injected storage failure")
	}
	return nil
}

func TestPrioritySnapshotLifecycle(t *testing.T) {
	for _, mode := range []string{"invalid_response", "cancel_before_save", "saved_resume", "replace_invalidated", "replace_release_failure"} {
		t.Run(mode, func(t *testing.T) {
			active := map[string]bool{}
			creates, quota, releases := 0, 0, 0
			var before *model_struct.LocalConversationSyncV2
			if mode == "replace_invalidated" || mode == "replace_release_failure" {
				active["old"] = true
				before = &model_struct.LocalConversationSyncV2{UserID: "u", SnapshotID: "old", NotificationTask: &model_struct.ConversationSyncV2Task{Status: "invalidated"}}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/conversation/sync_snapshot":
					if len(active) >= 2 {
						quota++
						fmt.Fprint(w, `{"errCode":8,"errMsg":"SYNC_V2_SNAPSHOT_QUOTA"}`)
						return
					}
					creates++
					id := fmt.Sprintf("snapshot-%d", creates)
					active[id] = true
					p := &pb.SnapshotResponse{SnapshotID: id, Scope: "notifications", PolicyVersion: PolicyVersion, Mode: "full", VersionID: "v", Version: 1, DirectoryToken: strings.Repeat("a", 64), ExpiresAt: time.Now().Add(time.Hour).Unix(), BaselineCursor: "base", IncludeSelf: true}
					if mode == "invalid_response" || mode == "replace_invalidated" {
						p.DirectoryToken = "invalid"
					}
					json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": p})
				case "/conversation/sync_snapshot_release":
					if mode == "replace_release_failure" {
						w.WriteHeader(503)
						return
					}
					var q pb.ReleaseRequest
					json.NewDecoder(r.Body).Decode(&q)
					delete(active, q.SnapshotID)
					releases++
					fmt.Fprint(w, `{"errCode":0}`)
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			c := NewClient(server.URL, "", server.Client())
			caps := &pb.Capabilities{MaxPageSize: 500, MaxNotificationPriority: 500}
			rounds := 3
			if mode == "saved_resume" || mode == "replace_invalidated" {
				rounds = 1
			}
			for i := 0; i < rounds; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				store := &snapshotSaveStore{cancel: cancel}
				if mode == "cancel_before_save" {
					store.failAt = 1
				}
				if mode == "saved_resume" {
					store.failAt = 4
				}
				_, err := c.synchronizePriority(ctx, "test", "u", store, CoordinatorHooks{}, caps, before)
				cancel()
				if err == nil {
					t.Fatal("expected injected failure")
				}
			}
			if mode == "replace_release_failure" {
				if creates != 0 || len(active) != 1 {
					t.Fatalf("created snapshot despite failed cleanup: creates=%d active=%d", creates, len(active))
				}
			} else if mode == "cancel_before_save" {
				if creates != 0 || len(active) != 0 {
					t.Fatal("request sent before durable intent")
				}
			} else if mode == "saved_resume" {
				if len(active) != 1 || releases != 0 {
					t.Fatalf("durable resumable snapshot lost: active=%d releases=%d", len(active), releases)
				}
			} else if quota != 0 || len(active) != 0 || creates != rounds {
				t.Fatalf("snapshot leak: creates=%d quota=%d active=%d releases=%d", creates, quota, len(active), releases)
			}
		})
	}
}
