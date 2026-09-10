package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

type metadataTimingTransport struct {
	started  chan string
	released map[string]chan struct{}
	active   atomic.Int32
	peak     atomic.Int32
	fail     string
}

func (tr *metadataTimingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var req pb.StateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, err
	}
	id := req.ConversationIDs[0]
	active := tr.active.Add(1)
	defer tr.active.Add(-1)
	for old := tr.peak.Load(); active > old; old = tr.peak.Load() {
		if tr.peak.CompareAndSwap(old, active) {
			break
		}
	}
	tr.started <- id
	select {
	case <-tr.released[id]:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	body := `{"errCode":0,"data":{"unavailableIDs":["` + id + `"]}}`
	if id == tr.fail {
		body = `{"errCode":8,"errMsg":"SYNC_V2_READ_CONCURRENCY"}`
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
}

type metadataTimingStore struct {
	current  *model_struct.LocalConversationSyncV2
	response *pb.StateResponse
	commits  int
}

func (s *metadataTimingStore) GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error) {
	return map[string]uint64{}, nil
}
func (s *metadataTimingStore) ApplyConversationSyncV2Metadata(_ context.Context, before, next *model_struct.LocalConversationSyncV2, response *pb.StateResponse, _ []string) error {
	s.commits++
	s.response = response
	n := *next
	n.Revision = before.Revision + 1
	s.current = &n
	return nil
}
func (s *metadataTimingStore) GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error) {
	return s.current, nil
}

func TestMetadataStateBatchesBoundConcurrencyAndPreserveOrder(t *testing.T) {
	for _, mode := range []string{"success", "failure", "late_failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ids := []string{"a", "b", "c", "d"}
			tr := &metadataTimingTransport{started: make(chan string, 4), released: map[string]chan struct{}{}}
			for _, id := range ids {
				tr.released[id] = make(chan struct{})
			}
			if mode == "failure" {
				tr.fail = "b"
			} else if mode == "late_failure" {
				tr.fail = "d"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var progress [][2]int
			ctx = withStateProgress(ctx, func(done, total int) { progress = append(progress, [2]int{done, total}) })
			store := new(metadataTimingStore)
			completed := make(chan error, 1)
			go func() {
				_, err := NewClient("http://metadata.test", "", &http.Client{Transport: tr}).ApplyMetadataDelta(ctx, "op", &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", VersionID: "v", Version: 1, SelectedIDs: ids}, &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1}, &pb.Capabilities{MaxBatchSize: 1, MaxRequestBytes: 65536}, store)
				completed <- err
			}()
			finished := false
			defer func() {
				cancel()
				if !finished {
					select {
					case <-completed:
					case <-time.After(time.Second):
						t.Error("metadata workers did not stop")
					}
				}
			}()
			next := func() string {
				select {
				case id := <-tr.started:
					return id
				case <-time.After(time.Second):
					t.Fatal("two independent metadata requests did not overlap")
					return ""
				}
			}
			first := map[string]bool{next(): true, next(): true}
			if !first["a"] || !first["b"] {
				t.Fatalf("unexpected first wave: %v", first)
			}
			if mode == "cancel" {
				cancel()
			} else {
				close(tr.released["b"])
			}
			if mode == "success" || mode == "late_failure" {
				close(tr.released["a"])
				second := map[string]bool{next(): true, next(): true}
				if !second["c"] || !second["d"] {
					t.Fatalf("unexpected second wave: %v", second)
				}
				close(tr.released["d"])
				if mode == "success" {
					close(tr.released["c"])
				}
			}
			var err error
			select {
			case err = <-completed:
				finished = true
			case <-time.After(time.Second):
				t.Fatal("request failure did not cancel sibling")
			}
			if tr.active.Load() != 0 || tr.peak.Load() != 2 {
				t.Fatalf("incorrect concurrency: active=%d peak=%d", tr.active.Load(), tr.peak.Load())
			}
			if mode == "success" {
				if err != nil || store.commits != 1 || !reflect.DeepEqual(store.response.UnavailableIDs, ids) || !reflect.DeepEqual(progress, [][2]int{{2, 4}, {4, 4}}) {
					t.Fatalf("incomplete or reordered commit: %v %+v %v", err, store, progress)
				}
			} else {
				var expectedProgress [][2]int
				if mode == "late_failure" {
					expectedProgress = [][2]int{{2, 4}}
				}
				if err == nil || store.commits != 0 || !reflect.DeepEqual(progress, expectedProgress) {
					t.Fatalf("failed wave published partial results: %v %+v %v", err, store, progress)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
				if mode == "failure" || mode == "late_failure" {
					var api *APIError
					if !errors.As(err, &api) || api.ErrCode != 8 || api.ErrMsg != "SYNC_V2_READ_CONCURRENCY" {
						t.Fatalf("coded failure lost: %v", err)
					}
				}
			}
		})
	}
}
