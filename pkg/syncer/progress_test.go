package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/page"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type progressRequest struct {
	Pagination *sdkws.RequestPagination `json:"pagination"`
}

func (r *progressRequest) GetPagination() *sdkws.RequestPagination { return r.Pagination }

type progressResponse struct {
	Items []int `json:"items"`
}

func TestFullSyncProgressReportsOnlyCommittedRows(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "fallback_failure"}[fallback], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req progressRequest
				json.NewDecoder(r.Body).Decode(&req)
				rows := []int{1, 2}
				if req.Pagination.PageNumber > 1 {
					rows = []int{3}
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": progressResponse{rows}})
			}))
			defer server.Close()
			cfg := &ccontext.GlobalConfig{}
			cfg.ApiAddr = server.URL
			ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), cfg), "progress-test")
			stored := 0
			s := &Syncer[int, progressResponse, int]{
				deleteAll:                func(context.Context, string) error { return nil },
				batchPageReq:             func(string) page.PageReq { return &progressRequest{&sdkws.RequestPagination{ShowNumber: 2}} },
				batchPageRespConvertFunc: func(r *progressResponse) []int { return r.Items },
				reqApiRouter:             "/items",
				batchInsert: func(ctx context.Context, rows []int) error {
					if fallback {
						return errors.New("batch failure")
					}
					stored += len(rows)
					return nil
				},
				insert: func(ctx context.Context, row int) error {
					if row == 2 {
						return errors.New("row failure")
					}
					stored++
					return nil
				},
			}
			var reports []int
			err := s.FullSyncWithProgress(ctx, "fixture", func(n int) {
				if n != stored {
					t.Errorf("reported before commit: %d != %d", n, stored)
				}
				reports = append(reports, n)
			})
			want := []int{0, 2, 3}
			if fallback {
				want = []int{0, 1, 2}
			}
			if !reflect.DeepEqual(reports, want) {
				t.Fatalf("counts %v want %v", reports, want)
			}
			if (err != nil) != fallback {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
}
