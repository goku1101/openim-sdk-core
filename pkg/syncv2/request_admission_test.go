package syncv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestAdmissionReadCapacityAndSerialControl(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(fmt.Sprint(control), func(t *testing.T) {
			var active, peak atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := active.Add(1)
				defer active.Add(-1)
				for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
				}
				time.Sleep(25 * time.Millisecond)
				fmt.Fprint(w, `{"errCode":0,"data":{}}`)
			}))
			defer server.Close()
			client := NewClient(server.URL, "", server.Client())
			client.clusterAdmission.Store(true)
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var err error
					if control {
						_, err = client.invoke(context.Background(), "control", "/control", &pb.SnapshotRequest{}, &pb.SnapshotResponse{})
					} else {
						_, err = client.Pull(context.Background(), "read", &pb.PullRequest{ConversationID: "fixture", Seqs: []int64{1}})
					}
					if err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			want := int32(2)
			if control {
				want = 1
			}
			if peak.Load() != want {
				t.Fatalf("peak=%d want=%d", peak.Load(), want)
			}
		})
	}
}

func waitAdmissionQueue(t *testing.T, gate *requestAdmission, foreground, background int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		ready := len(gate.foreground) == foreground && len(gate.background) == background
		gate.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("request did not enter the expected admission queue")
}

func TestAdmissionPrioritizesForegroundWithoutStarvingBackground(t *testing.T) {
	var order []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, r.Header.Get("operationID"))
		fmt.Fprint(w, `{"errCode":0,"data":{}}`)
	}))
	defer server.Close()
	client := NewClient(server.URL, "", server.Client())
	client.requestSlot.limit = 1 // isolate queue fairness from available capacity
	client.clusterAdmission.Store(true)
	if err := client.requestSlot.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	foreground, background := 0, 0
	for _, name := range []string{"background-1", "background-2", "foreground-1", "foreground-2", "foreground-3", "foreground-4", "foreground-5"} {
		ctx := context.Background()
		if name[0] == 'f' {
			ctx = WithForegroundRead(ctx)
			foreground++
		} else {
			background++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.Pull(ctx, name, &pb.PullRequest{ConversationID: "fixture", Seqs: []int64{1}}); err != nil {
				t.Error(err)
			}
		}()
		waitAdmissionQueue(t, &client.requestSlot, foreground, background)
	}
	client.requestSlot.release()
	wg.Wait()
	want := []string{"foreground-1", "foreground-2", "foreground-3", "foreground-4", "background-1", "foreground-5", "background-2"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("request order=%v want=%v", order, want)
	}
}

func TestAdmissionCancelGrantRaceDoesNotLeakSlot(t *testing.T) {
	for i := 0; i < 100; i++ {
		gate := &requestAdmission{}
		if err := gate.acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(WithForegroundRead(context.Background()))
		done := make(chan error, 1)
		go func() {
			err := gate.acquire(ctx)
			if err == nil {
				gate.release()
			}
			done <- err
		}()
		waitAdmissionQueue(t, gate, 1, 0)
		go cancel()
		gate.release()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		probe, stop := context.WithTimeout(context.Background(), time.Second)
		if err := gate.acquire(probe); err != nil {
			stop()
			t.Fatalf("lost admission slot: %v", err)
		}
		gate.release()
		stop()
	}
}
