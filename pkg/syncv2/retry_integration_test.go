//go:build !js

package syncv2_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
)

type observedRetryStore struct {
	*db.DataBase
	events chan string
}

func (s *observedRetryStore) RescheduleConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry, due int64) error {
	err := s.DataBase.RescheduleConversationSyncV2Retry(ctx, job, due)
	if err == nil {
		s.events <- "defer:" + job.TaskID
	}
	return err
}
func (s *observedRetryStore) AcknowledgeConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
	err := s.DataBase.AcknowledgeConversationSyncV2Retry(ctx, job)
	if err == nil {
		s.events <- "ack:" + job.TaskID
	}
	return err
}
func awaitRetryEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("event %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("missing event %q", want)
	}
}

func TestSyncV2RetryWorkerBackoffAndEarlierWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	store := &observedRetryStore{d, make(chan string, 8)}
	wake := make(chan struct{}, 1)
	if err = d.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: "hole", Kind: "reconcile", Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var holeCalls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- syncv2.RunRetryTasks(ctx, store, wake, func(_ context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
			if job.TaskID == "hole" {
				holeCalls.Add(1)
				return syncv2.ErrStatePending
			}
			return nil
		})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("worker exit: %v", err)
		}
	}()
	started := time.Now()
	awaitRetryEvent(t, store.events, "defer:hole")
	job, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || job.Attempts != 1 || job.DueAt < started.Add(900*time.Millisecond).UnixMilli() {
		t.Fatalf("no durable backoff: %+v %v", job, err)
	}
	// New work interrupts the timer without resetting the old hole's deadline.
	if err = d.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: "urgent", Kind: "push", Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	awaitRetryEvent(t, store.events, "ack:urgent")
	if holeCalls.Load() != 1 {
		t.Fatal("wake retried a hole before its deadline")
	}
}

func TestSyncV2RetryWorkerPausesPermanentErrorsAndStopsUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	store := &observedRetryStore{d, make(chan string, 8)}
	wake := make(chan struct{}, 1)
	if err = d.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: "paused", Kind: "push", Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- syncv2.RunRetryTasks(ctx, store, wake, func(_ context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
			if job.TaskID == "denied" {
				return &syncv2.APIError{ErrCode: 7, ErrMsg: "SYNC_V2_CONVERSATION_UNAVAILABLE"}
			}
			if attempts.Add(1) == 1 {
				return fmt.Errorf("invalid task data")
			}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	awaitRetryEvent(t, store.events, "defer:paused")
	job, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || job.DueAt != math.MaxInt64 {
		t.Fatalf("permanent error not paused: %+v %v", job, err)
	}
	if err = d.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: "denied", Kind: "push", Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	awaitRetryEvent(t, store.events, "ack:denied")
	if err = d.ResumeConversationSyncV2Retries(ctx, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	awaitRetryEvent(t, store.events, "ack:paused")
}

func TestSyncV2RetryWorkerCancellationKeepsInflightTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	if err = d.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: "push", Kind: "push", Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- syncv2.RunRetryTasks(ctx, d, nil, func(ctx context.Context, _ *model_struct.LocalConversationSyncV2Retry) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	job, err := d.NextConversationSyncV2Retry(context.Background())
	if err != nil || job == nil || job.Attempts != 0 {
		t.Fatalf("logout discarded in-flight work: %+v %v", job, err)
	}
}
