//go:build !js

package db

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func TestSyncV2RetrySurvivesRestartAndOldAcknowledgement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	d, err := NewDataBase(ctx, "9007199254740993", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	job := &model_struct.LocalConversationSyncV2Retry{TaskID: "push-9007199254740995", Kind: "push", Payload: `{"conversationID":"9007199254740995","seqs":[7]}`, DueAt: 100}
	if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
		t.Fatal(err)
	}
	first, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || first == nil || first.Revision != 1 {
		t.Fatalf("missing task: %+v %v", first, err)
	}
	if err = d.RescheduleConversationSyncV2Retry(ctx, first, 500); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = NewDataBase(ctx, "9007199254740993", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	got, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || got == nil || got.Payload != job.Payload || got.DueAt != 500 || got.Attempts != 1 {
		t.Fatalf("retry lost at restart: %+v %v", got, err)
	}
	job.Payload = `{"conversationID":"9007199254740995","seqs":[8]}`
	job.DueAt = 200
	if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err = d.AcknowledgeConversationSyncV2Retry(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err = d.RescheduleConversationSyncV2Retry(ctx, got, 999); err != nil {
		t.Fatal(err)
	}
	next, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || next == nil || next.Payload != job.Payload || next.DueAt != 200 || next.Attempts != 0 || next.Revision <= got.Revision {
		t.Fatalf("old worker changed new work: %+v %v", next, err)
	}
	if err = d.AcknowledgeConversationSyncV2Retry(ctx, next); err != nil {
		t.Fatal(err)
	}
	if next, err = d.NextConversationSyncV2Retry(ctx); err != nil || next != nil {
		t.Fatalf("ack failed: %+v %v", next, err)
	}
}

func TestSyncV2RetryDeadlineAndAccountIsolation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	d, err := NewDataBase(ctx, "a", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	other, err := NewDataBase(ctx, "b", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	for _, job := range []*model_struct.LocalConversationSyncV2Retry{
		{TaskID: "later", Kind: "push", Payload: `{}`, DueAt: 300},
		{TaskID: "sooner", Kind: "reconcile", Payload: `{}`, DueAt: 100},
	} {
		if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	if next, err := d.NextConversationSyncV2Retry(ctx); err != nil || next == nil || next.TaskID != "sooner" {
		t.Fatalf("wrong ordering: %+v %v", next, err)
	}
	if next, err := other.NextConversationSyncV2Retry(ctx); err != nil || next != nil {
		t.Fatalf("cross-account work: %+v %v", next, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = d.PutConversationSyncV2Retry(canceled, &model_struct.LocalConversationSyncV2Retry{TaskID: "canceled", Kind: "push", Payload: `{}`}); err == nil {
		t.Fatal("canceled login wrote a retry")
	}
}

func TestSyncV2RetryPayloadPruningPreservesNewerWork(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	job := &model_struct.LocalConversationSyncV2Retry{TaskID: "push", Kind: "push", Payload: `{"streams":["pending","denied"]}`, DueAt: 123}
	if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
		t.Fatal(err)
	}
	current, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale := *current
	if err = d.UpdateConversationSyncV2RetryPayload(ctx, current, `{"streams":["pending"]}`); err != nil {
		t.Fatal(err)
	}
	if current.Revision != stale.Revision+1 {
		t.Fatal("payload update did not invalidate old receipt")
	}
	if err = d.UpdateConversationSyncV2RetryPayload(ctx, &stale, `{}`); err == nil {
		t.Fatal("stale worker replaced current payload")
	}
	if err = d.AcknowledgeConversationSyncV2Retry(ctx, &stale); err != nil {
		t.Fatal(err)
	}
	got, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || got == nil || got.Payload != current.Payload || got.DueAt != 123 {
		t.Fatalf("pruning lost unrelated work/deadline: %+v %v", got, err)
	}
	if err = d.AcknowledgeConversationSyncV2Retry(ctx, current); err != nil {
		t.Fatal(err)
	}
	got, err = d.NextConversationSyncV2Retry(ctx)
	if err != nil || got != nil {
		t.Fatal("current worker could not acknowledge pruned task")
	}
}

func TestUnchangedReconcileRetainsBackoffAfterRestart(t *testing.T) {
	for _, kind := range []string{"reconcile", "state"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			d, err := NewDataBase(ctx, "u", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			job := &model_struct.LocalConversationSyncV2Retry{TaskID: "reconcile-c", Kind: kind, Payload: `{"conversationID":"c"}`, DueAt: 100}
			if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
				t.Fatal(err)
			}
			old, err := d.NextConversationSyncV2Retry(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = d.RescheduleConversationSyncV2Retry(ctx, old, 10000); err != nil {
				t.Fatal(err)
			}
			old, err = d.NextConversationSyncV2Retry(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Close(ctx); err != nil {
				t.Fatal(err)
			}
			d, err = NewDataBase(ctx, "u", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			job.DueAt = 200
			if err = d.PutConversationSyncV2Retry(ctx, job); err != nil {
				t.Fatal(err)
			}
			if err = d.AcknowledgeConversationSyncV2Retry(ctx, old); err != nil {
				t.Fatal(err)
			}
			next, err := d.NextConversationSyncV2Retry(ctx)
			if err != nil || next == nil || next.DueAt != 10000 || next.Attempts != 1 || next.Revision <= old.Revision {
				t.Fatalf("restart reset retry backoff or accepted stale ack: %v %v", next, err)
			}

		})
	}
}
