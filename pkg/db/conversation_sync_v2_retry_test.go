//go:build !js

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func TestSyncV2RetryOrderedLookupAfterMigration(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			d, err := NewDataBase(ctx, "9007199254740993", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close(ctx) }()
			jobs := make([]*model_struct.LocalConversationSyncV2Retry, 0, 1001)
			for i := 999; i >= 0; i-- {
				jobs = append(jobs, &model_struct.LocalConversationSyncV2Retry{TaskID: fmt.Sprintf("task-%04d", i), Kind: "reconcile", Payload: `{}`, DueAt: 100, Revision: 1})
			}
			jobs = append(jobs, &model_struct.LocalConversationSyncV2Retry{TaskID: "earliest", Kind: "push", Payload: `{}`, DueAt: 50, Revision: 1})
			if err = d.conn.CreateInBatches(jobs, 100).Error; err != nil {
				t.Fatal(err)
			}
			if legacy {
				if err = d.conn.Exec("DROP INDEX IF EXISTS idx_sync_v2_retry_order").Error; err != nil {
					t.Fatal(err)
				}
				if err = d.Close(ctx); err != nil {
					t.Fatal(err)
				}
				d, err = NewDataBase(ctx, "9007199254740993", dir, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			var plan []struct{ Detail string }
			if err = d.conn.Raw("EXPLAIN QUERY PLAN SELECT * FROM local_conversation_sync_v2_retries ORDER BY due_at ASC, task_id ASC LIMIT 1").Scan(&plan).Error; err != nil {
				t.Fatal(err)
			}
			indexed := false
			for _, step := range plan {
				if strings.Contains(step.Detail, "TEMP B-TREE") {
					t.Fatalf("retry lookup sorts the queue: %s", step.Detail)
				}
				indexed = indexed || strings.Contains(step.Detail, "USING INDEX")
			}
			if !indexed {
				t.Fatalf("retry lookup does not use an ordered index: %+v", plan)
			}
			first, err := d.NextConversationSyncV2Retry(ctx)
			if err != nil || first == nil || first.TaskID != "earliest" {
				t.Fatalf("deadline order changed: %+v %v", first, err)
			}
			if err = d.AcknowledgeConversationSyncV2Retry(ctx, first); err != nil {
				t.Fatal(err)
			}
			next, err := d.NextConversationSyncV2Retry(ctx)
			if err != nil || next == nil || next.TaskID != "task-0000" {
				t.Fatalf("tie order changed: %+v %v", next, err)
			}
			if err = d.RescheduleConversationSyncV2Retry(ctx, next, 200); err != nil {
				t.Fatal(err)
			}
			next, err = d.NextConversationSyncV2Retry(ctx)
			if err != nil || next == nil || next.TaskID != "task-0001" {
				t.Fatalf("reschedule order changed: %+v %v", next, err)
			}
		})
	}
}

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
