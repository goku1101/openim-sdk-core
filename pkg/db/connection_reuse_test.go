//go:build !js

package db

import (
	"context"
	"testing"
	"time"
)

func TestReadsReuseBusyDatabaseConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	pool, err := d.conn.DB()
	if err != nil {
		t.Fatal(err)
	}
	held, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	done := make(chan error, 1)
	go func() { _, e := d.GetAppSDKVersion(ctx); done <- e }()
	select {
	case e := <-done:
		t.Fatalf("read opened another connection while existing connection was busy: %v", e)
	case <-time.After(30 * time.Millisecond):
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("read failed to resume after connection release")
	}
	if got := pool.Stats().OpenConnections; got != 1 {
		t.Fatalf("open connections=%d", got)
	}
}

func TestFailedUnreadUpdateReleasesConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.conn.Exec("INSERT INTO local_conversations (conversation_id, unread_count) VALUES ('qa', 2)").Error; err != nil {
		t.Fatal(err)
	}
	if err = d.conn.Exec("CREATE TRIGGER qa_reject_unread BEFORE UPDATE ON local_conversations BEGIN SELECT RAISE(ABORT, 'qa'); END").Error; err != nil {
		t.Fatal(err)
	}
	if err = d.DecrConversationUnreadCount(ctx, "qa", 1); err == nil {
		t.Fatal("expected write failure")
	}
	if _, err = d.GetAppSDKVersion(ctx); err != nil {
		t.Fatalf("failed update retained connection: %v", err)
	}
}
