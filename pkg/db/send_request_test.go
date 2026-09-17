//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"sync"
	"testing"
)

func TestFrozenSendRequestFirstWriterAndAccountIsolation(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
	dir := t.TempDir()
	database, err := NewDataBase(ctx, "self", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	original := []byte("original request")
	got, err := database.FreezeSendRequest(ctx, "conversation", "message", original)
	if err != nil || string(got) != string(original) {
		t.Fatalf("first freeze: %q %v", got, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := database.FreezeSendRequest(ctx, "conversation", "message", []byte("changed"))
			if err != nil || string(got) != string(original) {
				t.Errorf("concurrent retry replaced original: %q %v", got, err)
			}
		}()
	}
	wg.Wait()
	otherCtx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "other"})
	other, err := NewDataBase(otherCtx, "other", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(otherCtx)
	got, err = other.FreezeSendRequest(otherCtx, "conversation", "message", []byte("other account"))
	if err != nil || string(got) != "other account" {
		t.Fatal("request crossed account boundary")
	}
	if err = database.DeleteSendRequest(ctx, "conversation", "message"); err != nil {
		t.Fatal(err)
	}
	got, err = database.FreezeSendRequest(ctx, "conversation", "message", []byte("after cleanup"))
	if err != nil || string(got) != "after cleanup" {
		t.Fatal("confirmed request not released")
	}
}

func TestDeletingMessagesRemovesFrozenRequests(t *testing.T) {
	for _, kind := range []string{"all", "mark-all", "selected"} {
		t.Run(kind, func(t *testing.T) {
			ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
			d, err := NewDataBase(ctx, "self", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			if err = d.initChatLog(ctx, "chat"); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"delete", "keep"} {
				if _, err = d.FreezeSendRequest(ctx, "chat", id, []byte("original")); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "all":
				err = d.DeleteConversationAllMessages(ctx, "chat")
			case "mark-all":
				err = d.MarkDeleteConversationAllMessages(ctx, "chat")
			case "selected":
				err = d.DeleteConversationMsgs(ctx, "chat", []string{"delete"})
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := d.FreezeSendRequest(ctx, "chat", "delete", []byte("new"))
			if err != nil || string(got) != "new" {
				t.Fatal("deleted payload retained")
			}
			if kind == "selected" {
				got, err = d.FreezeSendRequest(ctx, "chat", "keep", []byte("new"))
				if err != nil || string(got) != "original" {
					t.Fatal("unrelated retry removed")
				}
			}
		})
	}
}

func TestLateRetryCannotRecreateConfirmedRequest(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
	d, err := NewDataBase(ctx, "self", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.initChatLog(ctx, "chat"); err != nil {
		t.Fatal(err)
	}
	if err = d.InsertMessage(ctx, "chat", &model_struct.LocalChatLog{ClientMsgID: "id", Status: constant.MsgStatusSendFailed}); err != nil {
		t.Fatal(err)
	}
	if _, err = d.FreezeSendRequest(ctx, "chat", "id", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err = d.UpdateMessageTimeAndStatus(ctx, "chat", "id", "server", 123, constant.MsgStatusSendSuccess); err != nil {
		t.Fatal(err)
	}
	if err = d.DeleteSendRequest(ctx, "chat", "id"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.FreezeSendRequest(ctx, "chat", "id", []byte("changed")); !sdkerrs.ErrMsgRepeated.Is(err) {
		t.Fatalf("late retry recreated confirmed identity: %v", err)
	}
}
