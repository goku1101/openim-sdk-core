package db

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func Test_BatchInsertNotificationSeq(t *testing.T) {
	ctx := context.Background()
	db, err := NewDataBase(ctx, "9007199254740993", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	notificationSeqs := []*model_struct.NotificationSeqs{
		{
			ConversationID: "n_2882899447_69085919821",
			Seq:            2,
		},
		{
			ConversationID: "n_2882899447_69085919822",
			Seq:            3,
		},
	}
	err = db.BatchInsertNotificationSeq(ctx, notificationSeqs)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.GetNotificationAllSeqs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(notificationSeqs) {
		t.Fatalf("got %d rows, want %d", len(rows), len(notificationSeqs))
	}
	byID := make(map[string]int64, len(rows))
	for _, row := range rows {
		byID[row.ConversationID] = row.Seq
	}
	for _, want := range notificationSeqs {
		if got, ok := byID[want.ConversationID]; !ok || got != want.Seq {
			t.Fatalf("sequence for %s = %d (present=%v), want %d", want.ConversationID, got, ok, want.Seq)
		}
	}

}
