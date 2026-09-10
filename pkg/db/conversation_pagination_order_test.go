//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"testing"
)

func TestConversationPaginationSortsNullableDraftTimesBeforePaging(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "9007199254740993", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	for _, row := range []struct {
		id     string
		time   int64
		pinned bool
	}{
		{"old", 10, false}, {"new", 100, false}, {"pinned", 1, true}, {"tie-b", 50, false}, {"tie-a", 50, false}, {"draft", 5, false},
	} {
		if err := d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: row.id, LatestMsgSendTime: row.time, IsPinned: row.pinned}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.conn.Model(&model_struct.LocalConversation{}).Where("conversation_id <> ?", "draft").Update("draft_text_time", nil).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.conn.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", "draft").Update("draft_text_time", 200).Error; err != nil {
		t.Fatal(err)
	}
	expected := []string{"pinned", "draft", "new", "tie-a", "tie-b", "old"}
	all, err := d.GetAllConversationListDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range all {
		if row.ConversationID != expected[i] {
			t.Fatalf("all item %d got %s want %s", i, row.ConversationID, expected[i])
		}
	}

	for offset := 0; offset < len(expected); offset += 2 {
		page, err := d.GetConversationListSplitDB(ctx, offset, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 2 {
			t.Fatalf("page %d length %d", offset, len(page))
		}
		for i, row := range page {
			if row.ConversationID != expected[offset+i] {
				t.Fatalf("page %d item %d got %s want %s", offset, i, row.ConversationID, expected[offset+i])
			}
		}
	}
}
