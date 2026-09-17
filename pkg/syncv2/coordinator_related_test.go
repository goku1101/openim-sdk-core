package syncv2

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

type relatedCheckpointStore struct {
	CoordinatorStore
	current *model_struct.LocalConversationSyncV2
}

func (s relatedCheckpointStore) GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error) {
	return s.current, nil
}

func TestReloadAfterRelatedPreservesForegroundCheckpoint(t *testing.T) {
	before := &model_struct.LocalConversationSyncV2{UserID: "u", PolicyVersion: "p", SyncDomainID: "d", RecoveryVersion: "r"}
	current := *before
	current.Revision++
	current.SelectedIDs = model_struct.StringArray{"foreground-chat"}
	got, err := reloadAfterRelated(context.Background(), relatedCheckpointStore{current: &current}, before)
	if err != nil || got != &current {
		t.Fatalf("foreground checkpoint was not retained: %v", err)
	}
}

func TestReloadAfterRelatedRejectsReplacedBaseline(t *testing.T) {
	before := &model_struct.LocalConversationSyncV2{UserID: "u", PolicyVersion: "p", SyncDomainID: "d", RecoveryVersion: "r"}
	for _, field := range []string{"missing", "user", "policy", "domain", "recovery", "generation"} {
		t.Run(field, func(t *testing.T) {
			current := *before
			store := relatedCheckpointStore{current: &current}
			switch field {
			case "missing":
				store.current = nil
			case "user":
				current.UserID = "other"
			case "policy":
				current.PolicyVersion = "other"
			case "domain":
				current.SyncDomainID = "other"
			case "recovery":
				current.RecoveryVersion = "other"
			case "generation":
				current.Generation = "other"
			}
			if _, err := reloadAfterRelated(context.Background(), store, before); err == nil {
				t.Fatal("replaced baseline was accepted")
			}
		})
	}
}
