package syncv2

import (
	"encoding/json"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// PendingRetryTasks retains only unfinished streams, never the full snapshot.
// Notification IDs are resolved against the authoritative checkpoint parents.
func PendingRetryTasks(result *SyncResult, dueAt int64) ([]*model_struct.LocalConversationSyncV2Retry, error) {
	if result == nil || result.Checkpoint == nil {
		return nil, fmt.Errorf("sync v2: missing retry checkpoint")
	}
	cp := result.Checkpoint
	requests := make(map[string]*pb.PullRequest)
	for _, id := range cp.SelectedIDs {
		requests[id] = &pb.PullRequest{UserID: cp.UserID, ConversationID: id}
	}
	for _, parent := range cp.NotificationIDs {
		req := &pb.PullRequest{UserID: cp.UserID, ConversationID: parent, Notifications: true}
		id, err := pullStreamID(req)
		if err != nil {
			return nil, err
		}
		requests[id] = req
	}
	if cp.IncludeSelf {
		req := &pb.PullRequest{UserID: cp.UserID, Notifications: true, Self: true}
		id, err := pullStreamID(req)
		if err != nil {
			return nil, err
		}
		requests[id] = req
	}
	var jobs []*model_struct.LocalConversationSyncV2Retry
	metadataSeen := make(map[string]bool)
	for _, id := range cp.PendingStateIDs {
		if id == "" {
			return nil, fmt.Errorf("sync v2: empty pending state ID")
		}
		if metadataSeen[id] {
			continue
		}
		metadataSeen[id] = true
		payload, err := json.Marshal(&pb.PullRequest{UserID: cp.UserID, ConversationID: id})
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, &model_struct.LocalConversationSyncV2Retry{TaskID: "state:" + id, Kind: "state", Payload: string(payload), DueAt: dueAt})
	}
	seen := make(map[string]bool)
	for _, streams := range []*StreamResults{result.Notifications, result.Chats} {
		if streams == nil {
			continue
		}
		for _, id := range streams.PendingIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			req := requests[id]
			if req == nil {
				return nil, fmt.Errorf("sync v2: pending stream outside checkpoint")
			}
			payload, err := json.Marshal(req)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, &model_struct.LocalConversationSyncV2Retry{TaskID: "reconcile:" + id, Kind: "reconcile", Payload: string(payload), DueAt: dueAt})
		}
	}
	return jobs, nil
}
