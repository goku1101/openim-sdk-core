package syncv2

import (
	"fmt"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/protocol/sdkws"
)

type PushChat struct {
	ConversationID string
	Activate       bool
}

type PushPlan struct {
	Chats                  []PushChat
	Notifications          []*pb.PullRequest
	UnknownNotificationIDs []string
	Ephemeral              *sdkws.PushMessages
}

// PlanPush separates transient events from durable streams. Notification parent
// IDs come from the server checkpoint, never by guessing a chat prefix.
func PlanPush(checkpoint *model_struct.LocalConversationSyncV2, push *sdkws.PushMessages) (*PushPlan, error) {
	if checkpoint == nil || checkpoint.UserID == "" || push == nil {
		return nil, fmt.Errorf("sync v2: invalid push context")
	}
	plan := &PushPlan{Ephemeral: &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{}, NotificationMsgs: map[string]*sdkws.PullMsgs{}}}
	selected := make(map[string]bool, len(checkpoint.SelectedIDs))
	for _, id := range checkpoint.SelectedIDs {
		selected[id] = true
	}
	for id, batch := range push.Msgs {
		if id == "" || batch == nil {
			return nil, fmt.Errorf("sync v2: invalid pushed chat")
		}
		persistent, activate := false, false
		for _, message := range batch.Msgs {
			if message == nil || message.Seq < 0 {
				return nil, fmt.Errorf("sync v2: invalid pushed message")
			}
			if message.Seq == 0 {
				appendEphemeral(plan.Ephemeral.Msgs, id, message)
				continue
			}
			persistent = true
			if message.Status < constant.MsgStatusHasDeleted && message.ContentType >= constant.Text && message.ContentType < constant.NotificationBegin && message.ContentType != constant.Typing && utils.GetSwitchFromOptions(message.Options, constant.IsConversationUpdate) && (message.SendID != checkpoint.UserID || utils.GetSwitchFromOptions(message.Options, constant.IsSenderConversationUpdate)) {
				activate = true
			}
		}
		if persistent && (selected[id] || activate) {
			plan.Chats = append(plan.Chats, PushChat{ConversationID: id, Activate: !selected[id] && activate})
		}
	}
	parents := make(map[string]string, len(checkpoint.NotificationIDs))
	ambiguous := make(map[string]bool)
	for _, id := range checkpoint.NotificationIDs {
		stream, err := pullStreamID(&pb.PullRequest{ConversationID: id, Notifications: true})
		if err != nil {
			return nil, err
		}
		if previous, ok := parents[stream]; ok && previous != id {
			ambiguous[stream] = true
		}
		parents[stream] = id
	}
	selfID := "n_" + checkpoint.UserID + "_" + checkpoint.UserID
	for id, batch := range push.NotificationMsgs {
		if id == "" || batch == nil {
			return nil, fmt.Errorf("sync v2: invalid pushed notification")
		}
		persistent := false
		for _, message := range batch.Msgs {
			if message == nil || message.Seq < 0 {
				return nil, fmt.Errorf("sync v2: invalid pushed notification message")
			}
			if message.Seq == 0 {
				appendEphemeral(plan.Ephemeral.NotificationMsgs, id, message)
			} else {
				persistent = true
			}
		}
		if !persistent {
			continue
		}
		if id == selfID {
			plan.Notifications = append(plan.Notifications, &pb.PullRequest{UserID: checkpoint.UserID, Notifications: true, Self: true})
			continue
		}
		parent, ok := parents[id]
		if !ok || ambiguous[id] {
			plan.UnknownNotificationIDs = append(plan.UnknownNotificationIDs, id)
			continue
		}
		plan.Notifications = append(plan.Notifications, &pb.PullRequest{UserID: checkpoint.UserID, ConversationID: parent, Notifications: true})
	}
	sort.Slice(plan.Chats, func(i, j int) bool { return plan.Chats[i].ConversationID < plan.Chats[j].ConversationID })
	sort.Slice(plan.Notifications, func(i, j int) bool {
		return plan.Notifications[i].ConversationID < plan.Notifications[j].ConversationID
	})
	sort.Strings(plan.UnknownNotificationIDs)
	return plan, nil
}

func appendEphemeral(target map[string]*sdkws.PullMsgs, id string, message *sdkws.MsgData) {
	if target[id] == nil {
		target[id] = &sdkws.PullMsgs{}
	}
	target[id].Msgs = append(target[id].Msgs, message)
}
