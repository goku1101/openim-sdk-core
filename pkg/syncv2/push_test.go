package syncv2

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/protocol/sdkws"
	"reflect"
	"testing"
)

func TestPushPlanSeparatesNotificationScopeAndChatActivation(t *testing.T) {
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", SelectedIDs: []string{"si_loaded_u"}, NotificationIDs: []string{"sg_deferred", "si_a_b_u"}}
	text := func(seq int64) *sdkws.MsgData {
		return &sdkws.MsgData{Seq: seq, ClientMsgID: "m", SendID: "peer", ContentType: constant.Text}
	}
	suppressed := text(4)
	suppressed.Options = map[string]bool{constant.IsConversationUpdate: false}
	selfSuppressed := text(5)
	selfSuppressed.SendID = "u"
	selfSuppressed.Options = map[string]bool{constant.IsSenderConversationUpdate: false}
	deleted := text(7)
	deleted.Status = constant.MsgStatusHasDeleted
	push := &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{
		"si_loaded_u":     {Msgs: []*sdkws.MsgData{suppressed}},
		"si_new_u":        {Msgs: []*sdkws.MsgData{text(2), text(3)}},
		"si_suppressed_u": {Msgs: []*sdkws.MsgData{suppressed}},
		"si_self_u":       {Msgs: []*sdkws.MsgData{selfSuppressed}},
		"si_deleted_u":    {Msgs: []*sdkws.MsgData{deleted}},
		"si_transient_u":  {Msgs: []*sdkws.MsgData{{ContentType: constant.Typing}}},
	}, NotificationMsgs: map[string]*sdkws.PullMsgs{
		"n_deferred":   {Msgs: []*sdkws.MsgData{text(10)}},
		"n_a_b_u":      {Msgs: []*sdkws.MsgData{text(11)}},
		"n_u_u":        {Msgs: []*sdkws.MsgData{text(12)}},
		"n_new_parent": {Msgs: []*sdkws.MsgData{text(13)}},
	}}
	plan, err := PlanPush(cp, push)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.Chats, []PushChat{{ConversationID: "si_loaded_u"}, {ConversationID: "si_new_u", Activate: true}}) {
		t.Fatalf("wrong activation: %+v", plan.Chats)
	}
	if len(plan.Notifications) != 3 || !plan.Notifications[0].Self || plan.Notifications[0].ConversationID != "" || plan.Notifications[1].ConversationID != "sg_deferred" || plan.Notifications[2].ConversationID != "si_a_b_u" {
		t.Fatalf("notification scope/mapping changed: %+v", plan.Notifications)
	}
	if !reflect.DeepEqual(plan.UnknownNotificationIDs, []string{"n_new_parent"}) {
		t.Fatalf("unknown parent was guessed: %v", plan.UnknownNotificationIDs)
	}
	if len(plan.Ephemeral.Msgs) != 1 || plan.Ephemeral.Msgs["si_transient_u"].Msgs[0].ContentType != constant.Typing {
		t.Fatal("transient event entered durable sync")
	}
	if len(cp.SelectedIDs) != 1 || len(cp.NotificationIDs) != 2 {
		t.Fatal("planning mutated persisted selection")
	}
}

func TestPushPlanRefusesAmbiguousNotificationParents(t *testing.T) {
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", NotificationIDs: []string{"sg_a_b", "si_a_b"}}
	plan, err := PlanPush(cp, &sdkws.PushMessages{NotificationMsgs: map[string]*sdkws.PullMsgs{"n_a_b": {Msgs: []*sdkws.MsgData{{Seq: 1}}}}})
	if err != nil || len(plan.Notifications) != 0 || !reflect.DeepEqual(plan.UnknownNotificationIDs, []string{"n_a_b"}) {
		t.Fatalf("ambiguous parent silently selected: %+v %v", plan, err)
	}
}
