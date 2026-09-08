package group

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

func TestSyncV2PermissionNotificationsInvalidateBeforeFurtherProcessing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   int32
		detail any
	}{
		{"quit", constant.MemberQuitNotification, &sdkws.MemberQuitTips{Group: &sdkws.GroupInfo{GroupID: "group_9007199254740993"}, QuitUser: &sdkws.GroupMemberFullInfo{UserID: "u"}}},
		{"kicked", constant.MemberKickedNotification, &sdkws.MemberKickedTips{Group: &sdkws.GroupInfo{GroupID: "group_9007199254740993"}, KickedUserList: []*sdkws.GroupMemberFullInfo{{UserID: "other"}, {UserID: "u"}}}},
		{"dismissed", constant.GroupDismissedNotification, &sdkws.GroupDismissedTips{Group: &sdkws.GroupInfo{GroupID: "group_9007199254740993"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &Group{loginUserID: "u"}
			stop := errors.New("stop before group synchronization")
			calls := 0
			g.SetConversationSyncV2Invalidator(func(ctx context.Context, id string) error {
				calls++
				if id != "group_9007199254740993" {
					t.Errorf("group identity altered: %s", id)
				}
				return stop
			})
			detail, err := json.Marshal(tc.detail)
			if err != nil {
				t.Fatal(err)
			}
			content, err := json.Marshal(map[string]string{"detail": string(detail)})
			if err != nil {
				t.Fatal(err)
			}
			if err = g.DoNotification(context.Background(), &sdkws.MsgData{ContentType: tc.kind, Content: content}); !errors.Is(err, stop) || calls != 1 {
				t.Fatalf("permission invalidation not propagated first: calls=%d err=%v", calls, err)
			}
		})
	}
}
