package conversation_msg

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/internal/user"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	protocolconstant "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2NotificationErrorsReachConsumer(t *testing.T) {
	cases := []struct {
		name   string
		kind   int32
		handle func(context.Context, *sdkws.MsgData) error
	}{
		{"conversation", constant.ClearConversationNotification, (&Conversation{}).DoNotification},
		{"group", constant.GroupApplicationAcceptedNotification, (&group.Group{}).DoNotification},
		{"friend", constant.FriendAddedNotification, (&relation.Relation{}).DoNotification},
		{"user", protocolconstant.UserInfoUpdatedNotification, (&user.User{}).DoNotification},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.handle(context.Background(), &sdkws.MsgData{ContentType: tc.kind, Content: []byte("invalid-json")}); err == nil {
				t.Fatal("notification failure hidden from progress consumer")
			}
		})
	}
}
