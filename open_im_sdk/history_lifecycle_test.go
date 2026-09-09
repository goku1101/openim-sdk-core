package open_im_sdk

import (
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
)

type historyLifecycleCallback struct{ result chan int32 }

func (c *historyLifecycleCallback) OnError(code int32, _ string) { c.result <- code }
func (c *historyLifecycleCallback) OnSuccess(string)             { c.result <- 0 }

func TestHistoryAfterUninitializationReturnsResourceError(t *testing.T) {
	previous := UserForSDK
	UserForSDK = nil
	defer func() { UserForSDK = previous }()
	for _, tc := range []struct {
		name string
		call func(open_im_sdk_callback.Base, string, string)
	}{
		{"history", GetAdvancedHistoryMessageList},
		{"reverse-history", GetAdvancedHistoryMessageListReverse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callback := &historyLifecycleCallback{result: make(chan int32, 1)}
			tc.call(callback, "after-uninit", `{"conversationID":"si_u_peer","count":10}`)
			select {
			case code := <-callback.result:
				if code != int32(sdkerrs.ErrResourceLoad.Code()) {
					t.Fatalf("got code %d", code)
				}
			case <-time.After(time.Second):
				t.Fatal("callback did not settle")
			}
		})
	}
}
