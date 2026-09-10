package open_im_sdk

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
)

type syncV2CallbackResult struct {
	code    int32
	message string
}

type syncV2ErrorCallback struct{ result chan syncV2CallbackResult }

func (c *syncV2ErrorCallback) OnError(code int32, message string) {
	c.result <- syncV2CallbackResult{code, message}
}
func (c *syncV2ErrorCallback) OnSuccess(message string) {
	c.result <- syncV2CallbackResult{0, message}
}

func TestSyncV2ErrorsPreserveCodeThroughNativeCallback(t *testing.T) {
	previous := UserForSDK
	UserForSDK = &LoginMgr{ctx: context.Background(), loginStatus: Logged}
	defer func() { UserForSDK = previous }()
	for _, wrapped := range []bool{false, true} {
		for _, code := range []int{7, 9, 10, 500} {
			t.Run(fmt.Sprintf("code_%d_wrapped_%t", code, wrapped), func(t *testing.T) {
				apiError := &syncv2.APIError{ErrCode: code, ErrMsg: "SYNC_V2_TEST_REASON", ErrDlt: "private-detail"}
				var source error = apiError
				if wrapped {
					source = fmt.Errorf("/msg/sync_messages_v2: %w", source)
				}
				callback := &syncV2ErrorCallback{result: make(chan syncV2CallbackResult, 1)}
				call(callback, "qa-sync-v2-callback", func(context.Context) error { return source })
				select {
				case got := <-callback.result:
					if got.code != int32(code) || !strings.Contains(got.message, apiError.ErrMsg) || strings.Contains(got.message, apiError.ErrDlt) {
						t.Fatalf("callback lost code/reason or leaked detail: %+v", got)
					}
				case <-time.After(time.Second):
					t.Fatal("callback did not settle")
				}
			})
		}
	}
}
