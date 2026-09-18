package open_im_sdk

import (
	"context"
	"testing"
	"time"
)

func TestExitJoinsPublicCallAndRejectsLateAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mgr := &LoginMgr{ctx: ctx, cancel: cancel, calls: &sessionCalls{}, loginStatus: Logged}
	previous := UserForSDK
	UserForSDK = mgr
	defer func() { UserForSDK = previous }()
	captured, done, err := captureSDKCall(func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { mgr.Exit(); close(exited) }()
	<-captured.Done()
	select {
	case <-exited:
		t.Fatal("closed while API owns database")
	case <-time.After(20 * time.Millisecond):
	}
	if _, _, err = captureSDKCall(func(context.Context) error { return nil }); err == nil {
		t.Fatal("admitted API during shutdown")
	}
	done()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("exit did not finish")
	}
}
