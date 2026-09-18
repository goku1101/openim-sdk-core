package open_im_sdk

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"reflect"
	"sync"
	"testing"
)

type recordingConnectionListener struct {
	mu    sync.Mutex
	calls []string
}

func (l *recordingConnectionListener) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, s)
}
func (l *recordingConnectionListener) OnConnecting()     { l.add("connecting") }
func (l *recordingConnectionListener) OnConnectSuccess() { l.add("connected") }
func (l *recordingConnectionListener) OnConnectFailed(code int32, msg string) {
	l.add(fmt.Sprintf("failed:%d:%s", code, msg))
}
func (l *recordingConnectionListener) OnKickedOffline()              { l.add("kicked") }
func (l *recordingConnectionListener) OnUserTokenExpired()           { l.add("expired") }
func (l *recordingConnectionListener) OnUserTokenInvalid(msg string) { l.add("invalid:" + msg) }

func TestRepeatedInitRebindsRetainedConnectionListener(t *testing.T) {
	oldListener, newListener := &recordingConnectionListener{}, &recordingConnectionListener{}
	mgr := &LoginMgr{}
	if !mgr.InitSDK(sdk_struct.IMConfig{PlatformID: 1}, oldListener) {
		t.Fatal("init failed")
	}
	defer mgr.cancel()
	previous := UserForSDK
	UserForSDK = mgr
	defer func() { UserForSDK = previous }()
	retained := mgr.connListener
	if !InitSDK(newListener, "listener-rebind-test", `{"platformID":1}`) {
		t.Fatal("rebind failed")
	}
	retained.OnConnecting()
	retained.OnConnectSuccess()
	retained.OnConnectFailed(7, "reason")
	retained.OnKickedOffline()
	retained.OnUserTokenExpired()
	retained.OnUserTokenInvalid("token-reason")
	if len(oldListener.calls) != 0 || len(newListener.calls) != 6 {
		t.Fatalf("callbacks delivered to old=%d new=%d; expected old=0 new=6", len(oldListener.calls), len(newListener.calls))
	}
	if UserForSDK != mgr || mgr.connListener != retained {
		t.Fatal("rebinding replaced the existing SDK or listener handle")
	}
}

func TestConnectionListenerRebindPreservesCallbackArguments(t *testing.T) {
	listener := &recordingConnectionListener{}
	proxy := &connectionListenerProxy{listener: listener}
	proxy.OnConnectFailed(7, "reason")
	proxy.OnUserTokenInvalid("invalid-reason")
	if !reflect.DeepEqual(listener.calls, []string{"failed:7:reason", "invalid:invalid-reason"}) {
		t.Fatal(listener.calls)
	}
}

func TestRepeatedInitRejectsNilWithoutDroppingListener(t *testing.T) {
	listener := &recordingConnectionListener{}
	previous := UserForSDK
	UserForSDK = &LoginMgr{connListener: &connectionListenerProxy{listener: listener}}
	defer func() { UserForSDK = previous }()
	if InitSDK(nil, "nil-listener", "") {
		t.Fatal("accepted nil listener")
	}
	UserForSDK.connListener.OnConnecting()
	if len(listener.calls) != 1 {
		t.Fatal("lost existing listener")
	}
}

func TestConnectionListenerConcurrentRebind(t *testing.T) {
	first, second := &recordingConnectionListener{}, &recordingConnectionListener{}
	proxy := &connectionListenerProxy{listener: first}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			proxy.replace(second)
			proxy.replace(first)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			proxy.OnConnectSuccess()
		}
	}()
	wg.Wait()
	if len(first.calls)+len(second.calls) != 1000 {
		t.Fatal("lost callbacks")
	}
}

func TestCancelledSessionDoesNotDeliverToReboundConnectionListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	oldListener, newListener := &recordingConnectionListener{}, &recordingConnectionListener{}
	bridge := &connectionListenerProxy{listener: oldListener}
	session := &connectionListenerProxy{ctx: ctx, listener: bridge}
	session.OnConnectSuccess()
	cancel()
	bridge.replace(newListener)
	session.OnKickedOffline()
	session.OnConnectSuccess()
	session.OnUserTokenExpired()
	if len(oldListener.calls) != 1 || len(newListener.calls) != 0 {
		t.Fatal("stale connection callbacks escaped session")
	}
}

func TestExitWaitsForSessionWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mgr := &LoginMgr{ctx: ctx, cancel: cancel}
	mgr.syncWorkers.Add(1)
	stopped := make(chan struct{})
	go func() { mgr.Exit(); close(stopped) }()
	<-ctx.Done()
	select {
	case <-stopped:
		t.Fatal("exit returned with active database worker")
	default:
	}
	mgr.syncWorkers.Done()
	<-stopped
}
