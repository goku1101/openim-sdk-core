package open_im_sdk

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

type sessionCalls struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func (s *sessionCalls) enter() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, context.Canceled
	}
	s.wg.Add(1)
	return s.wg.Done, nil
}
func (s *sessionCalls) stopAndWait() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.wg.Wait()
}

func captureSDKCall(fn any) (context.Context, func(), error) {
	name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	manager := UserForSDK
	if err := CheckResourceLoad(manager, name); err != nil {
		return nil, nil, sdkerrs.ErrResourceLoad.WrapMsg("not load resource")
	}
	manager.callMu.Lock()
	defer manager.callMu.Unlock()
	ctx := manager.ctx
	// Logout itself owns shutdown; joining it would deadlock. Login establishes resources.
	if strings.HasSuffix(name, ".Login-fm") || strings.HasSuffix(name, ".Logout-fm") || strings.HasSuffix(name, ".Log-fm") {
		return ctx, func() {}, nil
	}
	if manager.calls == nil {
		manager.calls = &sessionCalls{}
	}
	done, err := manager.calls.enter()
	return ctx, done, err
}
