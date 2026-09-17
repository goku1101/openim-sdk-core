//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"testing"
	"time"
)

func TestAppVersionWaitsForSyncWriter(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	d.mRWMutex.Lock()
	done := make(chan error, 1)
	go func() {
		done <- d.SetAppSDKVersion(ctx, &model_struct.LocalAppSDKVersion{Version: "test", Installed: true})
	}()
	select {
	case err := <-done:
		d.mRWMutex.Unlock()
		t.Fatalf("version write bypassed active sync writer: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	d.mRWMutex.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("version write did not resume")
	}
	v, err := d.GetAppSDKVersion(ctx)
	if err != nil || !v.Installed {
		t.Fatalf("version not committed: %+v %v", v, err)
	}
}
