package open_im_sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/db_interface"
)

type cancelledLoginDB struct {
	db_interface.DataBase
	closed bool
}

func (d *cancelledLoginDB) Close(context.Context) error { d.closed = true; return nil }

func TestUnInitWhileDatabaseOpeningCancelsLateLogin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := &LoginMgr{ctx: ctx, cancel: cancel, info: &ccontext.GlobalConfig{}}
	entered, release := make(chan struct{}), make(chan struct{})
	database := &cancelledLoginDB{}
	result := make(chan error, 1)
	go func() {
		result <- mgr.loginWithDatabase(ctx, "user", "", func(context.Context, string, string, int) (db_interface.DataBase, error) {
			close(entered)
			<-release
			return database, nil
		})
	}()
	<-entered
	done := make(chan struct{})
	go func() { mgr.UnInitSDK(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("uninitialization waited for database open")
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("late login: %v", err)
	}
	if !database.closed {
		t.Fatal("late database was not closed")
	}
	if mgr.db != nil || mgr.conversation != nil || mgr.getLoginStatus(ctx) == Logged {
		t.Fatal("cancelled login published session resources")
	}
}
