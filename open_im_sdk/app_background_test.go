package open_im_sdk

import (
	"errors"
	"sync"
	"testing"
)

func TestAppBackgroundSyncOnlyWakesOnForegroundTransition(t *testing.T) {
	var state appBackgroundSync
	notifications, wakes := 0, 0
	notify := func() error { notifications++; return nil }
	wake := func() error { wakes++; return nil }
	for _, background := range []bool{false, false, true, true, false, false} {
		if err := state.update(background, notify, wake); err != nil {
			t.Fatal(err)
		}
	}
	if notifications != 6 || wakes != 1 {
		t.Fatalf("notifications=%d wakes=%d", notifications, wakes)
	}
}

func TestAppBackgroundSyncRetainsFailedForegroundCatchup(t *testing.T) {
	for _, fail := range []string{"notify", "wake"} {
		t.Run(fail, func(t *testing.T) {
			var state appBackgroundSync
			ok := func() error { return nil }
			sentinel := errors.New("temporarily unavailable")
			if err := state.update(true, ok, ok); err != nil {
				t.Fatal(err)
			}
			notify, wake := ok, ok
			if fail == "notify" {
				notify = func() error { return sentinel }
			} else {
				wake = func() error { return sentinel }
			}
			if err := state.update(false, notify, wake); !errors.Is(err, sentinel) {
				t.Fatalf("got %v", err)
			}
			wakes := 0
			wake = func() error { wakes++; return nil }
			if err := state.update(false, ok, wake); err != nil {
				t.Fatal(err)
			}
			if err := state.update(false, ok, wake); err != nil {
				t.Fatal(err)
			}
			if wakes != 1 {
				t.Fatalf("retried wake %d times", wakes)
			}
		})
	}
}

func TestAppBackgroundSyncSerializesDuplicateForegroundReports(t *testing.T) {
	var state appBackgroundSync
	ok := func() error { return nil }
	if err := state.update(true, ok, ok); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	wakes := 0
	for i := 0; i < 30; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := state.update(false, ok, func() error { wakes++; return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if wakes != 1 {
		t.Fatalf("wakes=%d", wakes)
	}
}

func TestAppBackgroundSyncNewSessionStartsInForeground(t *testing.T) {
	var state appBackgroundSync
	ok := func() error { return nil }
	if err := state.update(true, ok, ok); err != nil {
		t.Fatal(err)
	}
	state.reset()
	wakes := 0
	if err := state.update(false, ok, func() error { wakes++; return nil }); err != nil {
		t.Fatal(err)
	}
	if wakes != 0 {
		t.Fatalf("fresh login woke %d times", wakes)
	}
}
