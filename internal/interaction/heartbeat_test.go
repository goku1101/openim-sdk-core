package interaction

import (
	"testing"
	"time"
)

func TestHeartbeatAllowsTwoPingPeriods(t *testing.T) {
	if writeWait != 10*time.Second || pingPeriod != 24*time.Second || pongWait != 60*time.Second || pongWait < 2*pingPeriod {
		t.Fatal("heartbeat contract violated")
	}
}
