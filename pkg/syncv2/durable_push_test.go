package syncv2

import (
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

func TestDurablePushExcludesTransientEventsWithoutMutatingInput(t *testing.T) {
	push := &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"si_a_b": {Msgs: []*sdkws.MsgData{{Seq: 0}, {Seq: 8}}}}, NotificationMsgs: map[string]*sdkws.PullMsgs{"n_a": {Msgs: []*sdkws.MsgData{{Seq: 0}}}, "n_b": {Msgs: []*sdkws.MsgData{{Seq: 3}, {Seq: 0}}}}}
	stored, err := DurablePush(push)
	if err != nil || len(stored.Msgs["si_a_b"].Msgs) != 1 || stored.Msgs["si_a_b"].Msgs[0].Seq != 8 || len(stored.NotificationMsgs) != 1 || len(stored.NotificationMsgs["n_b"].Msgs) != 1 {
		t.Fatalf("bad replay payload: %v %v", stored, err)
	}
	stored.Msgs["si_a_b"].Msgs[0].Seq = 9
	if len(push.Msgs["si_a_b"].Msgs) != 2 || push.Msgs["si_a_b"].Msgs[1].Seq != 8 || len(push.NotificationMsgs) != 2 {
		t.Fatal("mutated live push")
	}
	push.Msgs["si_a_b"].Msgs = append(push.Msgs["si_a_b"].Msgs, nil)
	if _, err = DurablePush(push); err == nil {
		t.Fatal("invalid event accepted")
	}
}
