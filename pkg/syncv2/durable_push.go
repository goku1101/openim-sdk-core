package syncv2

import (
	"fmt"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

// DurablePush copies only sequenced events. Typing and other sequence-zero
// notifications must never be replayed after a process or account restart.
func DurablePush(push *sdkws.PushMessages) (*sdkws.PushMessages, error) {
	if push == nil {
		return nil, fmt.Errorf("sync v2: missing push")
	}
	// Validate before protobuf cloning, which normalizes nil repeated messages.
	for _, batches := range []map[string]*sdkws.PullMsgs{push.Msgs, push.NotificationMsgs} {
		for id, batch := range batches {
			if id == "" || batch == nil {
				return nil, fmt.Errorf("sync v2: invalid pushed stream")
			}
			for _, message := range batch.Msgs {
				if message == nil || message.Seq < 0 {
					return nil, fmt.Errorf("sync v2: invalid pushed message")
				}
			}
		}
	}
	copy := proto.Clone(push).(*sdkws.PushMessages)
	for _, batches := range []map[string]*sdkws.PullMsgs{copy.Msgs, copy.NotificationMsgs} {
		for id, batch := range batches {
			retained := batch.Msgs[:0]
			for _, message := range batch.Msgs {
				if message.Seq > 0 {
					retained = append(retained, message)
				}
			}
			batch.Msgs = retained
			if len(retained) == 0 {
				delete(batches, id)
			}
		}
	}
	return copy, nil
}
