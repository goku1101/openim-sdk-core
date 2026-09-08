package syncv2

import (
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// A success envelope is not evidence that omitted sequences were deleted.
func validatePullOutcomes(seqs []int64, result *pb.PullResponse) error {
	outstanding := make(map[int64]bool, len(seqs))
	for _, seq := range seqs {
		outstanding[seq] = true
	}
	for _, message := range result.Messages {
		if message == nil || message.ClientMsgID == "" || !outstanding[message.Seq] {
			return fmt.Errorf("sync v2: unexpected message sequence")
		}
		delete(outstanding, message.Seq)
	}
	for _, seq := range result.UnresolvedSeqs {
		if !outstanding[seq] {
			return fmt.Errorf("sync v2: unexpected unresolved sequence")
		}
		delete(outstanding, seq)
	}
	if len(outstanding) != 0 {
		return fmt.Errorf("sync v2: missing pull outcomes")
	}

	return nil
}
