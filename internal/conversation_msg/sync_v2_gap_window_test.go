package conversation_msg

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"math"
	"reflect"
	"testing"
)

func TestSyncV2HistoryGapWindowIsBoundedAndPreservesOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		got := getLostSeqListWithLimitLength(1, math.MaxInt64, nil, reverse)
		if len(got) != constant.PullMsgNumForReadDiffusion {
			t.Fatalf("wrong page length: %d", len(got))
		}
		start := int64(math.MaxInt64) - int64(len(got)) + 1
		if reverse {
			start = 1
		}
		for i, seq := range got {
			if seq != start+int64(i) {
				t.Fatalf("wrong ordered window: %v", got)
			}
		}
		if got := getLostSeqListWithLimitLength(1, 5, []int64{1, 3, 5}, reverse); !reflect.DeepEqual(got, []int64{2, 4}) {
			t.Fatalf("known sequences not excluded: %v", got)
		}
		if got := getLostSeqListWithLimitLength(5, 1, nil, reverse); len(got) != 0 {
			t.Fatal("invalid range not empty")
		}
	}
	if got := getLostSeqListWithLimitLength(math.MaxInt64, math.MaxInt64, nil, true); !reflect.DeepEqual(got, []int64{math.MaxInt64}) {
		t.Fatalf("maximum endpoint overflowed: %v", got)
	}
}
