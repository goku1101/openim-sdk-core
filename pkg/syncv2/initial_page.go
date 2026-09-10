package syncv2

import (
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// InitialPageIDs chooses a bounded preview from authoritative selected states.
// Local drafts affect order only, never membership or permissions.
func InitialPageIDs(states []*pb.State, draftTimes map[string]int64, limit int) []string {
	if limit <= 0 {
		return nil
	}
	candidates := make([]*pb.State, 0, len(states))
	seen := make(map[string]bool, len(states))
	for _, state := range states {
		if state == nil || !state.Known || state.Conversation == nil || state.ConversationID == "" || seen[state.ConversationID] {
			continue
		}
		seen[state.ConversationID] = true
		candidates = append(candidates, state)
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.IsPinned != b.IsPinned {
			return a.IsPinned
		}
		at, bt := max(a.LastMessageTime, draftTimes[a.ConversationID]), max(b.LastMessageTime, draftTimes[b.ConversationID])
		if at != bt {
			return at > bt
		}
		return a.ConversationID < b.ConversationID
	})
	ids := make([]string, 0, min(limit, len(candidates)))
	for _, state := range candidates[:min(limit, len(candidates))] {
		ids = append(ids, state.ConversationID)
	}
	return ids
}
