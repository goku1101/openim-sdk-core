package group

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/network"
	"time"
)

// Missing display profiles are optional. Serialize the shared lookup and recheck
// its cache after admission so overlapping batches reuse completed results.
// No goroutine is created per sender; cancellation stops queued callers.
func (g *Group) GetMissingDisplayProfiles(ctx context.Context, groupID string, ids []string) (map[string]*model_struct.LocalGroupMember, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case g.displayQuerySlot <- struct{}{}:
		defer func() { <-g.displayQuerySlot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	result, err := g.GetLocalGroupMemberProfiles(ctx, groupID, ids)
	if err != nil {
		return nil, err
	}
	if time.Now().Before(g.displayRetryAt) {
		return result, nil
	}
	missing := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] || result[id] != nil {
			continue
		}
		seen[id] = true
		key := g.buildGroupMemberKey(groupID, id)
		if until, ok := g.displayMisses.Get(key); ok && time.Now().Before(until) {
			continue
		}
		missing = append(missing, id)
	}
	for start := 0; start < len(missing); start += 100 {
		batch := missing[start:min(start+100, len(missing))]
		members, err := g.getDesignatedGroupMembers(ctx, groupID, batch)
		// Negative or transient results are deduplicated briefly, never persisted as
		// membership absence. The next ordinary message may retry after this window.
		for _, id := range batch {
			g.displayMisses.Add(g.buildGroupMemberKey(groupID, id), time.Now().Add(10*time.Second))
		}
		if err != nil {
			var admission *network.RetryAfterError
			if errors.As(err, &admission) {
				// One cooldown for this account's optional profile endpoint:
				// a different sender/group must not bypass server admission.
				g.displayRetryAt = time.Now().Add(10 * time.Second)
				if admission.RetryAt.After(g.displayRetryAt) {
					g.displayRetryAt = admission.RetryAt
				}
			}
			return result, err
		}
		for _, member := range members {
			if member == nil || !seen[member.UserID] {
				continue
			}
			local := ServerGroupMemberToLocalGroupMember(member)
			result[local.UserID] = local
			g.groupMemberCache.Store(g.buildGroupMemberKey(groupID, local.UserID), local)
			g.displayMisses.Remove(g.buildGroupMemberKey(groupID, local.UserID))
		}
	}
	return result, nil
}
