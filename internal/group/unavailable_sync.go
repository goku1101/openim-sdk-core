package group

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/tools/errs"
	"time"
)

func unavailableGroupError(err error) bool {
	var coded errs.CodeError
	return errors.As(err, &coded) && (coded.Code() == 1002 || coded.Code() == 1204)
}

const (
	memberSuccess            = "success"
	memberInaccessible       = "inaccessible"
	memberDeferred           = "deferred"
	maxMemberDeferredRetries = 3
)

var errGroupInaccessible = errors.New("group is inaccessible")
var errGroupDeferred = errors.New("group member synchronization deferred")

type memberBatchResult struct {
	status  string
	payload *protocol.GetIncrementalGroupMemberResp
}

// A missing payload in a legacy batch is ambiguous. Confirm only that group;
// never turn truncation or an unrecognized new-contract result into a deletion.
func (g *Group) getAccessibleGroupMemberBatch(ctx context.Context, reqs []*protocol.GetIncrementalGroupMemberReq) (map[string]memberBatchResult, error) {
	resp, err := g.getIncrementalGroupMemberBatch(ctx, reqs)
	if err != nil && !unavailableGroupError(err) {
		return nil, err
	}
	result := make(map[string]memberBatchResult, len(reqs))
	for _, req := range reqs {
		if err == nil && resp.GroupResults != nil {
			outcome := resp.GroupResults[req.GroupID]
			if outcome == nil {
				return nil, fmt.Errorf("missing group member result for %s", req.GroupID)
			}
			item := memberBatchResult{status: outcome.Status, payload: resp.RespList[req.GroupID]}
			switch outcome.Status {
			case memberSuccess:
				if item.payload == nil {
					return nil, fmt.Errorf("group member success without payload for %s", req.GroupID)
				}
			case memberInaccessible:
				if outcome.Reason != "left_or_dismissed" {
					return nil, fmt.Errorf("invalid inaccessible group member reason")
				}
			case memberDeferred:
				if outcome.Reason != "batch_change_limit" && outcome.Reason != "not_returned" {
					return nil, fmt.Errorf("invalid deferred group member reason")
				}
			default:
				return nil, fmt.Errorf("invalid group member result status")
			}
			result[req.GroupID] = item
			continue
		}
		if err == nil && resp.RespList[req.GroupID] != nil {
			result[req.GroupID] = memberBatchResult{status: memberSuccess, payload: resp.RespList[req.GroupID]}
			continue
		}
		if len(reqs) == 1 {
			// Only a single-group response can authoritatively disambiguate legacy
			// empty success or permission denial from a batch budget cutoff.
			result[req.GroupID] = memberBatchResult{status: memberInaccessible}
			continue
		}
		one, singleErr := g.getAccessibleGroupMemberBatch(ctx, []*protocol.GetIncrementalGroupMemberReq{req})
		if singleErr != nil {
			return nil, singleErr
		}
		result[req.GroupID] = one[req.GroupID]
	}
	return result, nil
}

func waitMemberDeferred(ctx context.Context, attempt int) error {
	timer := time.NewTimer((250 * time.Millisecond) << attempt)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Single-group notification and partial-member callers use the same classifier.
// A terminal result is returned as a sentinel so no empty version gets applied.
func (g *Group) getGroupMemberChange(ctx context.Context, req *protocol.GetIncrementalGroupMemberReq) (*protocol.GetIncrementalGroupMemberResp, error) {
	for attempt := 0; ; attempt++ {
		results, err := g.getAccessibleGroupMemberBatch(ctx, []*protocol.GetIncrementalGroupMemberReq{req})
		if err != nil {
			return nil, err
		}
		item := results[req.GroupID]
		switch item.status {
		case memberSuccess:
			return item.payload, nil
		case memberInaccessible:
			return nil, errGroupInaccessible
		case memberDeferred:
			if attempt == maxMemberDeferredRetries {
				return nil, errGroupDeferred
			}
			if err := waitMemberDeferred(ctx, attempt); err != nil {
				return nil, err
			}
		}
	}
}

func (g *Group) clearUnavailableGroup(ctx context.Context, id string) error {
	groups, err := g.db.GetJoinedGroupListDB(ctx)
	if err != nil {
		return err
	}
	for _, local := range groups {
		if local.GroupID == id {
			return g.groupSyncer.Sync(ctx, nil, []*model_struct.LocalGroup{local}, nil)
		}
	}
	if err = g.invalidateConversationSyncV2(ctx, id); err != nil {
		return err
	}
	if err = g.db.DeleteGroupAllMembers(ctx, id); err != nil {
		return err
	}
	if err = g.db.DeleteVersionSync(ctx, g.groupAndMemberVersionTableName(), id); err != nil {
		return err
	}
	if err = g.db.DeleteVersionSync(ctx, partialMemberTable, id); err != nil {
		return err
	}
	g.groupInfoCache.Delete(id)
	g.invalidateMemberDirectory(id)
	return nil
}
