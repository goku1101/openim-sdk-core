package group

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/tools/errs"
)

func unavailableGroupError(err error) bool {
	var coded errs.CodeError
	return errors.As(err, &coded) && (coded.Code() == 1002 || coded.Code() == 1204)
}

// An upgraded server omits inaccessible groups. Older servers reject the batch;
// retry each requested group once, preserving all errors other than denial.
func (g *Group) getAccessibleGroupMemberBatch(ctx context.Context, reqs []*protocol.GetIncrementalGroupMemberReq) (map[string]*protocol.GetIncrementalGroupMemberResp, error) {
	resp, err := g.getIncrementalGroupMemberBatch(ctx, reqs)
	if err == nil || !unavailableGroupError(err) {
		return resp, err
	}
	result := make(map[string]*protocol.GetIncrementalGroupMemberResp)
	for _, req := range reqs {
		one, err := g.getIncrementalGroupMemberBatch(ctx, []*protocol.GetIncrementalGroupMemberReq{req})
		if unavailableGroupError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if value := one[req.GroupID]; value != nil {
			result[req.GroupID] = value
		}
	}
	return result, nil
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
