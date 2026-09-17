package group

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/api"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
)

func (g *Group) getFullGroupMemberUserIDs(ctx context.Context, req *group.GetFullGroupMemberUserIDsReq) (*group.GetFullGroupMemberUserIDsResp, error) {
	return api.GetFullGroupMemberUserIDs.Invoke(ctx, req)
}

func (g *Group) getIncrementalJoinGroup(ctx context.Context, req *group.GetIncrementalJoinGroupReq) (*group.GetIncrementalJoinGroupResp, error) {
	return api.GetIncrementalJoinGroup.Invoke(ctx, req)
}

func (g *Group) getFullJoinGroupIDs(ctx context.Context, req *group.GetFullJoinGroupIDsReq) (*group.GetFullJoinGroupIDsResp, error) {
	return api.GetFullJoinedGroupIDs.Invoke(ctx, req)
}

func (g *Group) getIncrementalGroupMemberBatch(ctx context.Context, reqs []*group.GetIncrementalGroupMemberReq) (map[string]*group.GetIncrementalGroupMemberResp, error) {
	req := &group.BatchGetIncrementalGroupMemberReq{UserID: g.loginUserID, ReqList: reqs}
	return api.ExtractField(ctx, api.GetIncrementalGroupMemberBatch.Invoke, req, (*group.BatchGetIncrementalGroupMemberResp).GetRespList)
}

func (g *Group) createGroup(ctx context.Context, req *group.CreateGroupReq) (*group.CreateGroupResp, error) {
	return api.CreateGroup.Invoke(ctx, req)
}

func (g *Group) joinGroup(ctx context.Context, req *group.JoinGroupReq) error {
	return api.JoinGroup.Execute(ctx, req)
}

func (g *Group) quitGroup(ctx context.Context, groupID string) error {
	return api.QuitGroup.Execute(ctx, &group.QuitGroupReq{GroupID: groupID, UserID: g.loginUserID})
}

func (g *Group) dismissGroup(ctx context.Context, groupID string) error {
	return api.DismissGroup.Execute(ctx, &group.DismissGroupReq{GroupID: groupID})
}

func (g *Group) setGroupInfo(ctx context.Context, req *group.SetGroupInfoExReq) error {
	return api.SetGroupInfoEx.Execute(ctx, req)
}

func (g *Group) setGroupMemberInfo(ctx context.Context, req *group.SetGroupMemberInfoReq) error {
	return api.SetGroupMemberInfo.Execute(ctx, req)
}

func (g *Group) kickGroupMember(ctx context.Context, req *group.KickGroupMemberReq) error {
	return api.KickGroupMember.Execute(ctx, req)
}

func (g *Group) transferGroup(ctx context.Context, req *group.TransferGroupOwnerReq) error {
	return api.TransferGroup.Execute(ctx, req)
}

func (g *Group) cancelMuteGroupMember(ctx context.Context, req *group.CancelMuteGroupMemberReq) error {
	return api.CancelMuteGroupMember.Execute(ctx, req)
}

func (g *Group) muteGroupMember(ctx context.Context, req *group.MuteGroupMemberReq) error {
	return api.MuteGroupMember.Execute(ctx, req)
}

func (g *Group) muteGroup(ctx context.Context, groupID string) error {
	return api.MuteGroup.Execute(ctx, &group.MuteGroupReq{GroupID: groupID})
}

func (g *Group) cancelMuteGroup(ctx context.Context, groupID string) error {
	return api.CancelMuteGroup.Execute(ctx, &group.CancelMuteGroupReq{GroupID: groupID})
}

func (g *Group) getDesignatedGroupMembers(ctx context.Context, groupID string, userIDs []string) ([]*sdkws.GroupMemberFullInfo, error) {
	if len(userIDs) == 0 {
		return []*sdkws.GroupMemberFullInfo{}, nil
	}
	ids := append([]string(nil), userIDs...)
	sort.Strings(ids)
	unique := ids[:0]
	for _, id := range ids {
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	key, _ := json.Marshal([]any{groupID, unique})
	g.memberLookupOnce.Do(func() { g.memberLookupSlot = make(chan struct{}, 1) })
	ch := g.memberLookup.DoChan(string(key), func() (any, error) {
		select {
		case g.memberLookupSlot <- struct{}{}:
			defer func() { <-g.memberLookupSlot }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		result := []*sdkws.GroupMemberFullInfo{}
		for start := 0; start < len(unique); start += 100 {
			req := &group.GetGroupMembersInfoReq{GroupID: groupID, UserIDs: unique[start:min(start+100, len(unique))]}
			members, err := api.ExtractField(ctx, api.GetGroupMembersInfo.Invoke, req, (*group.GetGroupMembersInfoResp).GetMembers)
			if err != nil {
				return nil, err
			}
			result = append(result, members...)
		}
		return result, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]*sdkws.GroupMemberFullInfo), nil
	}
}

func (g *Group) getServerSelfGroupApplication(ctx context.Context, groupIDs []string,
	handleResults []int32, pageNumber, showNumber int32) ([]*sdkws.GroupRequest, error) {
	req := &group.GetUserReqApplicationListReq{UserID: g.loginUserID, Pagination: &sdkws.RequestPagination{PageNumber: pageNumber, ShowNumber: showNumber},
		GroupIDs: groupIDs, HandleResults: handleResults}
	if showNumber <= 0 {
		return api.Page(ctx, req, api.GetSendGroupApplicationList.Invoke, (*group.GetUserReqApplicationListResp).GetGroupRequests)
	}
	resp, err := api.GetSendGroupApplicationList.Invoke(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGroupRequests(), nil
}

func (g *Group) getServerJoinGroup(ctx context.Context) ([]*sdkws.GroupInfo, error) {
	req := &group.GetJoinedGroupListReq{FromUserID: g.loginUserID, Pagination: &sdkws.RequestPagination{}}
	return api.Page(ctx, req, api.GetJoinedGroupList.Invoke, (*group.GetJoinedGroupListResp).GetGroups)
}

func (g *Group) getServerAdminGroupApplicationList(ctx context.Context, groupIDs []string,
	handleResults []int32, pageNumber, showNumber int32) ([]*sdkws.GroupRequest, error) {
	req := &group.GetGroupApplicationListReq{FromUserID: g.loginUserID, Pagination: &sdkws.RequestPagination{PageNumber: pageNumber, ShowNumber: showNumber},
		GroupIDs: groupIDs, HandleResults: handleResults}
	if showNumber <= 0 {
		return api.Page(ctx, req, api.GetRecvGroupApplicationList.Invoke, (*group.GetGroupApplicationListResp).GetGroupRequests)
	}
	resp, err := api.GetRecvGroupApplicationList.Invoke(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetGroupRequests(), nil
}

func (g *Group) getGroupsInfoFromServer(ctx context.Context, groupIDs []string) ([]*sdkws.GroupInfo, error) {
	req := &group.GetGroupsInfoReq{GroupIDs: groupIDs}
	return api.ExtractField(ctx, api.GetGroupsInfo.Invoke, req, (*group.GetGroupsInfoResp).GetGroupInfos)
}

func (g *Group) inviteUserToGroup(ctx context.Context, req *group.InviteUserToGroupReq) error {
	return api.InviteUserToGroup.Execute(ctx, req)
}

func (g *Group) handlerGroupApplication(ctx context.Context, req *group.GroupApplicationResponseReq) error {
	return api.AcceptGroupApplication.Execute(ctx, req)
}

func (g *Group) getGroupApplicationUnhandledCount(ctx context.Context, time int64) (int32, error) {
	req := &group.GetGroupApplicationUnhandledCountReq{UserID: g.loginUserID, Time: time}
	resp, err := api.GetGroupApplicationUnhandledCount.Invoke(ctx, req)
	if err != nil {
		return 0, err
	}
	return int32(resp.GetCount()), nil
}
