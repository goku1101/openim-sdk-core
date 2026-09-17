package group

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdk_params_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
	"sort"
)

type memberPagePosition struct {
	groupID, cursor string
	offset          int32
	end             bool
}

func (g *Group) rollbackMemberDirectory(ctx context.Context, groupID string) error {
	d := g.memberDirectory
	d.mu.Lock()
	defer d.mu.Unlock()
	d.caps = &pb.GroupMemberCapabilities{}
	err := g.resetPartialLocked(ctx, groupID)
	if err != nil {
		return err
	}
	if err := g.syncLegacyGroupAndMember(ctx, groupID); err != nil {
		return err
	}
	return g.db.DeleteVersionSync(ctx, partialMemberTable, groupID)
}
func (g *Group) partialMemberPage(ctx context.Context, req *pb.GroupMemberPageRequest, offset, count int32) ([]*model_struct.LocalGroupMember, bool, error) {
	enabled, err := g.ensurePartialGroup(ctx, req.GroupID)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	if offset < 0 || count == 0 || count < -1 {
		return nil, true, fmt.Errorf("invalid member pagination")
	}
	result, err := g.readMemberDirectory(ctx, req, offset, count)
	if unsupportedMemberV2(err) {
		if e := g.rollbackMemberDirectory(ctx, req.GroupID); e != nil {
			return nil, true, e
		}
		return nil, false, nil
	}
	return result, true, err
}
func (g *Group) readMemberDirectory(ctx context.Context, request *pb.GroupMemberPageRequest, offset, count int32) ([]*model_struct.LocalGroupMember, error) {
	d := g.memberDirectory
	// Keep mode stable during paging, then use a single bounded request stream.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pageMu.Lock()
	defer d.pageMu.Unlock()
	req := proto.Clone(request).(*pb.GroupMemberPageRequest)
	sort.Strings(req.ExcludeUserIDs)
	raw, _ := json.Marshal(req)
	query := string(raw)
	prefix := func(n int32) string { return fmt.Sprintf("%s:%d", query, n) }
	position := int32(0)
	cursor := ""
	if p, ok := d.pages.Get(prefix(offset)); ok {
		if p.end {
			return []*model_struct.LocalGroupMember{}, nil
		}
		position, cursor = p.offset, p.cursor
	}
	result := []*model_struct.LocalGroupMember{}
	seen := map[string]bool{}
	restarted := false
	for count < 0 || int32(len(result)) < count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		req.Cursor = cursor
		req.PageSize = d.caps.MaxPageSize
		if position < offset {
			req.PageSize = min(req.PageSize, offset-position)
		} else if count > 0 {
			req.PageSize = min(req.PageSize, count-int32(len(result)))
		}
		page, err := d.client.GroupMemberPage(ctx, ccontext.Info(ctx).OperationID(), req)
		if err != nil {
			var e *syncv2.APIError
			if !restarted && errors.As(err, &e) && e.ErrCode == 9 && e.ErrMsg == "SYNC_V2_GROUP_MEMBER_CURSOR_EXPIRED" {
				d.pages.Purge()
				position, cursor = 0, ""
				result = result[:0]
				seen = map[string]bool{}
				restarted = true
				continue
			}
			return nil, err
		}
		if !page.Live || page.VersionID == "" || len(page.Members) > int(req.PageSize) || page.HasMore && (page.NextCursor == "" || page.NextCursor == cursor) {
			return nil, fmt.Errorf("invalid live member page")
		}
		for _, member := range page.Members {
			if member == nil || member.UserID == "" || member.GroupID != req.GroupID {
				return nil, fmt.Errorf("invalid member directory row")
			}
			local := ServerGroupMemberToLocalGroupMember(member)
			g.groupMemberCache.Store(g.buildGroupMemberKey(req.GroupID, member.UserID), local)
			if position >= offset && !seen[member.UserID] {
				result = append(result, local)
				seen[member.UserID] = true
			}
			position++
		}
		cursor = page.NextCursor
		nextOffset := position
		if position >= offset {
			nextOffset = offset + int32(len(result))
		}
		d.pages.Add(prefix(nextOffset), &memberPagePosition{groupID: req.GroupID, offset: nextOffset, cursor: cursor, end: !page.HasMore})
		if !page.HasMore {
			break
		}
	}
	return result, nil
}
func (g *Group) partialMemberPointLookup(ctx context.Context, groupID string, ids []string) ([]*model_struct.LocalGroupMember, bool, error) {
	if len(ids) == 0 {
		return []*model_struct.LocalGroupMember{}, true, nil
	}
	enabled, err := g.ensurePartialGroup(ctx, groupID)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	// Membership-sensitive APIs always query the authoritative endpoint. Cached
	// absence, or an old cached role, is never used as permission evidence.
	result := []*model_struct.LocalGroupMember{}
	unique := []string{}
	seen := map[string]bool{}
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	for start := 0; start < len(unique); start += 100 {
		members, err := g.getDesignatedGroupMembers(ctx, groupID, unique[start:min(start+100, len(unique))])
		if err != nil {
			return nil, true, err
		}
		for _, m := range members {
			local := ServerGroupMemberToLocalGroupMember(m)
			result = append(result, local)
			g.groupMemberCache.Store(g.buildGroupMemberKey(groupID, m.UserID), local)
		}
	}
	return result, true, nil
}

func (g *Group) searchPartialMembers(ctx context.Context, p *sdk_params_callback.SearchGroupMembersParam) ([]*model_struct.LocalGroupMember, bool, error) {
	enabled, err := g.ensurePartialGroup(ctx, p.GroupID)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	if len(p.KeywordList) == 0 || p.Offset < 0 || p.Count < 1 || p.Offset > 2147483647 || p.Count > 2147483647 {
		return nil, true, fmt.Errorf("invalid member search")
	}
	keyword := p.KeywordList[0]
	result := []*model_struct.LocalGroupMember{}
	req := &pb.GroupMemberPageRequest{GroupID: p.GroupID, Keyword: keyword}
	offset, count := int32(p.Offset), int32(p.Count)
	if p.IsSearchUserID {
		exact, _, err := g.partialMemberPointLookup(ctx, p.GroupID, []string{keyword})
		if err != nil {
			return nil, true, err
		}
		if len(exact) > 0 {
			req.ExcludeUserIDs = []string{keyword}
			if offset > 0 {
				offset--
			} else {
				result = append(result, exact[0])
				count--
			}
		}
	}
	if p.IsSearchMemberNickname && count > 0 {
		page, handled, err := g.partialMemberPage(ctx, req, offset, count)
		if err != nil || !handled {
			return nil, handled, err
		}
		result = append(result, page...)
	}
	return result, true, nil
}
