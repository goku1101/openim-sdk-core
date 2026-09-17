package group

import (
	"context"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"net/http"
	"sync"
)

const partialMemberTable = "local_group_member_partial_version_v2"

type partialMemberDB interface {
	CommitGroupMemberPartialV2(context.Context, string, *model_struct.LocalVersionSync, string, uint64, []*model_struct.LocalGroupMember) error
	ResetGroupMemberPartialV2(context.Context, string) error
}
type memberDirectory struct {
	mu              sync.Mutex
	pageMu          sync.Mutex
	client          *syncv2.Client
	endpoint, token string
	caps            *pb.GroupMemberCapabilities
	pages           *lru.Cache[string, *memberPagePosition]
}

func newMemberDirectory() *memberDirectory {
	pages, _ := lru.New[string, *memberPagePosition](1024)
	return &memberDirectory{pages: pages}
}
func unsupportedMemberV2(err error) bool {
	var h *syncv2.HTTPError
	var a *syncv2.APIError
	return errors.As(err, &h) && h.StatusCode == http.StatusNotFound || errors.As(err, &a) && a.ErrCode == 12
}
func (g *Group) memberCapabilitiesLocked(ctx context.Context) (*pb.GroupMemberCapabilities, error) {
	d := g.memberDirectory
	info := ccontext.Info(ctx)
	if d.client == nil || d.endpoint != info.ApiAddr() || d.token != info.Token() {
		d.endpoint, d.token = info.ApiAddr(), info.Token()
		d.client = syncv2.NewClient(d.endpoint, d.token, nil)
		d.caps = nil
		d.pages.Purge()
	}
	if d.caps != nil {
		return d.caps, nil
	}
	caps, err := d.client.GroupMemberCapabilities(ctx, ccontext.Info(ctx).OperationID(), &pb.GroupMemberCapabilitiesRequest{UserID: g.loginUserID})
	if unsupportedMemberV2(err) {
		caps = &pb.GroupMemberCapabilities{}
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if caps.PartialMemberSync && caps.MemberDirectoryV2 && (caps.ProtocolVersion != "group-member-v2.1" || caps.LargeGroupThreshold < 1 || caps.MaxPageSize < 1 || caps.MaxPageSize > 100) {
		return nil, fmt.Errorf("invalid group member capability")
	}
	d.caps = caps
	return caps, nil
}
func (g *Group) invalidateMemberDirectory(groupID string) {
	d := g.memberDirectory
	d.pageMu.Lock()
	defer d.pageMu.Unlock()
	for _, k := range d.pages.Keys() {
		if p, ok := d.pages.Peek(k); ok && p.groupID == groupID {
			d.pages.Remove(k)
		}
	}
	g.groupMemberCache.clearGroup(groupID)
}

// ensurePartialGroup is also used by direct member APIs before startup has
// reached group synchronization. Unsupported capability alone selects legacy.
func (g *Group) ensurePartialGroup(ctx context.Context, groupID string) (bool, error) {
	d := g.memberDirectory
	d.mu.Lock()
	defer d.mu.Unlock()
	caps, err := g.memberCapabilitiesLocked(ctx)
	if err != nil {
		return false, err
	}
	_, readErr := g.db.GetVersionSync(ctx, partialMemberTable, groupID)
	if readErr != nil && !errs.ErrRecordNotFound.Is(readErr) {
		return false, readErr
	}
	supported := caps.PartialMemberSync && caps.MemberDirectoryV2
	if !supported {
		if readErr == nil {
			if err := g.resetPartialLocked(ctx, groupID); err != nil {
				return false, err
			}
			if err := g.syncLegacyGroupAndMember(ctx, groupID); err != nil {
				return false, err
			}
			if err := g.db.DeleteVersionSync(ctx, partialMemberTable, groupID); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if readErr == nil {
		return true, nil
	}
	info, err := g.FetchGroupOrError(ctx, groupID)
	if err != nil {
		return false, err
	}
	if info.MemberCount < caps.LargeGroupThreshold {
		return false, nil
	}
	err = g.establishPartialLocked(ctx, groupID, nil)
	if unsupportedMemberV2(err) {
		d.caps = &pb.GroupMemberCapabilities{}
		if e := g.resetPartialLocked(ctx, groupID); e != nil {
			return false, e
		}
		return false, g.syncLegacyGroupAndMember(ctx, groupID)
	}
	return true, err
}
func (g *Group) resetPartialLocked(ctx context.Context, groupID string) error {
	store, ok := g.db.(partialMemberDB)
	if !ok {
		return fmt.Errorf("partial group storage unavailable")
	}
	if err := store.ResetGroupMemberPartialV2(ctx, groupID); err != nil {
		return err
	}
	g.invalidateMemberDirectory(groupID)
	return nil
}
func (g *Group) establishPartialLocked(ctx context.Context, groupID string, before *model_struct.LocalVersionSync) error {
	d := g.memberDirectory
	page, err := d.client.GroupMemberPage(ctx, ccontext.Info(ctx).OperationID(), &pb.GroupMemberPageRequest{GroupID: groupID, Filter: 1, PageSize: d.caps.MaxPageSize})
	if err != nil {
		return err
	}
	if !page.Live || page.VersionID == "" || page.HasMore || len(page.Members) != 1 || page.Members[0] == nil || page.Members[0].RoleLevel != 100 {
		return fmt.Errorf("invalid group owner baseline")
	}
	keys := []*model_struct.LocalGroupMember{ServerGroupMemberToLocalGroupMember(page.Members[0])}
	if keys[0].UserID != g.loginUserID {
		self, err := g.getDesignatedGroupMembers(ctx, groupID, []string{g.loginUserID})
		if err != nil {
			return err
		}
		if len(self) != 1 || self[0] == nil || self[0].UserID != g.loginUserID {
			return fmt.Errorf("group self membership unconfirmed")
		}
		keys = append(keys, ServerGroupMemberToLocalGroupMember(self[0]))
	}
	store, ok := g.db.(partialMemberDB)
	if !ok {
		return fmt.Errorf("partial group storage unavailable")
	}
	if err = store.CommitGroupMemberPartialV2(ctx, groupID, before, page.VersionID, page.Version, keys); err != nil {
		return err
	}
	g.invalidateMemberDirectory(groupID)
	return nil
}
func (g *Group) syncPartialGroup(ctx context.Context, groupID string) (bool, error) {
	enabled, err := g.ensurePartialGroup(ctx, groupID)
	if err != nil || !enabled {
		return enabled, err
	}
	d := g.memberDirectory
	d.mu.Lock()

	before, err := g.db.GetVersionSync(ctx, partialMemberTable, groupID)
	if err != nil {
		d.mu.Unlock()
		return true, err
	}
	resp, err := g.getAccessibleGroupMemberBatch(ctx, []*protocol.GetIncrementalGroupMemberReq{{GroupID: groupID, VersionID: before.VersionID, Version: before.Version}})
	if err != nil {
		d.mu.Unlock()
		return true, err
	}
	change := resp[groupID]
	if change == nil {
		d.mu.Unlock()
		return true, g.clearUnavailableGroup(ctx, groupID)
	}
	err = g.applyPartialDeltaLocked(ctx, groupID, before, change)
	d.mu.Unlock()
	if unsupportedMemberV2(err) {
		return true, g.rollbackMemberDirectory(ctx, groupID)
	}
	return true, err
}
func (g *Group) applyPartialDeltaLocked(ctx context.Context, groupID string, before *model_struct.LocalVersionSync, change *protocol.GetIncrementalGroupMemberResp) error {
	if change.Full || change.VersionID != before.VersionID {
		return g.establishPartialLocked(ctx, groupID, before)
	}
	if change.Version <= before.Version {
		return nil
	}
	members, err := g.db.GetGroupMemberListByGroupID(ctx, groupID)
	if err != nil {
		return err
	}
	keys := map[string]*model_struct.LocalGroupMember{}
	for _, m := range members {
		keys[m.UserID] = m
	}
	for _, id := range change.Delete {
		delete(keys, id)
	}
	for _, m := range append(append([]*sdkws.GroupMemberFullInfo(nil), change.Insert...), change.Update...) {
		if m == nil || m.GroupID != groupID || m.UserID == "" {
			return fmt.Errorf("invalid group delta member")
		}
		if m.UserID == g.loginUserID || m.RoleLevel == 100 {
			keys[m.UserID] = ServerGroupMemberToLocalGroupMember(m)
		} else {
			delete(keys, m.UserID)
		}
	}
	list := make([]*model_struct.LocalGroupMember, 0, 2)
	for _, m := range keys {
		list = append(list, m)
	}
	store, ok := g.db.(partialMemberDB)
	if !ok {
		return fmt.Errorf("partial group storage unavailable")
	}
	if change.Group != nil {
		if change.Group.GroupID != groupID {
			return fmt.Errorf("invalid delta group")
		}
		if err := g.db.UpdateGroup(ctx, ServerGroupToLocalGroup(change.Group)); err != nil {
			return err
		}
	}
	if err := store.CommitGroupMemberPartialV2(ctx, groupID, before, change.VersionID, change.Version, list); err != nil {
		return err
	}
	g.invalidateMemberDirectory(groupID)
	if change.Group != nil {
		g.groupInfoCache.Store(groupID, ServerGroupToLocalGroup(change.Group))
	}
	if g.listener != nil {
		for _, id := range change.Delete {
			g.listener().OnGroupMemberDeleted(utils.StructToJsonString(&model_struct.LocalGroupMember{GroupID: groupID, UserID: id}))
		}
		for _, m := range change.Insert {
			g.listener().OnGroupMemberAdded(utils.StructToJsonString(ServerGroupMemberToLocalGroupMember(m)))
		}
		for _, m := range change.Update {
			g.listener().OnGroupMemberInfoChanged(utils.StructToJsonString(ServerGroupMemberToLocalGroupMember(m)))
		}
	}
	return nil
}
