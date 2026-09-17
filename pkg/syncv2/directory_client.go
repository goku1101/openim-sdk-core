package syncv2

import (
	"context"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// These transport methods do not select a synchronization mode. Callers must
// negotiate the corresponding capability before starting a round.
func (c *Client) Scan(ctx context.Context, op string, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	resp := new(pb.ScanResponse)
	_, err := c.invoke(ctx, op, "/conversation/sync_scan", req, resp)
	return resp, err
}

func (c *Client) Changes(ctx context.Context, op string, req *pb.ChangeRequest) (*pb.ChangeResponse, error) {
	resp := new(pb.ChangeResponse)
	_, err := c.invoke(ctx, op, "/conversation/sync_changes_v2", req, resp)
	return resp, err
}

func (c *Client) GroupMemberCapabilities(ctx context.Context, op string, req *pb.GroupMemberCapabilitiesRequest) (*pb.GroupMemberCapabilities, error) {
	resp := new(pb.GroupMemberCapabilities)
	_, err := c.invoke(ctx, op, "/group/sync_capabilities", req, resp)
	return resp, err
}

func (c *Client) GroupMemberPage(ctx context.Context, op string, req *pb.GroupMemberPageRequest) (*pb.GroupMemberPageResponse, error) {
	resp := new(pb.GroupMemberPageResponse)
	_, err := c.invoke(ctx, op, "/group/get_group_member_page_v2", req, resp)
	return resp, err
}
