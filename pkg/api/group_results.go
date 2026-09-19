package api

import "github.com/openimsdk/protocol/group"

// GroupMemberBatchResponse retains the existing payload while decoding the
// optional per-group outcomes introduced by the server contract.
type GroupMemberBatchResponse struct {
	RespList     map[string]*group.GetIncrementalGroupMemberResp `json:"respList"`
	GroupResults map[string]*GroupMemberResult                   `json:"groupResults"`
}

type GroupMemberResult struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}
