package group

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
)

type memberSyncProgressKey struct{}
type memberSyncProgress struct{ done, total int }

func (p *memberSyncProgress) complete(ctx context.Context) {
	p.done++
	syncv2.ReportGroupMemberProgress(ctx, p.done, p.total, nil)
}
