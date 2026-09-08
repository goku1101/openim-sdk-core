package sdkerrs

import "errors"

// Local invalidation is a retryable conflict, not a server business error or
// evidence that V2 is unsupported. The caller must read state again.
var ErrSyncV2StateChanged = errors.New("sync v2: local state changed")
