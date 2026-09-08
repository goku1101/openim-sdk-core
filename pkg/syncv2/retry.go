package syncv2

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

type RetryStore interface {
	NextConversationSyncV2Retry(context.Context) (*model_struct.LocalConversationSyncV2Retry, error)
	AcknowledgeConversationSyncV2Retry(context.Context, *model_struct.LocalConversationSyncV2Retry) error
	RescheduleConversationSyncV2Retry(context.Context, *model_struct.LocalConversationSyncV2Retry, int64) error
}

func retryDelay(attempts uint32) time.Duration {
	if attempts >= 9 {
		return 5 * time.Minute
	}
	return min(time.Second<<attempts, 5*time.Minute)
}

// RunRetryTasks is a single, cancelable consumer. The owner wakes it after
// persisting work and waits for it to return before closing the account DB.
// Unknown holes back off independently of UI readiness; permanent errors pause
// their task until a new login explicitly resumes it. Unavailable reads stop.
func RunRetryTasks(ctx context.Context, store RetryStore, wake <-chan struct{}, run func(context.Context, *model_struct.LocalConversationSyncV2Retry) error) error {
	var storageFailures uint32
	wait := func(delay time.Duration) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			return nil
		case <-timer.C:
			return nil
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, err := store.NextConversationSyncV2Retry(ctx)
		if err != nil {
			if err = wait(retryDelay(storageFailures)); err != nil {
				return err
			}
			if storageFailures < 9 {
				storageFailures++
			}
			continue
		}
		if job == nil || job.DueAt == math.MaxInt64 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wake:
				continue
			}
		}
		// Limit the conversion before multiplying milliseconds into Duration.
		// A newly enqueued earlier task interrupts this wait via wake.
		if remaining := job.DueAt - time.Now().UnixMilli(); remaining > 0 {
			if err = wait(time.Duration(min(remaining, int64(time.Hour/time.Millisecond))) * time.Millisecond); err != nil {
				return err
			}
			continue
		}
		err = run(ctx, job)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		action := RecoveryFor(err)
		if err == nil || action == Unavailable {
			err = store.AcknowledgeConversationSyncV2Retry(ctx, job)
		} else {
			due := int64(math.MaxInt64)
			if errors.Is(err, ErrStatePending) || action == Retry || action == RefreshState || action == RebuildSnapshot || action == Renegotiate {
				due = time.Now().Add(retryDelay(job.Attempts)).UnixMilli()
			}
			err = store.RescheduleConversationSyncV2Retry(ctx, job, due)
		}
		if err != nil {
			// Failed acknowledgements remain durable, but must not spin the
			// same network request while storage is unavailable.
			if err = wait(retryDelay(storageFailures)); err != nil {
				return err
			}
			if storageFailures < 9 {
				storageFailures++
			}
		} else {
			storageFailures = 0
		}
	}
}
