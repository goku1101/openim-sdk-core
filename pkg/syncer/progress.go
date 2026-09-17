package syncer

import "context"

// FullSyncWithProgress reports successfully persisted rows, including the
// existing single-row fallback. The copy leaves shared Syncer callbacks intact.
func (s *Syncer[T, RESP, V]) FullSyncWithProgress(ctx context.Context, entityID string, report func(int)) error {
	copy := *s
	count := 0
	copy.batchInsert = func(ctx context.Context, rows []T) error {
		if err := s.batchInsert(ctx, rows); err != nil {
			return err
		}
		count += len(rows)
		report(count)
		return nil
	}
	copy.insert = func(ctx context.Context, row T) error {
		if err := s.insert(ctx, row); err != nil {
			return err
		}
		count++
		report(count)
		return nil
	}
	report(0)
	return copy.FullSync(ctx, entityID)
}
