//go:build !js

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataBase) PutConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
	if job == nil || job.TaskID == "" || job.DueAt < 0 || !json.Valid([]byte(job.Payload)) || job.Kind != "push" && job.Kind != "reconcile" && job.Kind != "synchronize" {
		return fmt.Errorf("sync v2: invalid retry task")
	}
	row := *job
	row.Revision = 1
	row.Attempts = 0
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "task_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"kind": row.Kind, "payload": row.Payload, "due_at": row.DueAt,
			"revision": gorm.Expr("revision + 1"), "attempts": 0,
		}),
	}).Create(&row).Error
}

// Return the earliest task, including its deadline, so one worker can sleep
// until it is due. An empty queue is not a database failure.
func (d *DataBase) NextConversationSyncV2Retry(ctx context.Context) (*model_struct.LocalConversationSyncV2Retry, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var row model_struct.LocalConversationSyncV2Retry
	err := d.conn.WithContext(ctx).Order("due_at ASC").Order("task_id ASC").Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (d *DataBase) AcknowledgeConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
	if job == nil || job.TaskID == "" || job.Revision == 0 {
		return fmt.Errorf("sync v2: invalid retry acknowledgement")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Where("task_id = ? AND revision = ?", job.TaskID, job.Revision).Delete(&model_struct.LocalConversationSyncV2Retry{}).Error
}

func (d *DataBase) RescheduleConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry, dueAt int64) error {
	if job == nil || job.TaskID == "" || job.Revision == 0 || dueAt < 0 {
		return fmt.Errorf("sync v2: invalid retry deadline")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Model(&model_struct.LocalConversationSyncV2Retry{}).
		Where("task_id = ? AND revision = ?", job.TaskID, job.Revision).
		Updates(map[string]interface{}{"due_at": dueAt, "attempts": gorm.Expr("attempts + 1"), "revision": gorm.Expr("revision + 1")}).Error
}

// A new authenticated session may retry tasks paused for transport/auth errors.
// Ordinary wakeups do not reset deadlines or retry unavailable conversations.
func (d *DataBase) ResumeConversationSyncV2Retries(ctx context.Context, now int64) error {
	if now < 0 {
		return fmt.Errorf("sync v2: invalid retry resume time")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Model(&model_struct.LocalConversationSyncV2Retry{}).
		Where("due_at = ?", int64(math.MaxInt64)).
		Updates(map[string]interface{}{"due_at": now, "attempts": 0, "revision": gorm.Expr("revision + 1")}).Error
}

// The owning attempt can remove terminal streams without reviving a replaced job.
func (d *DataBase) UpdateConversationSyncV2RetryPayload(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry, payload string) error {
	if job == nil || job.TaskID == "" || job.Revision == 0 || !json.Valid([]byte(payload)) {
		return fmt.Errorf("sync v2: invalid retry payload update")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	result := d.conn.WithContext(ctx).Model(&model_struct.LocalConversationSyncV2Retry{}).Where("task_id = ? AND revision = ?", job.TaskID, job.Revision).Updates(map[string]interface{}{"payload": payload, "revision": gorm.Expr("revision + 1")})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("sync v2: retry payload superseded")
	}
	job.Revision++
	job.Payload = payload
	return nil
}
