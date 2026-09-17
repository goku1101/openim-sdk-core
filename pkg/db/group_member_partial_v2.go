//go:build !js

package db

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"gorm.io/gorm"
	"strconv"
)

// CommitGroupMemberPartialV2 atomically replaces the bounded authoritative
// member subset and its independent version, never a full UIDList.
func (d *DataBase) CommitGroupMemberPartialV2(ctx context.Context, groupID string, before *model_struct.LocalVersionSync, versionID string, version uint64, members []*model_struct.LocalGroupMember) error {
	if groupID == "" || versionID == "" || len(members) > 2 {
		return fmt.Errorf("invalid partial group baseline")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		const partial = "local_group_member_partial_version_v2"
		var current model_struct.LocalVersionSync
		err := tx.Where("table_name = ? AND entity_id = ?", partial, groupID).Take(&current).Error
		if before == nil {
			if err != gorm.ErrRecordNotFound {
				if err != nil {
					return err
				}
				return fmt.Errorf("partial group baseline already exists")
			}
		} else if err != nil || current.VersionID != before.VersionID || current.Version != before.Version {
			return fmt.Errorf("partial group version changed")
		}
		if err := tx.Where("group_id = ?", groupID).Delete(&model_struct.LocalGroupMember{}).Error; err != nil {
			return err
		}
		for _, member := range members {
			if member == nil || member.GroupID != groupID || (member.UserID == "" || member.UserID != d.loginUserID && member.RoleLevel != 100) {
				return fmt.Errorf("invalid partial group member")
			}
			if err := tx.Create(member).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("entity_id = ? AND table_name IN ?", groupID, []string{partial, "local_group_entities_version"}).Delete(&model_struct.LocalVersionSync{}).Error; err != nil {
			return err
		}
		row := &model_struct.LocalVersionSync{Table: partial, EntityID: groupID, VersionID: versionID, UIDList: model_struct.StringArray{}}
		if err := tx.Create(row).Error; err != nil {
			return err
		}
		// SQLite INTEGER cannot represent the upper half of uint64. BLOB text is
		// parsed losslessly by database/sql when scanning into the uint64 field.
		return tx.Model(row).Update("version", []byte(strconv.FormatUint(version, 10))).Error
	})
}

func (d *DataBase) ResetGroupMemberPartialV2(ctx context.Context, groupID string) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("group_id = ?", groupID).Delete(&model_struct.LocalGroupMember{}).Error; err != nil {
			return err
		}
		// Retain the partial marker until the caller has rebuilt the full baseline.
		// A crash or failed legacy request must not expose an empty cache as complete.
		return tx.Where("entity_id = ? AND table_name = ?", groupID, "local_group_entities_version").Delete(&model_struct.LocalVersionSync{}).Error
	})
}
