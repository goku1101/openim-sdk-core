package conversation_msg

import (
	"context"
	"github.com/jinzhu/copier"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

func (c *Conversation) prepareSendRequest(ctx context.Context, s *sdk_struct.MsgStruct, lc *model_struct.LocalConversation, offlinePushInfo *sdkws.OfflinePushInfo, options map[string]bool, isOnlineOnly bool) (*sdkws.MsgData, error) {
	//Protocol conversion
	var wsMsgData sdkws.MsgData
	copier.Copy(&wsMsgData, s)
	wsMsgData.AttachedInfo = utils.StructToJsonString(s.AttachedInfoElem)
	wsMsgData.Content = []byte(s.Content)
	wsMsgData.CreateTime = s.CreateTime
	wsMsgData.SendTime = 0
	wsMsgData.Options = options
	if wsMsgData.ContentType == constant.AtText {
		wsMsgData.AtUserIDList = s.AtTextElem.AtUserList
	}
	wsMsgData.OfflinePushInfo = offlinePushInfo
	if !isOnlineOnly && c.syncV2 != nil && c.syncV2.mode.Load() == syncV2Enabled {
		payload, err := proto.Marshal(&wsMsgData)
		if err != nil {
			return nil, err
		}
		payload, err = c.syncV2.store.FreezeSendRequest(ctx, lc.ConversationID, s.ClientMsgID, payload)
		if err != nil {
			return nil, err
		}
		var original sdkws.MsgData
		if err = proto.Unmarshal(payload, &original); err != nil {
			return nil, err
		}
		if !proto.Equal(&original, &wsMsgData) {
			// The callback/preview must describe the payload actually retried,
			// not newly observed profile/preferences or a reconstructed body.
			restored := LocalChatLogToMsgStruct(MsgDataToLocalChatLog(&original))
			restored.LocalEx, restored.Status, restored.SendTime = s.LocalEx, s.Status, s.SendTime
			*s = *restored
		}
		return &original, nil
	}
	return &wsMsgData, nil
}

// Restore only requests already dispatched after a successful upload. New sends
// and upload failures have no frozen request and keep the normal upload path.
func (c *Conversation) restoreFrozenSendRequest(ctx context.Context, s *sdk_struct.MsgStruct, lc *model_struct.LocalConversation, isOnlineOnly bool) (bool, error) {
	if isOnlineOnly || c.syncV2 == nil || c.syncV2.mode.Load() != syncV2Enabled {
		return false, nil
	}
	payload, err := c.syncV2.store.GetSendRequest(ctx, lc.ConversationID, s.ClientMsgID)
	if err != nil || payload == nil {
		return false, err
	}
	var original sdkws.MsgData
	if err = proto.Unmarshal(payload, &original); err != nil {
		return false, err
	}
	restored := LocalChatLogToMsgStruct(MsgDataToLocalChatLog(&original))
	restored.LocalEx, restored.Status, restored.SendTime = s.LocalEx, s.Status, s.SendTime
	restored.Content = string(original.Content)
	*s = *restored
	return true, nil
}

func (c *Conversation) frozenSendTempFiles(s *sdk_struct.MsgStruct) []string {
	var paths []string
	switch s.ContentType {
	case constant.Picture:
		paths = []string{s.PictureElem.SourcePath}
	case constant.Sound:
		paths = []string{s.SoundElem.SoundPath}
	case constant.Video:
		paths = []string{s.VideoElem.VideoPath, s.VideoElem.SnapshotPath}
	case constant.File:
		paths = []string{s.FileElem.FilePath}
	}
	var files []string
	for _, path := range paths {
		if path != "" {
			files = append(files, utils.FileTmpPath(path, c.DataDir))
		}
	}
	return files
}
