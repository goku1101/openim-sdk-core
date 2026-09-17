package group

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

type memberProfileCache struct {
	data *lru.Cache[string, *model_struct.LocalGroupMember]
}

func newMemberProfileCache() *memberProfileCache {
	data, _ := lru.New[string, *model_struct.LocalGroupMember](4096)
	return &memberProfileCache{data}
}
func (c *memberProfileCache) Load(k string) (*model_struct.LocalGroupMember, bool) {
	return c.data.Get(k)
}
func (c *memberProfileCache) Store(k string, v *model_struct.LocalGroupMember) { c.data.Add(k, v) }
func (c *memberProfileCache) Delete(k string)                                  { c.data.Remove(k) }
func (c *memberProfileCache) clearGroup(groupID string) {
	for _, k := range c.data.Keys() {
		if v, ok := c.data.Peek(k); ok && v.GroupID == groupID {
			c.data.Remove(k)
		}
	}
}
