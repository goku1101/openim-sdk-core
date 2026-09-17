package open_im_sdk

import (
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"sync"
)

// Keep the handle retained by the connection manager and API error callback
// stable when a new native bridge instance reinitializes an existing SDK.
type connectionListenerProxy struct {
	mu       sync.RWMutex
	listener open_im_sdk_callback.OnConnListener
}

func (p *connectionListenerProxy) replace(listener open_im_sdk_callback.OnConnListener) {
	p.mu.Lock()
	p.listener = listener
	p.mu.Unlock()
}

func (p *connectionListenerProxy) current() open_im_sdk_callback.OnConnListener {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.listener
}
func (p *connectionListenerProxy) OnConnecting() {
	if listener := p.current(); listener != nil {
		listener.OnConnecting()
	}
}
func (p *connectionListenerProxy) OnConnectSuccess() {
	if listener := p.current(); listener != nil {
		listener.OnConnectSuccess()
	}
}
func (p *connectionListenerProxy) OnConnectFailed(code int32, message string) {
	if listener := p.current(); listener != nil {
		listener.OnConnectFailed(code, message)
	}
}
func (p *connectionListenerProxy) OnKickedOffline() {
	if listener := p.current(); listener != nil {
		listener.OnKickedOffline()
	}
}
func (p *connectionListenerProxy) OnUserTokenExpired() {
	if listener := p.current(); listener != nil {
		listener.OnUserTokenExpired()
	}
}
func (p *connectionListenerProxy) OnUserTokenInvalid(message string) {
	if listener := p.current(); listener != nil {
		listener.OnUserTokenInvalid(message)
	}
}
