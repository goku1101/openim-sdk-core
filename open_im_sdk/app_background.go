package open_im_sdk

import "sync"

// appBackgroundSync tracks foreground transitions independently of request success.
// A failed foreground notification must retain its catch-up for the next retry.
type appBackgroundSync struct {
	mu          sync.Mutex
	background  bool
	wakePending bool
}

func (s *appBackgroundSync) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.background = false
	s.wakePending = false
}

func (s *appBackgroundSync) update(background bool, notify, wake func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.background && !background {
		s.wakePending = true
	}
	s.background = background
	if err := notify(); err != nil {
		return err
	}
	if !background && s.wakePending {
		if err := wake(); err != nil {
			return err
		}
		s.wakePending = false
	}
	return nil
}
