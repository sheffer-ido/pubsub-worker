package safe

import "sync"

type SafeHealth struct {
	mu     sync.Mutex
	status bool
}

func (s *SafeHealth) Set(status bool) {
	s.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	s.status = status
	s.mu.Unlock()
}

func (s *SafeHealth) GetStatus() bool {
	s.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer s.mu.Unlock()
	return s.status
}
