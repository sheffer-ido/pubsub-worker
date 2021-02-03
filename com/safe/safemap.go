package safe

import (
	"sync"

	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
)

type SafeMap struct {
	mu sync.Mutex
	v  map[int32]*pb.Request
}

// Inc increments the counter for the given key.
func (c *SafeMap) Add(key int32, value *pb.Request) {
	c.mu.Lock()

	if c.v == nil {
		c.v = make(map[int32]*pb.Request)
	}
	c.v[key] = value

	c.mu.Unlock()
}

func (c *SafeMap) GetAll() []*pb.Request {
	c.mu.Lock()
	arr := []*pb.Request{}
	for _, r := range c.v {
		arr = append(arr, r)
	}
	c.mu.Unlock()
	return arr
}

// Value returns the current value of the counter for the given key.
func (c *SafeMap) Value(key int32) *pb.Request {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	return c.v[key]
}

func (c *SafeMap) Remove(key int32) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	delete(c.v, key)
}
