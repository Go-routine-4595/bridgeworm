package usecase

import (
	"sync"
	"sync/atomic"

	"github.com/Go-routine-4595/bridgeworm/domain"
)

// Custom pool wrapper to track creation
type trackedPool struct {
	pool      *sync.Pool
	creations *atomic.Uint64
}

func (tp *trackedPool) Get() *domain.NATSMessage {
	return tp.pool.Get().(*domain.NATSMessage)
}

func (tp *trackedPool) Put(msg *domain.NATSMessage) {
	tp.pool.Put(msg)
}

func newTrackedPool(creations *atomic.Uint64) *trackedPool {
	return &trackedPool{
		creations: creations,
		pool: &sync.Pool{
			New: func() interface{} {
				// Increment counter when New() is called (= pool miss)
				creations.Add(1)
				return &domain.NATSMessage{
					Byte: make([]byte, 0, 1024),
				}
			},
		},
	}
}
