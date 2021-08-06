package mssqlx

import (
	"sync"
	"sync/atomic"
)

type dbList struct {
	lk           sync.RWMutex
	list         []*wrapper
	currentIndex uint32
}

func (b *dbList) current() (w *wrapper) {
	b.lk.RLock()

	if n := uint32(len(b.list)); n > 0 {
		w = b.list[atomic.LoadUint32(&b.currentIndex)%n]
	}

	b.lk.RUnlock()

	return
}

func (b *dbList) next() (w *wrapper) {
	b.lk.RLock()

	if n := uint32(len(b.list)); n > 0 {
		w = b.list[atomic.AddUint32(&b.currentIndex, 1)%n]
	}

	b.lk.RUnlock()

	return
}

func (b *dbList) add(w *wrapper) {
	if w != nil {
		b.lk.Lock()
		b.list = append(b.list, w)
		b.lk.Unlock()
	}
}

func (b *dbList) remove(w *wrapper) (removed bool) {
	if w != nil {
		b.lk.Lock()

		n := len(b.list)
		for i := 0; i < n; i++ {
			if b.list[i] == w { // found
				removed = true

				if i != n-1 {
					b.list[i] = b.list[n-1]
				}
				b.list = b.list[:n-1]

				break
			}
		}

		b.lk.Unlock()
	}

	return
}
