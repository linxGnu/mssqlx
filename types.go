package mssqlx

import (
	"runtime"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

type wrapper struct {
	db  *sqlx.DB
	dsn string
}

func (w *wrapper) checkWsrepReady() bool {
	type wsrepVariable struct {
		VariableName string `db:"Variable_name"`
		Value        string `db:"Value"`
	}

	var v wsrepVariable

	if err := w.db.Get(&v, "SHOW VARIABLES LIKE 'wsrep_on'"); err != nil {
		reportError("SHOW VARIABLES LIKE 'wsrep_on'", err)
		return false
	}

	if v.Value != "ON" {
		return true
	}

	if err := w.db.Get(&v, "SHOW STATUS LIKE 'wsrep_ready'"); err != nil || v.Value != "ON" {
		reportError("SHOW STATUS LIKE 'wsrep_ready'", err)
		return false
	}

	return true
}

var empty = []*wrapper{}

type dbList struct {
	list         atomic.Value // []*wrapper
	_            [9]uint64    // prevent false sharing
	state        int32
	_            [9]uint64
	currentIndex uint32
	_            [9]uint64
}

func (b *dbList) current() (w *wrapper) {
	list, stored := b.list.Load().([]*wrapper)
	if stored {
		if n := uint32(len(list)); n > 0 {
			w = list[atomic.LoadUint32(&b.currentIndex)%n]
		}
	}
	return
}

func (b *dbList) next() (w *wrapper) {
	list, stored := b.list.Load().([]*wrapper)
	if stored {
		if n := uint32(len(list)); n > 0 {
			w = list[atomic.AddUint32(&b.currentIndex, 1)%n]
		}
	}
	return
}

func (b *dbList) add(w *wrapper) {
	if w != nil {
		for {
			if atomic.CompareAndSwapInt32(&b.state, 0, 1) { // lock first
				list, stored := b.list.Load().([]*wrapper)
				if !stored {
					list = make([]*wrapper, 0, 8)
				} else {
					n := len(list)
					newList := make([]*wrapper, n, n+1)
					copy(newList, list) // copy-on-write
					list = newList
				}

				// append to list
				list = append(list, w)

				// store back
				b.list.Store(list)

				atomic.CompareAndSwapInt32(&b.state, 1, 0)
				return
			}
			runtime.Gosched()
		}
	}
}

func (b *dbList) remove(w *wrapper) (removed bool) {
	if w != nil {
		for {
			if atomic.CompareAndSwapInt32(&b.state, 0, 1) { // lock first
				list, stored := b.list.Load().([]*wrapper)
				if stored {
					if n := len(list); n > 0 {
						for i := range list {
							if list[i] == w { // found
								removed = true

								newList := make([]*wrapper, n-1, n)
								if i > 0 {
									copy(newList, list[:i])
								}
								if i < n-1 {
									copy(newList[i:], list[i+1:])
								}

								b.list.Store(newList)
								break
							}
						}
					}
				}

				atomic.CompareAndSwapInt32(&b.state, 1, 0)
				return
			}
			runtime.Gosched()
		}
	}
	return
}

func (b *dbList) clear() {
	atomic.StoreUint32(&b.currentIndex, 0)
	b.list.Store(empty)
}
