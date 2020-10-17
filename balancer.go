package mssqlx

import (
	"context"
	"sync/atomic"
	"time"
)

// database balancer and health checker.
type balancer struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	dbs                   *dbList
	fail                  chan *wrapper
	isWsrep               bool
	isMulti               bool
	numberOfHealthChecker int
	_                     [8]uint64 // prevent false sharing
	healthCheckPeriod     uint64
	_                     [8]uint64
}

// new balancer and start health checkers
func newBalancer(ctx context.Context, numHealthChecker int, numDbInstance int, isWsrep bool) *balancer {
	if numHealthChecker <= 0 {
		numHealthChecker = 2 // at least two checkers
	}

	c := &balancer{
		numberOfHealthChecker: numHealthChecker,
		dbs:                   &dbList{},
		fail:                  make(chan *wrapper, numDbInstance),
		isWsrep:               isWsrep,
		isMulti:               numDbInstance > 1,
		healthCheckPeriod:     DefaultHealthCheckPeriodInMilli,
	}

	// setup context
	c.ctx, c.cancel = context.WithCancel(ctx)

	// run health checker
	for i := 0; i < numHealthChecker; i++ {
		go c.healthChecker()
	}

	return c
}

func (c *balancer) size() int {
	return c.dbs.size()
}

func (c *balancer) getHealthCheckPeriod() uint64 {
	return atomic.LoadUint64(&c.healthCheckPeriod)
}

func (c *balancer) setHealthCheckPeriod(period uint64) {
	if period == 0 {
		period = DefaultHealthCheckPeriodInMilli
	}
	atomic.StoreUint64(&c.healthCheckPeriod, period)
}

// add a db connection to handle in balancer
func (c *balancer) add(w *wrapper) {
	c.dbs.add(w)
}

// get a db to handle our query
func (c *balancer) get(shouldBalancing bool) *wrapper {
	if shouldBalancing {
		return c.dbs.next()
	}
	return c.dbs.current()
}

// failure make a db node become failure and auto health tracking
func (c *balancer) failure(w *wrapper) {
	if c.dbs.remove(w) { // remove this node
		c.sendFailure(w)
	}
}

func (c *balancer) sendFailure(w *wrapper) {
	select {
	case <-c.ctx.Done():
		return

	case c.fail <- w: // give to health checker
	}
}

// healthChecker daemon to check health of db connection
func (c *balancer) healthChecker() {
	doneCh := c.ctx.Done()

	var db *wrapper
	for {
		select {
		case <-doneCh:
			return

		case db = <-c.fail:
			if ping(db) == nil && (!c.isWsrep || db.checkWsrepReady()) {
				c.dbs.add(db)
				continue
			}

			select {
			case <-doneCh:
				return

			case <-time.After(time.Duration(c.getHealthCheckPeriod()) * time.Millisecond):
			}

			select {
			case <-doneCh:
				return

			case c.fail <- db:
			}
		}
	}
}

func (c *balancer) destroy() {
	c.dbs.clear()
	c.cancel()
}
