package mssqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/mssqlx/types"
)

var (
	// ErrNetwork networking error
	ErrNetwork = errors.New("Network error/Connection refused")

	// ErrNoConnection there is no connection to db
	ErrNoConnection = errors.New("No connection available")

	// ErrNoConnectionOrWsrep there is no connection to db or Wsrep is not ready
	ErrNoConnectionOrWsrep = errors.New("No connection available or Wsrep is not ready")
)

const (
	DefaultHealthCheckPeriodInMilli = 500
)

func parseError(db *DB, err error) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(net.Error); ok {
		return ErrNetwork
	}

	if _, err := db.Exec("SELECT 1"); err != nil {
		return ErrNetwork
	}

	return err
}

// dbLinkListNode a node of linked-list contains sqlx.DB
type dbLinkListNode struct {
	db   *DB
	next *dbLinkListNode
	prev *dbLinkListNode
}

type DBNode interface {
	GetDB() *DB
}

func (c *dbLinkListNode) GetDB() *DB {
	return c.db
}

// dbLinkList a round robin and thread-safe linked-list of sqlx.DB
type dbLinkList struct {
	// head and tail of linked-list
	head *dbLinkListNode
	tail *dbLinkListNode

	// size of this linked-list
	size int

	// current point on linked-list
	current *dbLinkListNode

	lock sync.RWMutex
}

// Next return next element from current node on linked-list. If current node is last node, the next one is head.
func (c *dbLinkList) next() *dbLinkListNode {
	c.lock.RLock()
	if c.current == nil {
		c.lock.RUnlock()
		return nil
	}
	defer c.lock.RUnlock()

	return c.current.next
}

// Prev return previous element from current node on linked-list. If current node is head node, the previous one is tail.
func (c *dbLinkList) prev() *dbLinkListNode {
	c.lock.RLock()
	if c.current == nil {
		c.lock.RUnlock()
		return nil
	}
	defer c.lock.RUnlock()

	return c.current.prev
}

// add node to last of linked-list
func (c *dbLinkList) add(node *dbLinkListNode) {
	if node == nil || node.db == nil {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.size++

	if c.head == nil {
		c.head, c.tail, c.current = node, node, node
		node.next = node
		node.prev = node

		return
	}

	node.next, node.prev = c.head, c.tail
	c.head.prev, c.tail.next = node, node

	c.tail = node
}

// remove a node
func (c *dbLinkList) remove(node *dbLinkListNode) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if node == nil || node.next == nil || node.prev == nil || c.size == 0 { // important to prevent double remove
		return false
	}

	node.prev.next, node.next.prev = node.next, node.prev

	// Check size
	if c.size--; c.size == 0 {
		c.head, c.tail, c.current = nil, nil, nil
		node.next, node.prev = nil, nil // important to prevent double remove

		return true
	}

	if c.current == node {
		c.current = node.next
	}

	if c.head == node {
		c.head = node.next
	}

	if c.tail == node {
		c.tail = node.prev
	}

	node.next, node.prev = nil, nil // important to prevent double remove
	return true
}

// moveNext get current and make current pointer to next
func (c *dbLinkList) moveNext() (cur *dbLinkListNode) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if cur = c.current; cur != nil {
		c.current = cur.next
	}

	return
}

// movePrev get current and make current pointer to previous
func (c *dbLinkList) movePrev() (cur *dbLinkListNode) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if cur = c.current; cur != nil {
		c.current = cur.prev
	}

	return
}

// getCurrentNode get current pointer node
func (c *dbLinkList) getCurrentNode() *dbLinkListNode {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.current
}

// clear all nodes
func (c *dbLinkList) clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.head, c.tail, c.current = nil, nil, nil
}

// dbBalancer database balancer and health checker.
type dbBalancer struct {
	dbs                   *dbLinkList
	fail                  chan *DB
	isWsrep               bool
	_name                 string
	numberOfHealthChecker int
	healthCheckPeriod     int64
	healthCheckPeriodLock sync.RWMutex
}

// init balancer and start health checkers
func (c *dbBalancer) init(numHealthChecker int, numDbInstance int, isWsrep bool) {
	if numHealthChecker <= 0 {
		numHealthChecker = 2 // at least two checkers
	}

	c.numberOfHealthChecker = numHealthChecker
	c.dbs = &dbLinkList{}
	c.fail = make(chan *DB, numDbInstance)
	c.healthCheckPeriod = DefaultHealthCheckPeriodInMilli
	c.isWsrep = isWsrep

	for i := 0; i < numHealthChecker; i++ {
		go c.healthChecker()
	}
}

// add a db connection to handle in balancer
func (c *dbBalancer) add(db *DB) {
	c.dbs.add(&dbLinkListNode{db: db})
}

// get a db to handle our query
func (c *dbBalancer) get(autoBalance bool) *dbLinkListNode {
	if autoBalance {
		return c.dbs.moveNext()
	}

	return c.dbs.getCurrentNode()
}

// failure make a db node become failure and auto health tracking
func (c *dbBalancer) failure(node *dbLinkListNode) {
	defer func() {
		if e := recover(); e != nil {
		}
	}()

	if c.dbs.remove(node) { // remove this node
		c.fail <- node.db // give to health checker
	}
}

// setHealthCheckPeriod in miliseconds
func (c *dbBalancer) setHealthCheckPeriod(period uint64) {
	c.healthCheckPeriodLock.Lock()
	defer c.healthCheckPeriodLock.Unlock()

	if c.healthCheckPeriod = int64(period); c.healthCheckPeriod <= 0 {
		c.healthCheckPeriod = DefaultHealthCheckPeriodInMilli
	}
}

// checkWsrepReady check if wsrep is in ready state
func (c *dbBalancer) checkWsrepReady(db *DB) bool {
	var tmp types.WsrepVariable
	if err := db.Get(&tmp, "SHOW VARIABLES LIKE 'wsrep_on'"); err != nil {
		return false
	}

	if tmp.Value != "ON" {
		return true
	}

	if err := db.Get(&tmp, "SHOW STATUS LIKE 'wsrep_ready'"); err != nil || tmp.Value != "ON" {
		return false
	}

	return true
}

// healthChecker daemon to check health of db connection
func (c *dbBalancer) healthChecker() {
	defer func() {
		if e := recover(); e != nil {
		}
	}()

	var healthCheckPeriod int64
	for db := range c.fail {
		if db == nil {
			continue
		}

		if err := db.Ping(); err == nil {
			if !c.isWsrep || c.checkWsrepReady(db) { // check wresp
				c.dbs.add(&dbLinkListNode{db: db})
				continue
			}
		}

		c.healthCheckPeriodLock.RLock()
		healthCheckPeriod = c.healthCheckPeriod
		c.healthCheckPeriodLock.RUnlock()
		time.Sleep(time.Duration(healthCheckPeriod) * time.Millisecond)

		c.fail <- db
	}
}

func (c *dbBalancer) destroy() {
	c.dbs.clear()
	close(c.fail)
}

// DBs sqlx wrapper supports querying master-slave database instances for HA and scalability, auto-balancer integrated.
type DBs struct {
	driverName string

	// master instances
	masters  *dbBalancer
	_masters []*DB

	// slaves instances
	slaves  *dbBalancer
	_slaves []*DB

	// master and slave lock
	masterLock sync.RWMutex
	slaveLock  sync.RWMutex

	// store all database instances
	all  *dbBalancer
	_all []*DB
}

// DriverName returns the driverName passed to the Open function for this DB.
func (dbs *DBs) DriverName() string {
	return dbs.driverName
}

// GetMaster get master database instance from balancer
func (dbs *DBs) GetMaster() (DBNode, int) {
	return dbs.masters.get(false), len(dbs._masters)
}

// ProcessMasterErr process master error
func (dbs *DBs) ProcessMasterErr(db DBNode, err error) {
	switch db.(type) {
	case *dbLinkListNode:
		node := db.(*dbLinkListNode)
		if err = parseError(node.db, err); err == ErrNetwork {
			dbs.masters.failure(node)
		}
	default:
		return
	}
}

// GetAllSlaves get all slave database instances
func (dbs *DBs) GetAllSlaves() ([]*DB, int) {
	return dbs._slaves, len(dbs._slaves)
}

func _ping(target []*DB) []error {
	if target == nil {
		return nil
	}

	nn := len(target)
	if nn == 0 {
		return nil
	}

	errResult := make([]error, nn)

	var wg sync.WaitGroup
	for i := range target {
		wg.Add(1)
		go func(ind int, wg *sync.WaitGroup) {
			defer wg.Done()
			if target[ind] != nil {
				errResult[ind] = target[ind].Ping()
			}
		}(i, &wg)
	}
	wg.Wait()

	return errResult
}

// Ping all master-slave database instances
func (dbs *DBs) Ping() []error {
	return _ping(dbs._all)
}

// PingMaster all master database instances
func (dbs *DBs) PingMaster() []error {
	return _ping(dbs._masters)
}

// PingSlave all slave database instances
func (dbs *DBs) PingSlave() []error {
	return _ping(dbs._slaves)
}

func _close(target []*DB) []error {
	if target == nil {
		return nil
	}

	nn := len(target)
	if nn == 0 {
		return nil
	}

	errResult := make([]error, nn)

	var wg sync.WaitGroup
	for i, db := range target {
		wg.Add(1)
		go func(db *DB, ind int, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				errResult[ind] = db.Close()
			}
		}(db, i, &wg)
	}
	wg.Wait()

	return errResult
}

// Destroy closes all database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) Destroy() []error {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	res := _close(dbs._all)

	if dbs.masters != nil {
		dbs.masters.destroy()
	}

	if dbs.slaves != nil {
		dbs.slaves.destroy()
	}

	dbs.all.destroy()
	return res
}

// DestroyMaster closes all master database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) DestroyMaster() []error {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	if dbs.masters != nil {
		dbs.masters.destroy()
	}

	return _close(dbs._masters)
}

// DestroySlave closes all master database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) DestroySlave() []error {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	if dbs.slaves != nil {
		dbs.slaves.destroy()
	}

	return _close(dbs._slaves)
}

func _setMaxIdleConns(target []*DB, n int) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		wg.Add(1)
		go func(db *DB, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				db.SetMaxIdleConns(n)
			}
		}(db, &wg)
	}
	wg.Wait()
}

// SetHealthCheckPeriod sets the period (in millisecond) for checking health of failed nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetHealthCheckPeriod(period uint64) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.masters.setHealthCheckPeriod(period)

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	dbs.slaves.setHealthCheckPeriod(period)
}

// SetMasterHealthCheckPeriod sets the period (in millisecond) for checking health of failed master nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetMasterHealthCheckPeriod(period uint64) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.masters.setHealthCheckPeriod(period)
}

// SetSlaveHealthCheckPeriod sets the period (in millisecond) for checking health of failed slave nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetSlaveHealthCheckPeriod(period uint64) {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	dbs.slaves.setHealthCheckPeriod(period)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for all masters-slaves.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (dbs *DBs) SetMaxIdleConns(n int) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setMaxIdleConns(dbs._all, n)
}

// SetMasterMaxIdleConns sets the maximum number of connections in the idle
// connection pool for masters.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (dbs *DBs) SetMasterMaxIdleConns(n int) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	_setMaxIdleConns(dbs._masters, n)
}

// SetSlaveMaxIdleConns sets the maximum number of connections in the idle
// connection pool for slaves.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (dbs *DBs) SetSlaveMaxIdleConns(n int) {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setMaxIdleConns(dbs._slaves, n)
}

func _setMaxOpenConns(target []*DB, n int) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		wg.Add(1)
		go func(db *DB, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				db.SetMaxOpenConns(n)
			}
		}(db, &wg)
	}
	wg.Wait()
}

// SetMaxOpenConns sets the maximum number of open connections to all master-slave databases.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (dbs *DBs) SetMaxOpenConns(n int) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setMaxOpenConns(dbs._all, n)
}

// SetMasterMaxOpenConns sets the maximum number of open connections to the master databases.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (dbs *DBs) SetMasterMaxOpenConns(n int) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	_setMaxOpenConns(dbs._masters, n)
}

// SetSlaveMaxOpenConns sets the maximum number of open connections to the slave databases.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (dbs *DBs) SetSlaveMaxOpenConns(n int) {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setMaxOpenConns(dbs._slaves, n)
}

func _setConnMaxLifetime(target []*DB, d time.Duration) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		wg.Add(1)
		go func(db *DB, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				db.SetConnMaxLifetime(d)
			}
		}(db, &wg)
	}
	wg.Wait()
}

// SetConnMaxLifetime sets the maximum amount of time a master-slave connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetConnMaxLifetime(d time.Duration) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setConnMaxLifetime(dbs._all, d)
}

// SetMasterConnMaxLifetime sets the maximum amount of time a master connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetMasterConnMaxLifetime(d time.Duration) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	_setConnMaxLifetime(dbs._masters, d)
}

// SetSlaveConnMaxLifetime sets the maximum amount of time a slave connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetSlaveConnMaxLifetime(d time.Duration) {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setConnMaxLifetime(dbs._slaves, d)
}

func _stats(target []*DB) []sql.DBStats {
	if target == nil {
		return nil
	}

	nn := len(target)
	if nn == 0 {
		return nil
	}

	result := make([]sql.DBStats, nn)

	var wg sync.WaitGroup
	for ind, db := range target {
		wg.Add(1)
		go func(db *DB, ind int, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				result[ind] = db.Stats()
			}
		}(db, ind, &wg)
	}
	wg.Wait()

	return result
}

// Stats returns database statistics.
func (dbs *DBs) Stats() []sql.DBStats {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	return _stats(dbs._all)
}

// StatsMaster returns master database statistics.
func (dbs *DBs) StatsMaster() []sql.DBStats {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	return _stats(dbs._masters)
}

// StatsSlave returns slave database statistics.
func (dbs *DBs) StatsSlave() []sql.DBStats {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	return _stats(dbs._slaves)
}

func _mapperFunc(target []*DB, mf func(string) string) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for ind, db := range target {
		wg.Add(1)
		go func(db *DB, ind int) {
			defer wg.Done()
			if db != nil {
				db.MapperFunc(mf)
			}
		}(db, ind)
	}
	wg.Wait()
}

// MapperFunc sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (dbs *DBs) MapperFunc(mf func(string) string) {
	_mapperFunc(dbs._all, mf)
}

// MapperFuncMaster sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (dbs *DBs) MapperFuncMaster(mf func(string) string) {
	_mapperFunc(dbs._masters, mf)
}

// MapperFuncSlave sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (dbs *DBs) MapperFuncSlave(mf func(string) string) {
	_mapperFunc(dbs._slaves, mf)
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (dbs *DBs) Rebind(query string) string {
	if dbs._all == nil || len(dbs._all) == 0 {
		return ""
	}

	for _, db := range dbs._all {
		if db != nil {
			return db.Rebind(query)
		}
	}

	return ""
}

// BindNamed binds a query using the DB driver's bindvar type.
func (dbs *DBs) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	if dbs._all == nil || len(dbs._all) == 0 {
		return "", nil, ErrNoConnection
	}

	for _, db := range dbs._all {
		if db != nil {
			return db.BindNamed(query, arg)
		}
	}

	return "", nil, nil
}

func _namedQuery(target *dbBalancer, query string, arg interface{}) (res *Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		db = target.get(true)
		if db == nil {
			if target.isWsrep {
				return nil, ErrNoConnectionOrWsrep
			}

			return nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.NamedQuery(query, arg)
		if e = parseError(db.db, e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		if e != nil && target.isWsrep && (strings.HasPrefix(e.Error(), "ERROR 1047") || strings.HasPrefix(e.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return r, e
	}
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.slaves, query, arg)
}

// NamedQueryOnMaster using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryOnMaster(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.masters, query, arg)
}

func _namedExec(target *dbBalancer, query string, arg interface{}) (res sql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		db = target.get(false)
		if db == nil {
			if target.isWsrep {
				return nil, ErrNoConnectionOrWsrep
			}

			return nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.NamedExec(query, arg)
		if e = parseError(db.db, e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		if e != nil && target.isWsrep && (strings.HasPrefix(e.Error(), "ERROR 1047") || strings.HasPrefix(e.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return r, e
	}
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.masters, query, arg)
}

// NamedExecOnSlave using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecOnSlave(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.slaves, query, arg)
}

func _query(target *dbBalancer, query string, args ...interface{}) (dbr *DB, res *sql.Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		db = target.get(true)
		if db == nil {
			if target.isWsrep {
				return nil, nil, ErrNoConnectionOrWsrep
			}

			return nil, nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.Query(query, args...)
		if e = parseError(db.db, e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		if e != nil && target.isWsrep && (strings.HasPrefix(e.Error(), "ERROR 1047") || strings.HasPrefix(e.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return db.db, r, e
	}
}

func _queryx(target *dbBalancer, query string, args ...interface{}) (*Rows, error) {
	db, r, err := _query(target, query, args...)
	if err != nil {
		return nil, err
	}

	if db == nil {
		return &Rows{Rows: r}, err
	}

	return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Queryx queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) Queryx(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.slaves, query, args...)
}

// QueryxOnMaster queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryxOnMaster(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.masters, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRow(query string, args ...interface{}) *Row {
	_, rows, err := _query(dbs.slaves, query, args...)
	return &Row{rows: rows, err: err}
}

// QueryRowOnMaster executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowOnMaster(query string, args ...interface{}) *Row {
	_, rows, err := _query(dbs.masters, query, args...)
	return &Row{rows: rows, err: err}
}

func _select(target *dbBalancer, dest interface{}, query string, args ...interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return ErrNoConnection
	}

	rows, err := _queryx(target, query, args...)
	if err != nil {
		return err
	}

	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()

	return scanAll(rows, dest, false)
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) Select(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.slaves, dest, query, args...)
}

// SelectOnMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectOnMaster(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.masters, dest, query, args...)
}

func _queryRowx(target *dbBalancer, que string, args ...interface{}) *Row {
	db, rows, err := _query(target, que, args...)
	if db == nil {
		return &Row{rows: rows, err: err}
	}

	return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
}

// QueryRowx queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowx(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.slaves, query, args...)
}

// QueryRowxOnMaster queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowxOnMaster(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.masters, query, args...)
}

func _get(target *dbBalancer, dest interface{}, query string, args ...interface{}) error {
	return _queryRowx(target, query, args...).scanAny(dest, false)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) Get(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.slaves, dest, query, args...)
}

// GetOnMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetOnMaster(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.masters, dest, query, args...)
}

// Exec do exec on masters
func (dbs *DBs) Exec(query string, args ...interface{}) (sql.Result, error) {
	return _exec(dbs.masters, query, args...)
}

// Exec do exec on slave only
func (dbs *DBs) ExecSlave(query string, args ...interface{}) (sql.Result, error) {
	return _exec(dbs.slaves, query, args...)
}

func _exec(target *dbBalancer, query string, args ...interface{}) (res sql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		db = target.get(false)
		if db == nil {
			if target.isWsrep {
				return nil, ErrNoConnectionOrWsrep
			}

			return nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.Exec(query, args...)
		if e = parseError(db.db, e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		if e != nil && target.isWsrep && (strings.HasPrefix(e.Error(), "ERROR 1047") || strings.HasPrefix(e.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return r, e
	}
}

// ConnectMasterSlaves to master-slave databases and verify with pings
// driverName: mysql, postgres, etc
// masterDSNs: data source names of Masters
// slaveDSNs: data source names of Slaves
// args: args[0] = true to indicates galera/wsrep cluster
func ConnectMasterSlaves(driverName string, masterDSNs []string, slaveDSNs []string, args ...interface{}) (*DBs, []error) {
	// Validate slave address
	if slaveDSNs == nil {
		slaveDSNs = []string{}
	}

	if masterDSNs == nil {
		masterDSNs = []string{}
	}

	nMaster := len(masterDSNs)
	nSlave := len(slaveDSNs)

	errResult := make([]error, nMaster+nSlave)
	dbs := &DBs{
		driverName: driverName,
		masters:    &dbBalancer{},
		_masters:   make([]*DB, nMaster),
		slaves:     &dbBalancer{},
		_slaves:    make([]*DB, nSlave),
		all:        &dbBalancer{},
		_all:       make([]*DB, nMaster+nSlave),
	}

	isWsrep := false
	if len(args) > 0 {
		switch args[0].(type) {
		case bool:
			isWsrep = args[0].(bool)
		}
	}

	dbs.masters.init(nMaster<<2/10, nMaster, isWsrep) // 40%
	dbs.masters._name = "masters"

	dbs.slaves.init(nSlave<<2/10, nSlave, isWsrep) // 40%
	dbs.slaves._name = "slaves"

	dbs.all.init((nMaster+nSlave)<<2/10, nMaster+nSlave, isWsrep) // 40%
	dbs.all._name = "all"

	// channel to sync routines
	c := make(chan byte, len(errResult))

	// Concurrency connect to master
	n := 0
	for i := range masterDSNs {
		go func(mId, eId int) {
			dbs._masters[mId], errResult[eId] = Connect(driverName, masterDSNs[mId])
			dbs.masters.add(dbs._masters[mId])

			dbs._all[eId] = dbs._masters[mId]
			dbs.all.add(dbs._masters[mId])

			c <- 0
		}(i, n)
		n++
	}

	// Concurrency connect to slaves
	for i := range slaveDSNs {
		go func(sId, eId int) {
			dbs._slaves[sId], errResult[eId] = Connect(driverName, slaveDSNs[sId])
			dbs.slaves.add(dbs._slaves[sId])

			dbs._all[eId] = dbs._slaves[sId]
			dbs.all.add(dbs._slaves[sId])

			c <- 0
		}(i, n)
		n++
	}

	for i := 0; i < len(errResult); i++ {
		<-c
	}

	return dbs, errResult
}
