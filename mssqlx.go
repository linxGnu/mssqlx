package mssqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
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
	// DefaultHealthCheckPeriodInMilli default period in millisecond mssqlx should do a health check of failed database
	DefaultHealthCheckPeriodInMilli = 40
)

func ping(db *sqlx.DB) (err error) {
	_, err = db.Exec("SELECT 1")
	return
}

func parseError(db *sqlx.DB, err error) error {
	if err == nil {
		return nil
	}

	if db != nil {
		if ping(db) != nil {
			return ErrNetwork
		}
	}

	return err
}

// dbLinkListNode a node of linked-list contains sqlx.DB
type dbLinkListNode struct {
	db   *sqlx.DB
	next *dbLinkListNode
	prev *dbLinkListNode
}

// DBNode interface of a db node
type DBNode interface {
	GetDB() *sqlx.DB
}

func (c *dbLinkListNode) GetDB() *sqlx.DB {
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

	c.head, c.tail, c.current, c.size = nil, nil, nil, 0
}

// dbBalancer database balancer and health checker.
type dbBalancer struct {
	driverName string

	dbs  *dbLinkList
	fail chan *sqlx.DB

	isWsrep bool
	isMulti bool

	_name string

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
	c.fail = make(chan *sqlx.DB, numDbInstance)
	c.isWsrep = isWsrep
	c.isMulti = numDbInstance > 1

	c.healthCheckPeriod = DefaultHealthCheckPeriodInMilli

	for i := 0; i < numHealthChecker; i++ {
		go c.healthChecker()
	}
}

// add a db connection to handle in balancer
func (c *dbBalancer) add(db *sqlx.DB) {
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

type wsrepVariable struct {
	VariableName string `db:"Variable_name"`
	Value        string `db:"Value"`
}

// checkWsrepReady check if wsrep is in ready state
func (c *dbBalancer) checkWsrepReady(db *sqlx.DB) bool {
	var tmp wsrepVariable
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

func (c *dbBalancer) getHealthCheckPeriod() int64 {
	c.healthCheckPeriodLock.RLock()
	defer c.healthCheckPeriodLock.RUnlock()

	return c.healthCheckPeriod
}

// healthChecker daemon to check health of db connection
func (c *dbBalancer) healthChecker() {
	defer func() {
		if e := recover(); e != nil {
		}
	}()

	for db := range c.fail {
		if ping(db) == nil && (!c.isWsrep || c.checkWsrepReady(db)) {
			c.dbs.add(&dbLinkListNode{db: db})
			continue
		}

		time.Sleep(time.Duration(c.getHealthCheckPeriod()) * time.Millisecond)

		c.fail <- db
	}
}

func (c *dbBalancer) destroy() {
	c.dbs.clear()
	close(c.fail)
}

// DBs sqlx wrapper supports querying master-slave database connections for HA and scalability, auto-balancer integrated.
type DBs struct {
	driverName string

	// master connections
	masters  *dbBalancer
	_masters []*sqlx.DB

	// slaves connections
	slaves  *dbBalancer
	_slaves []*sqlx.DB

	// master and slave lock
	masterLock sync.RWMutex
	slaveLock  sync.RWMutex

	// store all database connections
	all  *dbBalancer
	_all []*sqlx.DB
}

// DriverName returns the driverName passed to the Open function for this DB.
func (dbs *DBs) DriverName() string {
	return dbs.driverName
}

// GetAllMasters get all master database connections, included failing one.
func (dbs *DBs) GetAllMasters() ([]*sqlx.DB, int) {
	return dbs._masters, len(dbs._masters)
}

// GetAllSlaves get all slave database connections, included failing one.
func (dbs *DBs) GetAllSlaves() ([]*sqlx.DB, int) {
	return dbs._slaves, len(dbs._slaves)
}

func _ping(target []*sqlx.DB) []error {
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

// Ping all master-slave database connections
func (dbs *DBs) Ping() []error {
	return _ping(dbs._all)
}

// PingMaster all master database connections
func (dbs *DBs) PingMaster() []error {
	return _ping(dbs._masters)
}

// PingSlave all slave database connections
func (dbs *DBs) PingSlave() []error {
	return _ping(dbs._slaves)
}

func _close(target []*sqlx.DB) []error {
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
		go func(db *sqlx.DB, ind int, wg *sync.WaitGroup) {
			defer wg.Done()
			if db != nil {
				errResult[ind] = db.Close()
			}
		}(db, i, &wg)
	}
	wg.Wait()

	return errResult
}

// Destroy closes all database connections, releasing any open resources.
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

// DestroyMaster closes all master database connections, releasing any open resources.
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

// DestroySlave closes all master database connections, releasing any open resources.
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

func _setMaxIdleConns(target []*sqlx.DB, n int) {
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
		go func(db *sqlx.DB, wg *sync.WaitGroup) {
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

func _setMaxOpenConns(target []*sqlx.DB, n int) {
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
		go func(db *sqlx.DB, wg *sync.WaitGroup) {
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

func _setConnMaxLifetime(target []*sqlx.DB, d time.Duration) {
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
		go func(db *sqlx.DB, wg *sync.WaitGroup) {
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

func _stats(target []*sqlx.DB) []sql.DBStats {
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
		go func(db *sqlx.DB, ind int, wg *sync.WaitGroup) {
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

func _mapperFunc(target []*sqlx.DB, mf func(string) string) {
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
		go func(db *sqlx.DB, ind int) {
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

	return "", nil, ErrNoConnection
}

func isBadConn(errMessage string) bool {
	switch errMessage {
	case "invalid connection":
		return true
	case "bad connection":
		return true
	}

	return false
}

func isErrBadConn(err error) bool {
	return err == driver.ErrBadConn || // Postgres/Mysql Driver returns default driver.ErrBadConn
		(err != nil && isBadConn(err.Error())) // fix for Mysql Driver ("github.com/go-sql-driver/mysql")
}

func getDBFromBalancer(target *dbBalancer) (db *dbLinkListNode, err error) {
	if db = target.get(target.isMulti); db != nil {
		return
	}

	// retry if there is no connection available. This event could happen when database closes all non-interactive connection.
	for i := 0; i < 4; i++ {
		time.Sleep(time.Duration(target.getHealthCheckPeriod()) * time.Millisecond)
		if db = target.get(target.isMulti); db != nil {
			return
		}
	}

	// need to return error
	if target.isWsrep {
		return nil, ErrNoConnectionOrWsrep
	}

	return nil, ErrNoConnection
}

func _namedQuery(ctx context.Context, target *dbBalancer, query string, arg interface{}) (res *sqlx.Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		res, err = db.db.NamedQueryContext(ctx, query, arg)

		// detect driver.ErrBadConn occurring when a connection idle for a long time.
		// this prevents returning driver.ErrBadConn to application.
		if isErrBadConn(err) {
			if ping(db.db) == nil {
				res, err = db.db.NamedQueryContext(ctx, query, arg)
			}
		}

		// check networking error
		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		// check Wsrep error
		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return
	}
}

// NamedQuery do named query.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(context.Background(), dbs.slaves, query, arg)
}

// NamedQueryOnMaster do named query on master.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryOnMaster(query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(context.Background(), dbs.masters, query, arg)
}

// NamedQueryContext do named query with context.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(ctx, dbs.slaves, query, arg)
}

// NamedQueryContextOnMaster do named query with context on master.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryContextOnMaster(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(ctx, dbs.masters, query, arg)
}

func _namedExec(ctx context.Context, target *dbBalancer, query string, arg interface{}) (res sql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		} else {
			// try to ping db first. Tradeoff a little performance for auto-reset db connection when DBMS restarted
			ping(db.db)
		}

		res, err = db.db.NamedExecContext(ctx, query, arg)
		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		// for galera cluster
		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) {
			target.failure(db)
			continue
		}

		return
	}
}

// NamedExec do named exec.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(context.Background(), dbs.masters, query, arg)
}

// NamedExecOnSlave do named exec on slave.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecOnSlave(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(context.Background(), dbs.slaves, query, arg)
}

// NamedExecContext do named exec with context.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return _namedExec(ctx, dbs.masters, query, arg)
}

// NamedExecContextOnSlave do named exec with context on slave.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecContextOnSlave(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return _namedExec(ctx, dbs.slaves, query, arg)
}

func _query(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (dbr *sqlx.DB, res *sql.Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		res, err = db.db.QueryContext(ctx, query, args...)

		// detect driver.ErrBadConn occurring when a connection idle for a long time.
		// this prevents returning driver.ErrBadConn to application.
		if isErrBadConn(err) {
			if ping(db.db) == nil {
				res, err = db.db.QueryContext(ctx, query, args...)
			}
		}

		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		dbr = db.db
		return
	}
}

// Query executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) Query(query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(context.Background(), dbs.slaves, query, args...)
	return
}

// QueryOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryOnMaster(query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(context.Background(), dbs.masters, query, args...)
	return
}

// QueryContext executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryContext(ctx context.Context, query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(ctx, dbs.slaves, query, args...)
	return
}

// QueryContextOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(ctx, dbs.masters, query, args...)
	return
}

func _queryx(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (dbr *sqlx.DB, res *sqlx.Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		res, err = db.db.QueryxContext(ctx, query, args...)

		// detect driver.ErrBadConn occurring when a connection idle for a long time.
		// this prevents returning driver.ErrBadConn to application.
		if isErrBadConn(err) {
			if ping(db.db) == nil {
				res, err = db.db.QueryxContext(ctx, query, args...)
			}
		}

		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		dbr = db.db
		return
	}
}

// Queryx executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) Queryx(query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(context.Background(), dbs.slaves, query, args...)
	return
}

// QueryxOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryxOnMaster(query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(context.Background(), dbs.masters, query, args...)
	return
}

// QueryxContext executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryxContext(ctx context.Context, query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(ctx, dbs.slaves, query, args...)
	return
}

// QueryxContextOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryxContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(ctx, dbs.masters, query, args...)
	return
}

func _queryRow(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (dbr *sqlx.DB, res *sql.Row, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		res, dbr = db.db.QueryRowContext(ctx, query, args...), db.db
		return
	}
}

// QueryRow executes a query on slaves that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRow(query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(context.Background(), dbs.slaves, query, args...)
	return
}

// QueryRowOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowOnMaster(query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(context.Background(), dbs.masters, query, args...)
	return
}

// QueryRowContext executes a query on slaves that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowContext(ctx context.Context, query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(ctx, dbs.slaves, query, args...)
	return
}

// QueryRowContextOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(ctx, dbs.masters, query, args...)
	return
}

func _queryRowx(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (dbr *sqlx.DB, res *sqlx.Row, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		res, dbr = db.db.QueryRowxContext(ctx, query, args...), db.db
		return
	}
}

// QueryRowx executes a query on slaves that is expected to return at most one row.
// But return sqlx.Row instead of sql.Row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowx(query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(context.Background(), dbs.slaves, query, args...)
	return
}

// QueryRowxOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowxOnMaster(query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(context.Background(), dbs.masters, query, args...)
	return
}

// QueryRowxContext executes a query on slaves that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowxContext(ctx context.Context, query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(ctx, dbs.slaves, query, args...)
	return
}

// QueryRowxContextOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowxContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(ctx, dbs.masters, query, args...)
	return
}

func _select(ctx context.Context, target *dbBalancer, dest interface{}, query string, args ...interface{}) (dbr *sqlx.DB, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		err = db.db.SelectContext(ctx, dest, query, args...)

		// detect driver.ErrBadConn occurring when a connection idle for a long time.
		// this prevents returning driver.ErrBadConn to application.
		if isErrBadConn(err) {
			if ping(db.db) == nil {
				err = db.db.SelectContext(ctx, dest, query, args...)
			}
		}

		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		dbr = db.db
		return
	}
}

// Select do select on slaves.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) Select(dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(context.Background(), dbs.slaves, dest, query, args...)
	return
}

// SelectOnMaster do select on masters.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectOnMaster(dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(context.Background(), dbs.masters, dest, query, args...)
	return
}

// SelectContext do select on slaves with context.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(ctx, dbs.slaves, dest, query, args...)
	return
}

// SelectContextOnMaster do select on masters with context.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectContextOnMaster(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(ctx, dbs.masters, dest, query, args...)
	return
}

func _get(ctx context.Context, target *dbBalancer, dest interface{}, query string, args ...interface{}) (dbr *sqlx.DB, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		err = db.db.GetContext(ctx, dest, query, args...)

		// detect driver.ErrBadConn occurring when a connection idle for a long time.
		// this prevents returning driver.ErrBadConn to application.
		if isErrBadConn(err) {
			if ping(db.db) == nil {
				err = db.db.GetContext(ctx, dest, query, args...)
			}
		}

		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		dbr = db.db
		return
	}
}

// Get on slaves.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) Get(dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _get(context.Background(), dbs.slaves, dest, query, args...)
	return
}

// GetOnMaster on masters.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetOnMaster(dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _get(context.Background(), dbs.masters, dest, query, args...)
	return
}

// GetContext on slaves.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _get(ctx, dbs.slaves, dest, query, args...)
	return
}

// GetContextOnMaster on masters.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetContextOnMaster(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _get(ctx, dbs.masters, dest, query, args...)
	return
}

func _exec(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (res sql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			res, err = nil, fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, ErrNoConnection
	}

	var db *dbLinkListNode
	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		} else {
			// try to ping db first. Tradeoff a little performance for auto-reset db connection when DBMS restarted
			ping(db.db)
		}

		res, err = db.db.ExecContext(ctx, query, args...)
		if err = parseError(db.db, err); err == ErrNetwork {
			target.failure(db)
			continue
		}

		if err != nil && target.isWsrep && (strings.HasPrefix(err.Error(), "ERROR 1047") || strings.HasPrefix(err.Error(), "Error 1047")) { // for galera cluster
			target.failure(db)
			continue
		}

		return
	}
}

// Exec do exec on masters.
func (dbs *DBs) Exec(query string, args ...interface{}) (sql.Result, error) {
	return _exec(context.Background(), dbs.masters, query, args...)
}

// ExecOnSlave do exec on slaves.
func (dbs *DBs) ExecOnSlave(query string, args ...interface{}) (sql.Result, error) {
	return _exec(context.Background(), dbs.slaves, query, args...)
}

// ExecContext do exec on masters with context
func (dbs *DBs) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return _exec(ctx, dbs.masters, query, args...)
}

// ExecContextOnSlave do exec on slaves with context
func (dbs *DBs) ExecContextOnSlave(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return _exec(ctx, dbs.slaves, query, args...)
}

func _prepareContext(ctx context.Context, target *dbBalancer, query string) (dbx *sqlx.DB, stmt *sql.Stmt, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		dbx = db.db
		stmt, err = dbx.PrepareContext(ctx, query)
		return
	}
}

// Prepare creates a prepared statement for later queries or executions on masters.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) Prepare(query string) (db *sqlx.DB, stmt *sql.Stmt, err error) {
	return _prepareContext(context.Background(), dbs.masters, query)
}

// PrepareOnSlave creates a prepared statement for later queries or executions on slaves.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PrepareOnSlave(query string) (db *sqlx.DB, stmt *sql.Stmt, err error) {
	return _prepareContext(context.Background(), dbs.slaves, query)
}

// PrepareContext creates a prepared statement for later queries or executions on masters.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PrepareContext(ctx context.Context, query string) (db *sqlx.DB, stmt *sql.Stmt, err error) {
	return _prepareContext(ctx, dbs.masters, query)
}

// PrepareContextOnSlave creates a prepared statement for later queries or executions on slaves.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PrepareContextOnSlave(ctx context.Context, query string) (db *sqlx.DB, stmt *sql.Stmt, err error) {
	return _prepareContext(ctx, dbs.slaves, query)
}

func _preparexContext(ctx context.Context, target *dbBalancer, query string) (dbx *sqlx.DB, stmt *sqlx.Stmt, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		dbx = db.db
		stmt, err = dbx.PreparexContext(ctx, query)
		return
	}
}

// Preparex creates a prepared statement for later queries or executions on masters.
// But return sqlx.Stmt instead of sql.Stmt.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) Preparex(query string) (db *sqlx.DB, stmt *sqlx.Stmt, err error) {
	return _preparexContext(context.Background(), dbs.masters, query)
}

// PreparexOnSlave creates a prepared statement for later queries or executions on slaves.
// But return sqlx.Stmt instead of sql.Stmt.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PreparexOnSlave(query string) (db *sqlx.DB, stmt *sqlx.Stmt, err error) {
	return _preparexContext(context.Background(), dbs.slaves, query)
}

// PreparexContext creates a prepared statement for later queries or executions on masters.
// But return sqlx.Stmt instead of sql.Stmt.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PreparexContext(ctx context.Context, query string) (db *sqlx.DB, stmt *sqlx.Stmt, err error) {
	return _preparexContext(ctx, dbs.masters, query)
}

// PreparexContextOnSlave creates a prepared statement for later queries or executions on slaves.
// But return sqlx.Stmt instead of sql.Stmt.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (dbs *DBs) PreparexContextOnSlave(ctx context.Context, query string) (db *sqlx.DB, stmt *sqlx.Stmt, err error) {
	return _preparexContext(ctx, dbs.slaves, query)
}

func _prepareNamedContext(ctx context.Context, target *dbBalancer, query string) (dbx *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return nil, nil, ErrNoConnection
	}

	var db *dbLinkListNode

	for {
		if db, err = getDBFromBalancer(target); err != nil {
			return
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		dbx = db.db
		stmt, err = dbx.PrepareNamedContext(ctx, query)
		return
	}
}

// PrepareNamed returns an sqlx.NamedStmt on masters
func (dbs *DBs) PrepareNamed(query string) (db *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	return _prepareNamedContext(context.Background(), dbs.masters, query)
}

// PrepareNamedOnSlave returns an sqlx.NamedStmt on slaves
func (dbs *DBs) PrepareNamedOnSlave(query string) (db *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	return _prepareNamedContext(context.Background(), dbs.slaves, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt on masters
func (dbs *DBs) PrepareNamedContext(ctx context.Context, query string) (db *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	return _prepareNamedContext(ctx, dbs.masters, query)
}

// PrepareNamedContextOnSlave returns an sqlx.NamedStmt on slaves
func (dbs *DBs) PrepareNamedContextOnSlave(ctx context.Context, query string) (db *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	return _prepareNamedContext(ctx, dbs.slaves, query)
}

func _mustExec(ctx context.Context, target *dbBalancer, query string, args ...interface{}) (res sql.Result) {
	if target == nil {
		panic(ErrNoConnection)
	}

	var db *dbLinkListNode
	var err error
	for {
		if db, err = getDBFromBalancer(target); err != nil {
			panic(err)
		}

		if db.db == nil {
			target.failure(db)
			continue
		} else {
			// try to ping db first. Tradeoff a little performance for auto-reset db connection when DBMS restarted
			ping(db.db)
		}

		res = db.db.MustExecContext(ctx, query, args...)
		return
	}
}

// MustExec do exec on masters and panic on error
func (dbs *DBs) MustExec(query string, args ...interface{}) sql.Result {
	return _mustExec(context.Background(), dbs.masters, query, args...)
}

// MustExecOnSlave do exec on slave only and panic on error
func (dbs *DBs) MustExecOnSlave(query string, args ...interface{}) sql.Result {
	return _mustExec(context.Background(), dbs.slaves, query, args...)
}

// MustExecContext do exec on masters and panic on error
func (dbs *DBs) MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result {
	return _mustExec(ctx, dbs.masters, query, args...)
}

// MustExecContextOnSlave do exec on slave only and panic on error
func (dbs *DBs) MustExecContextOnSlave(ctx context.Context, query string, args ...interface{}) sql.Result {
	return _mustExec(ctx, dbs.slaves, query, args...)
}

// MustBegin starts a transaction, and panics on error.
// Transaction is bound to one of master connections.
func (dbs *DBs) MustBegin() *sql.Tx {
	tx, err := dbs.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginx starts a transaction, and panics on error.
// Returns an *sqlx.Tx instead of an *sql.Tx.
// Transaction is bound to one of master connections.
func (dbs *DBs) MustBeginx() *sqlx.Tx {
	tx, err := dbs.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginTx starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// MustBeginContext is canceled.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx {
	tx, err := dbs.BeginTxx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) Begin() (*sql.Tx, error) {
	return dbs.BeginTx(context.Background(), nil)
}

// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	for {
		db, err := getDBFromBalancer(dbs.masters)
		if err != nil {
			return nil, err
		}

		if db.db == nil {
			dbs.masters.failure(db)
			continue
		}

		return db.db.BeginTx(ctx, opts)
	}
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) Beginx() (*sqlx.Tx, error) {
	for {
		db, err := getDBFromBalancer(dbs.masters)
		if err != nil {
			return nil, err
		}

		if db.db == nil {
			dbs.masters.failure(db)
			continue
		}

		return db.db.Beginx()
	}
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	for {
		db, err := getDBFromBalancer(dbs.masters)
		if err != nil {
			return nil, err
		}

		if db.db == nil {
			dbs.masters.failure(db)
			continue
		}

		return db.db.BeginTxx(ctx, opts)
	}
}

// ConnectMasterSlaves to master-slave databases and verify with pings.
// driverName: mysql, postgres, etc.
// masterDSNs: data source names of Masters.
// slaveDSNs: data source names of Slaves.
// args: args[0] = true to indicates galera/wsrep cluster.
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
		_masters:   make([]*sqlx.DB, nMaster),
		slaves:     &dbBalancer{},
		_slaves:    make([]*sqlx.DB, nSlave),
		all:        &dbBalancer{},
		_all:       make([]*sqlx.DB, nMaster+nSlave),
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
			dbs._masters[mId], errResult[eId] = sqlx.Connect(driverName, masterDSNs[mId])
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
			dbs._slaves[sId], errResult[eId] = sqlx.Connect(driverName, slaveDSNs[sId])
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
