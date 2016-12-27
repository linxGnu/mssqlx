package mssqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	// ErrNoRecord no record found
	ErrNoRecord = sql.ErrNoRows

	// ErrNetwork networking error
	ErrNetwork = errors.New("Network error/Connection refused")

	// ErrNoConnection there is no connection to db
	ErrNoConnection = errors.New("No connection available")
)

func parseError(err error) error {
	if _, ok := err.(net.Error); ok {
		return ErrNetwork
	}

	if err == sql.ErrNoRows {
		return ErrNoRecord
	}

	return err
}

// dbLinkListNode a node of linked-list contains sqlx.DB
type dbLinkListNode struct {
	db   *DB
	next *dbLinkListNode
	prev *dbLinkListNode
}

// dbLinkList a round robin and thread-safe linked-list of sqlx.DB
type dbLinkList struct {
	// head and tail of linked-list
	head *dbLinkListNode
	tail *dbLinkListNode

	// size of this linked list
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
	numberOfHealthChecker int
}

// init balancer and start health checkers
func (c *dbBalancer) init(numHealthChecker int, numDbInstance int) {
	if numHealthChecker <= 0 {
		numHealthChecker = 1
	}

	c.numberOfHealthChecker = numHealthChecker
	c.dbs = &dbLinkList{}
	c.fail = make(chan *DB, numDbInstance)

	for i := 0; i < numHealthChecker; i++ {
		go c.healthChecker()
	}
}

// add a db connection to handle in balancer
func (c *dbBalancer) add(db *DB) {
	c.dbs.add(&dbLinkListNode{db: db})
}

// get a db to handle our query
func (c *dbBalancer) get() *dbLinkListNode {
	return c.dbs.moveNext()
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

// healthChecker daemon to check health of db connection
func (c *dbBalancer) healthChecker() {
	defer func() {
		if e := recover(); e != nil {
		}
	}()

	for db := range c.fail {
		if db == nil {
			continue
		}

		if err := db.Ping(); err == nil {
			c.dbs.add(&dbLinkListNode{db: db})
			continue
		}

		c.fail <- db
		time.Sleep(50 * time.Millisecond)
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

// GetMasterDB get all master database instances
func (dbs *DBs) GetMasterDB() *DB {
	if dbs._masters == nil || len(dbs._masters) == 0 {
		return nil
	}

	return dbs._masters[0]
}

// GetSlaveDBs get all slave database instances
func (dbs *DBs) GetSlaveDBs() []*DB {
	return dbs._slaves
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

	c := make(chan byte, nn)
	for i := range target {
		go func(ind int) {
			if target[ind] != nil {
				errResult[ind] = target[ind].Ping()
			}
			c <- 0
		}(i)
	}

	for i := 0; i < nn; i++ {
		<-c
	}

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

	c := make(chan byte, nn)
	for i, db := range target {
		go func(db *DB, ind int) {
			if db != nil {
				errResult[ind] = db.Close()
			}
			c <- 0
		}(db, i)
	}

	for i := 0; i < nn; i++ {
		<-c
	}

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

	c := make(chan byte, nn)
	for _, db := range target {
		go func(db *DB) {
			if db != nil {
				db.SetMaxIdleConns(n)
			}
			c <- 0
		}(db)
	}

	for i := 0; i < nn; i++ {
		<-c
	}
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

	c := make(chan byte, nn)
	for _, db := range target {
		go func(db *DB) {
			if db != nil {
				db.SetMaxOpenConns(n)
			}
			c <- 0
		}(db)
	}

	for i := 0; i < nn; i++ {
		<-c
	}
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

	c := make(chan byte, nn)
	for _, db := range target {
		go func(db *DB) {
			if db != nil {
				db.SetConnMaxLifetime(d)
			}
			c <- 0
		}(db)
	}

	for i := 0; i < nn; i++ {
		<-c
	}
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

	c := make(chan byte, nn)
	for ind, db := range target {
		go func(db *DB, ind int) {
			if db != nil {
				result[ind] = db.Stats()
				c <- 0
			}
		}(db, ind)
	}

	for i := 0; i < nn; i++ {
		<-c
	}

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

	c := make(chan byte, nn)
	for ind, db := range target {
		go func(db *DB, ind int) {
			if db != nil {
				db.MapperFunc(mf)
				c <- 0
			}
		}(db, ind)
	}

	for i := 0; i < nn; i++ {
		<-c
	}
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

	for {
		db := target.get()
		if db == nil {
			return nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.NamedQuery(query, arg)
		if e = parseError(e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		return r, e
	}
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.all, query, arg)
}

// NamedQueryOnMaster using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryOnMaster(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.masters, query, arg)
}

// NamedQueryOnSlave using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryOnSlave(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.slaves, query, arg)
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

	for {
		db := target.get()
		if db == nil {
			return nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.NamedExec(query, arg)
		if e = parseError(e); e == ErrNetwork {
			target.failure(db)
			continue
		}

		return r, e
	}
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.all, query, arg)
}

// NamedExecOnMaster using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecOnMaster(query string, arg interface{}) (sql.Result, error) {
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

	for {
		db := target.get()
		if db == nil {
			return nil, nil, ErrNoConnection
		}

		if db.db == nil {
			target.failure(db)
			continue
		}

		r, e := db.db.Query(query, args...)
		if e = parseError(e); e == ErrNetwork {
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
	return _queryx(dbs.all, query, args...)
}

// QueryxOnMaster queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryxOnMaster(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.masters, query, args...)
}

// QueryxOnSlave queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryxOnSlave(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.slaves, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRow(query string, args ...interface{}) *Row {
	_, rows, err := _query(dbs.all, query, args...)
	return &Row{rows: rows, err: err}
}

// QueryRowOnMaster executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowOnMaster(query string, args ...interface{}) *Row {
	_, rows, err := _query(dbs.masters, query, args...)
	return &Row{rows: rows, err: err}
}

// QueryRowOnSlave executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowOnSlave(query string, args ...interface{}) *Row {
	_, rows, err := _query(dbs.slaves, query, args...)
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
	return _select(dbs.all, dest, query, args...)
}

// SelectOnMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectOnMaster(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.masters, dest, query, args...)
}

// SelectOnSlave using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectOnSlave(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.slaves, dest, query, args...)
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
	return _queryRowx(dbs.all, query, args...)
}

// QueryRowxOnMaster queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowxOnMaster(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.masters, query, args...)
}

// QueryRowxOnSlave queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowxOnSlave(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.slaves, query, args...)
}

func _get(target *dbBalancer, dest interface{}, query string, args ...interface{}) error {
	r := _queryRowx(target, query, args...)
	return r.scanAny(dest, false)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) Get(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.all, dest, query, args...)
}

// GetOnMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetOnMaster(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.masters, dest, query, args...)
}

// GetOnSlave using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetOnSlave(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.slaves, dest, query, args...)
}

// ConnectMasterSlaves to master-slave databases and verify with pings
//
// masterDSN: data source names of Masters
// slaveDSNs: data source names of Slaves
func ConnectMasterSlaves(driverName string, masterDSN string, slaveDSNs []string) (*DBs, []error) {
	// Validate slave address
	if slaveDSNs == nil {
		slaveDSNs = []string{}
	}

	nSlave := len(slaveDSNs)
	errResult := make([]error, 1+nSlave)

	dbs := &DBs{
		driverName: driverName,
		masters:    &dbBalancer{},
		_masters:   make([]*DB, 1),
		slaves:     &dbBalancer{},
		_slaves:    make([]*DB, nSlave),
		all:        &dbBalancer{},
		_all:       make([]*DB, 1+nSlave),
	}
	dbs.masters.init(1, 1)
	dbs.slaves.init((nSlave<<1)/10, nSlave)  // 20%
	dbs.all.init((1+nSlave)<<1/10, 1+nSlave) // 20%

	// channel to sync routines
	c := make(chan byte, len(errResult))

	// Concurrency connect to master
	go func(mId, eId int) {
		dbs._masters[mId], errResult[eId] = Connect(driverName, masterDSN)
		dbs.masters.add(dbs._masters[mId])

		dbs._all[eId] = dbs._masters[mId]
		dbs.all.add(dbs._masters[mId])

		c <- 0
	}(0, 0)

	// number of db connections
	n := 1

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
