package mssqlx

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// DBs sqlx wrapper supports querying master-slave database instances for HA and scalability
type DBs struct {
	driverName string

	// master and slave database instances
	masters []*DB
	slaves  []*DB

	// master and slave lock
	masterLock sync.RWMutex
	slaveLock  sync.RWMutex

	// store all database instances
	all []*DB
}

// DriverName returns the driverName passed to the Open function for this DB.
func (dbs *DBs) DriverName() string {
	return dbs.driverName
}

// GetMasterDBs get all master database instances
func (dbs *DBs) GetMasterDBs() *DB {
	if dbs.masters == nil || len(dbs.masters) == 0 {
		return nil
	}

	return dbs.masters[0]
}

// GetSlaveDBs get all slave database instances
func (dbs *DBs) GetSlaveDBs() []*DB {
	return dbs.slaves
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
	return _ping(dbs.all)
}

// PingMaster all master database instances
func (dbs *DBs) PingMaster() []error {
	return _ping(dbs.masters)
}

// PingSlave all slave database instances
func (dbs *DBs) PingSlave() []error {
	return _ping(dbs.slaves)
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

// Close closes all database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) Close() []error {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	return _close(dbs.all)
}

// CloseMaster closes all master database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) CloseMaster() []error {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	return _close(dbs.masters)
}

// CloseSlave closes all master database instances, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) CloseSlave() []error {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	return _close(dbs.slaves)
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

	_setMaxIdleConns(dbs.all, n)
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

	_setMaxIdleConns(dbs.masters, n)
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

	_setMaxIdleConns(dbs.slaves, n)
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

	_setMaxOpenConns(dbs.all, n)
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

	_setMaxOpenConns(dbs.masters, n)
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

	_setMaxOpenConns(dbs.slaves, n)
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

	_setConnMaxLifetime(dbs.all, d)
}

// SetMasterConnMaxLifetime sets the maximum amount of time a master connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetMasterConnMaxLifetime(d time.Duration) {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	_setConnMaxLifetime(dbs.masters, d)
}

// SetSlaveConnMaxLifetime sets the maximum amount of time a slave connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetSlaveConnMaxLifetime(d time.Duration) {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	_setConnMaxLifetime(dbs.slaves, d)
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

	return _stats(dbs.all)
}

// StatsMaster returns master database statistics.
func (dbs *DBs) StatsMaster() []sql.DBStats {
	dbs.masterLock.Lock()
	defer dbs.masterLock.Unlock()

	return _stats(dbs.masters)
}

// StatsSlave returns slave database statistics.
func (dbs *DBs) StatsSlave() []sql.DBStats {
	dbs.slaveLock.Lock()
	defer dbs.slaveLock.Unlock()

	return _stats(dbs.slaves)
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
	_mapperFunc(dbs.all, mf)
}

// MapperFuncMaster sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (dbs *DBs) MapperFuncMaster(mf func(string) string) {
	_mapperFunc(dbs.masters, mf)
}

// MapperFuncSlave sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (dbs *DBs) MapperFuncSlave(mf func(string) string) {
	_mapperFunc(dbs.slaves, mf)
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (dbs *DBs) Rebind(query string) string {
	for _, db := range dbs.all {
		if db != nil {
			return db.Rebind(query)
		}
	}

	return ""
}

// BindNamed binds a query using the DB driver's bindvar type.
func (dbs *DBs) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	for _, db := range dbs.all {
		if db != nil {
			return db.BindNamed(query, arg)
		}
	}

	return "", nil, nil
}

func _namedQuery(target []*DB, query string, arg interface{}) (res *Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, fmt.Errorf("Database connections is not initialized")
	}

	nn := len(target)
	if nn == 0 {
		return nil, fmt.Errorf("Database connections is not initialized")
	}

	type x struct {
		rows *Rows
		err  error
	}

	c := make(chan *x, nn)
	for _, db := range target {
		go func(db *DB, query string, arg interface{}) {
			defer func() {
				if e := recover(); e != nil {
				}
			}()

			if db != nil {
				tmp := &x{}
				tmp.rows, tmp.err = db.NamedQuery(query, arg)

				c <- tmp
				return
			}

			c <- &x{
				err: fmt.Errorf("Database connection not initialized"),
			}
		}(db, query, arg)
	}

	var final *x
	for i := 0; i < nn; i++ {
		if tmp := <-c; tmp.err == nil {
			close(c)

			return tmp.rows, nil
		} else {
			final = tmp
		}
	}

	return final.rows, final.err
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.all, query, arg)
}

// NamedQueryMaster using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryMaster(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.masters, query, arg)
}

// NamedQuerySlave using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuerySlave(query string, arg interface{}) (*Rows, error) {
	return _namedQuery(dbs.slaves, query, arg)
}

func _namedExec(target []*DB, query string, arg interface{}) (res sql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, fmt.Errorf("Database connections is not initialized")
	}

	nn := len(target)
	if nn == 0 {
		return nil, fmt.Errorf("Database connections is not initialized")
	}

	type x struct {
		rows sql.Result
		err  error
	}

	c := make(chan *x, nn)
	for _, db := range target {
		go func(db *DB, query string, arg interface{}) {
			defer func() {
				if e := recover(); e != nil {
				}
			}()

			if db != nil {
				tmp := &x{}
				tmp.rows, tmp.err = db.NamedExec(query, arg)

				c <- tmp
				return
			}

			c <- &x{
				err: fmt.Errorf("Database connection not initialized"),
			}
		}(db, query, arg)
	}

	var final *x
	for i := 0; i < nn; i++ {
		if tmp := <-c; tmp.err == nil {
			close(c)

			return tmp.rows, nil
		} else {
			final = tmp
		}
	}

	return final.rows, final.err
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.all, query, arg)
}

// NamedExecMaster using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecMaster(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.masters, query, arg)
}

// NamedExecSlave using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedExecSlave(query string, arg interface{}) (sql.Result, error) {
	return _namedExec(dbs.slaves, query, arg)
}

func _query(target []*DB, query string, args ...interface{}) (dbr *DB, res *sql.Rows, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			res = nil
		}
	}()

	if target == nil {
		return nil, nil, fmt.Errorf("Database connections is not initialized")
	}

	nn := len(target)
	if nn == 0 {
		return nil, nil, fmt.Errorf("Database connections is not initialized")
	}

	type x struct {
		db   *DB
		rows *sql.Rows
		err  error
	}

	c := make(chan *x, nn)
	for _, db := range target {
		go func(db *DB, query string, args []interface{}) {
			defer func() {
				if e := recover(); e != nil {
				}
			}()

			if db != nil {
				tmp := &x{db: db}
				tmp.rows, tmp.err = db.Query(query, args...)

				c <- tmp
				return
			}

			c <- &x{err: fmt.Errorf("Database connections is not initialized")}
		}(db, query, args)
	}

	var final *x
	for i := 0; i < nn; i++ {
		if tmp := <-c; tmp.err == nil {
			close(c)

			return tmp.db, tmp.rows, tmp.err
		} else {
			final = tmp
		}
	}

	return final.db, final.rows, final.err
}

func _queryx(target []*DB, query string, args ...interface{}) (*Rows, error) {
	db, r, err := _query(target, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Queryx queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) Queryx(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.all, query, args...)
}

// QueryxMaster queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryxMaster(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.masters, query, args...)
}

// QueryxSlave queries the database and returns an *Rows.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryxSlave(query string, args ...interface{}) (*Rows, error) {
	return _queryx(dbs.slaves, query, args...)
}

func _select(target []*DB, dest interface{}, query string, args ...interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	if target == nil {
		return fmt.Errorf("Database connections is not initialized")
	}

	nn := len(target)
	if nn == 0 {
		return fmt.Errorf("Database connections is not initialized")
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

// SelectMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectMaster(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.masters, dest, query, args...)
}

// SelectSlave using this DB.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectSlave(dest interface{}, query string, args ...interface{}) error {
	return _select(dbs.slaves, dest, query, args...)
}

func _queryRowx(target []*DB, que string, args ...interface{}) *Row {
	db, rows, err := _query(target, que, args...)
	return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
}

// QueryRowx queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowx(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.all, query, args...)
}

// QueryRowxMaster queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowxMaster(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.masters, query, args...)
}

// QueryRowxSlave queries the database and returns an *Row.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) QueryRowxSlave(query string, args ...interface{}) *Row {
	return _queryRowx(dbs.slaves, query, args...)
}

func _get(target []*DB, dest interface{}, query string, args ...interface{}) error {
	r := _queryRowx(target, query, args...)
	return r.scanAny(dest, false)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) Get(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.all, dest, query, args...)
}

// GetFromMaster using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetFromMaster(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.masters, dest, query, args...)
}

// GetFromSlave using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetFromSlave(dest interface{}, query string, args ...interface{}) error {
	return _get(dbs.slaves, dest, query, args...)
}

// ConnectMasterSlaves to master-slave databases and verify with pings
//
// masterDSNs: data source names of Masters
// slaveDSNs: data source names of Slaves
func ConnectMasterSlaves(driverName string, masterDSNs string, slaveDSNs []string) (*DBs, []error) {
	// Validate slave address
	if slaveDSNs == nil {
		slaveDSNs = []string{}
	}

	nSlave := len(slaveDSNs)

	errResult := make([]error, 1+nSlave)
	if len(errResult) == 0 {
		return nil, nil
	}

	dbs := &DBs{
		driverName: driverName,
		masters:    make([]*DB, 1),
		slaves:     make([]*DB, nSlave),
		all:        make([]*DB, 1+nSlave),
	}

	// channel to sync routines
	c := make(chan byte, len(errResult))
	n := 0

	// Concurrency connect to masters
	go func(mId, eId int) {
		dbs.masters[mId], errResult[eId] = Connect(driverName, masterDSNs)
		dbs.all[eId] = dbs.masters[mId]
		c <- 0
	}(0, n)
	n++

	// Concurrency connect to slaves
	for i := range slaveDSNs {
		go func(sId, eId int) {
			dbs.slaves[sId], errResult[eId] = Connect(driverName, slaveDSNs[sId])
			dbs.all[eId] = dbs.slaves[sId]
			c <- 0
		}(i, n)
		n++
	}

	for i := 0; i < len(errResult); i++ {
		<-c
	}

	return dbs, errResult
}
