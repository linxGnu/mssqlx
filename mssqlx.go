package mssqlx

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	sqldblogger "github.com/simukti/sqldb-logger"
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

var hostName string

func init() {
	hostName, _ = os.Hostname()
}

func ping(w *wrapper) (err error) {
	_, err = w.db.Exec("SELECT 1")
	return
}

// DBs sqlx wrapper supports querying master-slave database connections for HA and scalability, auto-balancer integrated.
type DBs struct {
	driverName      string
	readQuerySource ReadQuerySource

	masters *balancer
	slaves  *balancer
	all     *balancer

	_masters []*wrapper
	_slaves  []*wrapper
	_all     []*wrapper
}

// DriverName returns the driverName passed to the Open function for this DB.
func (dbs *DBs) DriverName() string {
	return dbs.driverName
}

func (dbs *DBs) getDBs(s []*wrapper) ([]*sqlx.DB, int) {
	n := len(s)
	r := make([]*sqlx.DB, n)
	for i, v := range s {
		r[i] = v.db
	}
	return r, n
}

func (dbs *DBs) readBalancer() *balancer {
	if dbs.readQuerySource == ReadQuerySourceAll {
		return dbs.all
	}
	return dbs.slaves
}

// GetAllMasters get all master database connections, included failing one.
func (dbs *DBs) GetAllMasters() ([]*sqlx.DB, int) {
	return dbs.getDBs(dbs._masters)
}

// GetAllSlaves get all slave database connections, included failing one.
func (dbs *DBs) GetAllSlaves() ([]*sqlx.DB, int) {
	return dbs.getDBs(dbs._slaves)
}

func _ping(target []*wrapper) []error {
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
		if target[i] != nil && target[i].db != nil {
			wg.Add(1)
			go func(ind int, wg *sync.WaitGroup) {
				errResult[ind] = target[ind].db.Ping()
				wg.Done()
			}(i, &wg)
		}
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

func _close(target []*wrapper) []error {
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
		if db != nil && db.db != nil {
			wg.Add(1)
			go func(db *wrapper, ind int, wg *sync.WaitGroup) {
				errResult[ind] = db.db.Close()
				wg.Done()
			}(db, i, &wg)
		}
	}
	wg.Wait()

	return errResult
}

// Destroy closes all database connections, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (dbs *DBs) Destroy() []error {
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
	if dbs.slaves != nil {
		dbs.slaves.destroy()
	}

	return _close(dbs._slaves)
}

func _setMaxIdleConns(target []*wrapper, n int) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		if db != nil && db.db != nil {
			wg.Add(1)
			go func(db *wrapper, wg *sync.WaitGroup) {
				db.db.SetMaxIdleConns(n)
				wg.Done()
			}(db, &wg)
		}
	}
	wg.Wait()
}

// SetHealthCheckPeriod sets the period (in millisecond) for checking health of failed nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetHealthCheckPeriod(period uint64) {
	dbs.masters.setHealthCheckPeriod(period)
	dbs.slaves.setHealthCheckPeriod(period)
}

// SetMasterHealthCheckPeriod sets the period (in millisecond) for checking health of failed master nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetMasterHealthCheckPeriod(period uint64) {
	dbs.masters.setHealthCheckPeriod(period)
}

// SetSlaveHealthCheckPeriod sets the period (in millisecond) for checking health of failed slave nodes
// for automatic recovery.
//
// Default is 500
func (dbs *DBs) SetSlaveHealthCheckPeriod(period uint64) {
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
	_setMaxIdleConns(dbs._slaves, n)
}

func _setMaxOpenConns(target []*wrapper, n int) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		if db != nil && db.db != nil {
			wg.Add(1)
			go func(db *wrapper, wg *sync.WaitGroup) {
				db.db.SetMaxOpenConns(n)
				wg.Done()
			}(db, &wg)
		}
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
	_setMaxOpenConns(dbs._slaves, n)
}

func _setConnMaxLifetime(target []*wrapper, d time.Duration) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, db := range target {
		if db != nil && db.db != nil {
			wg.Add(1)
			go func(db *wrapper, wg *sync.WaitGroup) {
				db.db.SetConnMaxLifetime(d)
				wg.Done()
			}(db, &wg)
		}
	}
	wg.Wait()
}

// SetConnMaxLifetime sets the maximum amount of time a master-slave connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetConnMaxLifetime(d time.Duration) {
	_setConnMaxLifetime(dbs._all, d)
}

// SetMasterConnMaxLifetime sets the maximum amount of time a master connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetMasterConnMaxLifetime(d time.Duration) {
	_setConnMaxLifetime(dbs._masters, d)
}

// SetSlaveConnMaxLifetime sets the maximum amount of time a slave connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (dbs *DBs) SetSlaveConnMaxLifetime(d time.Duration) {
	_setConnMaxLifetime(dbs._slaves, d)
}

func _stats(target []*wrapper) []sql.DBStats {
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
		if db != nil && db.db != nil {
			wg.Add(1)
			go func(db *wrapper, ind int, wg *sync.WaitGroup) {
				result[ind] = db.db.Stats()
				wg.Done()
			}(db, ind, &wg)
		}
	}
	wg.Wait()

	return result
}

// Stats returns database statistics.
func (dbs *DBs) Stats() (stats []sql.DBStats) {
	stats = _stats(dbs._all)
	return
}

// StatsMaster returns master database statistics.
func (dbs *DBs) StatsMaster() (stats []sql.DBStats) {
	stats = _stats(dbs._masters)
	return
}

// StatsSlave returns slave database statistics.
func (dbs *DBs) StatsSlave() (stats []sql.DBStats) {
	stats = _stats(dbs._slaves)
	return
}

func _mapperFunc(target []*wrapper, mf func(string) string) {
	if target == nil {
		return
	}

	nn := len(target)
	if nn == 0 {
		return
	}

	var wg sync.WaitGroup
	for ind, db := range target {
		if db != nil {
			wg.Add(1)
			go func(db *wrapper, ind int) {
				db.db.MapperFunc(mf)
				wg.Done()
			}(db, ind)
		}
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
		if db != nil && db.db != nil {
			return db.db.Rebind(query)
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
			return db.db.BindNamed(query, arg)
		}
	}

	return "", nil, ErrNoConnection
}

func getDBFromBalancer(target *balancer) (db *wrapper, err error) {
	if db = target.get(target.isMulti); db != nil {
		return
	}

	// retry if there is no connection available. This event could happen when database closes all non-interactive connection.
	for i := 0; i < 3; i++ {
		time.Sleep(time.Duration(target.getHealthCheckPeriod()) * time.Millisecond)
		if db = target.get(target.isMulti); db != nil {
			return
		}
	}

	// need to return error
	if target.isWsrep {
		err = ErrNoConnectionOrWsrep
	} else {
		err = ErrNoConnection
	}

	return
}

func shouldFailure(w *wrapper, isWsrep bool, err error) bool {
	if err = parseError(w, err); err == nil {
		return false
	}

	if err == ErrNetwork || (isWsrep && IsWsrepNotReady(err)) {
		return true
	}

	return false
}

func _namedQuery(ctx context.Context, target *balancer, query string, arg interface{}) (res *sqlx.Rows, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.NamedQueryContext(ctx, query, arg)
		})
		if r != nil {
			res = r.(*sqlx.Rows)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		return
	}
}

// NamedQuery do named query.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(context.Background(), dbs.readBalancer(), query, arg)
}

// NamedQueryOnMaster do named query on master.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryOnMaster(query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(context.Background(), dbs.masters, query, arg)
}

// NamedQueryContext do named query with context.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(ctx, dbs.readBalancer(), query, arg)
}

// NamedQueryContextOnMaster do named query with context on master.
// Any named placeholder parameters are replaced with fields from arg.
func (dbs *DBs) NamedQueryContextOnMaster(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	return _namedQuery(ctx, dbs.masters, query, arg)
}

func _namedExec(ctx context.Context, target *balancer, query string, arg interface{}) (res sql.Result, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.NamedExecContext(ctx, query, arg)
		})
		if r != nil {
			res = r.(sql.Result)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
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

func _query(ctx context.Context, target *balancer, query string, args ...interface{}) (dbr *wrapper, res *sql.Rows, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.QueryContext(ctx, query, args...)
		})
		if r != nil {
			res = r.(*sql.Rows)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbr = w
		return
	}
}

// Query executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) Query(query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(context.Background(), dbs.readBalancer(), query, args...)
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
	_, r, err = _query(ctx, dbs.readBalancer(), query, args...)
	return
}

// QueryContextOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sql.Rows, err error) {
	_, r, err = _query(ctx, dbs.masters, query, args...)
	return
}

func _queryx(ctx context.Context, target *balancer, query string, args ...interface{}) (dbr *wrapper, res *sqlx.Rows, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.QueryxContext(ctx, query, args...)
		})
		if r != nil {
			res = r.(*sqlx.Rows)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbr = w
		return
	}
}

// Queryx executes a query on slaves that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) Queryx(query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(context.Background(), dbs.readBalancer(), query, args...)
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
	_, r, err = _queryx(ctx, dbs.readBalancer(), query, args...)
	return
}

// QueryxContextOnMaster executes a query on masters that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (dbs *DBs) QueryxContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sqlx.Rows, err error) {
	_, r, err = _queryx(ctx, dbs.masters, query, args...)
	return
}

func _queryRow(ctx context.Context, target *balancer, query string, args ...interface{}) (dbr *wrapper, res *sql.Row, err error) {
	var w *wrapper

	if w, err = getDBFromBalancer(target); err != nil {
		reportError(query, err)
	} else {
		res, dbr = w.db.QueryRowContext(ctx, query, args...), w
	}

	return
}

// QueryRow executes a query on slaves that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRow(query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(context.Background(), dbs.readBalancer(), query, args...)
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
	_, r, err = _queryRow(ctx, dbs.readBalancer(), query, args...)
	return
}

// QueryRowContextOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sql.Row, err error) {
	_, r, err = _queryRow(ctx, dbs.masters, query, args...)
	return
}

func _queryRowx(ctx context.Context, target *balancer, query string, args ...interface{}) (dbr *wrapper, res *sqlx.Row, err error) {
	var w *wrapper

	if w, err = getDBFromBalancer(target); err != nil {
		reportError(query, err)
	} else {
		res, dbr = w.db.QueryRowxContext(ctx, query, args...), w
	}

	return
}

// QueryRowx executes a query on slaves that is expected to return at most one row.
// But return sqlx.Row instead of sql.Row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowx(query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(context.Background(), dbs.readBalancer(), query, args...)
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
	_, r, err = _queryRowx(ctx, dbs.readBalancer(), query, args...)
	return
}

// QueryRowxContextOnMaster executes a query on masters that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (dbs *DBs) QueryRowxContextOnMaster(ctx context.Context, query string, args ...interface{}) (r *sqlx.Row, err error) {
	_, r, err = _queryRowx(ctx, dbs.masters, query, args...)
	return
}

func _select(ctx context.Context, target *balancer, dest interface{}, query string, args ...interface{}) (dbr *wrapper, err error) {
	var w *wrapper

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		_, err = retryFunc(query, func() (interface{}, error) {
			return nil, w.db.SelectContext(ctx, dest, query, args...)
		})

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbr = w
		return
	}
}

// Select do select on slaves.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) Select(dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(context.Background(), dbs.readBalancer(), dest, query, args...)
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
	_, err = _select(ctx, dbs.readBalancer(), dest, query, args...)
	return
}

// SelectContextOnMaster do select on masters with context.
// Any placeholder parameters are replaced with supplied args.
func (dbs *DBs) SelectContextOnMaster(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _select(ctx, dbs.masters, dest, query, args...)
	return
}

func _get(ctx context.Context, target *balancer, dest interface{}, query string, args ...interface{}) (dbr *wrapper, err error) {
	var w *wrapper

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		_, err = retryFunc(query, func() (interface{}, error) {
			return nil, w.db.GetContext(ctx, dest, query, args...)
		})

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbr = w
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
	_, err = _get(ctx, dbs.readBalancer(), dest, query, args...)
	return
}

// GetContextOnMaster on masters.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (dbs *DBs) GetContextOnMaster(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	_, err = _get(ctx, dbs.masters, dest, query, args...)
	return
}

func _exec(ctx context.Context, target *balancer, query string, args ...interface{}) (res sql.Result, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.ExecContext(ctx, query, args...)
		})
		if r != nil {
			res = r.(sql.Result)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
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

func _prepareContext(ctx context.Context, target *balancer, query string) (dbx *sqlx.DB, stmt *sql.Stmt, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.PrepareContext(ctx, query)
		})
		if r != nil {
			stmt = r.(*sql.Stmt)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbx = w.db
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

func _preparexContext(ctx context.Context, target *balancer, query string) (dbx *sqlx.DB, stmt *sqlx.Stmt, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.PreparexContext(ctx, query)
		})
		if r != nil {
			stmt = r.(*sqlx.Stmt)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbx = w.db
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

func _prepareNamedContext(ctx context.Context, target *balancer, query string) (dbx *sqlx.DB, stmt *sqlx.NamedStmt, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			reportError(query, err)
			return
		}

		// executing
		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.PrepareNamedContext(ctx, query)
		})
		if r != nil {
			stmt = r.(*sqlx.NamedStmt)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		dbx = w.db
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

func _mustExec(ctx context.Context, target *balancer, query string, args ...interface{}) (res sql.Result) {
	var (
		w   *wrapper
		err error
		r   interface{}
	)

	for {
		if w, err = getDBFromBalancer(target); err != nil {
			panic(err)
		}

		r, err = retryFunc(query, func() (interface{}, error) {
			return w.db.ExecContext(ctx, query, args...)
		})
		if r != nil {
			res = r.(sql.Result)
		}

		// check networking/wsrep error
		if shouldFailure(w, target.isWsrep, err) {
			target.failure(w)
			continue
		}

		if err != nil {
			panic(err)
		}
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
func (dbs *DBs) MustBegin() *Tx {
	tx, err := dbs.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginx starts a transaction, and panics on error.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) MustBeginx() *Txx {
	tx, err := dbs.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginTx starts a transaction, and panics on error.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// MustBeginContext is canceled.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Txx {
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
func (dbs *DBs) Begin() (*Tx, error) {
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
func (dbs *DBs) BeginTx(ctx context.Context, opts *sql.TxOptions) (res *Tx, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(dbs.masters); err != nil {
			reportError("BeginTx", err)
			return nil, err
		}

		// executing
		r, err = retryFunc("START TRANSACTION", func() (interface{}, error) {
			return w.db.BeginTx(ctx, opts)
		})
		if r != nil {
			res = &Tx{
				Tx: r.(*sql.Tx),
			}
		}

		// check networking/wsrep error
		if shouldFailure(w, dbs.masters.isWsrep, err) {
			dbs.masters.failure(w)
			continue
		}

		return
	}
}

// Beginx begins a transaction.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) Beginx() (res *Txx, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(dbs.masters); err != nil {
			reportError("Beginx", err)
			return nil, err
		}

		// executing
		r, err = retryFunc("START TRANSACTION", func() (interface{}, error) {
			return w.db.Beginx()
		})
		if r != nil {
			res = &Txx{
				Tx: r.(*sqlx.Tx),
			}
		}

		// check networking/wsrep error
		if shouldFailure(w, dbs.masters.isWsrep, err) {
			dbs.masters.failure(w)
			continue
		}

		return
	}
}

// BeginTxx begins a transaction.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
//
// Transaction is bound to one of master connections.
func (dbs *DBs) BeginTxx(ctx context.Context, opts *sql.TxOptions) (res *Txx, err error) {
	var (
		w *wrapper
		r interface{}
	)

	for {
		if w, err = getDBFromBalancer(dbs.masters); err != nil {
			reportError("BeginTxx", err)
			return nil, err
		}

		// executing
		r, err = retryFunc("START TRANSACTION", func() (interface{}, error) {
			return w.db.BeginTxx(ctx, opts)
		})
		if r != nil {
			res = &Txx{
				Tx: r.(*sqlx.Tx),
			}
		}

		// check networking/wsrep error
		if shouldFailure(w, dbs.masters.isWsrep, err) {
			dbs.masters.failure(w)
			continue
		}

		return
	}
}

// ConnectMasterSlaves to master-slave databases, healthchecks will ensure they are working
// driverName: mysql, postgres, etc.
// masterDSNs: data source names of Masters.
// slaveDSNs: data source names of ReadQuerySourceSlaves.
func ConnectMasterSlaves(driverName string, masterDSNs []string, slaveDSNs []string, options ...Option) (*DBs, []error) {
	// Validate slave address
	if slaveDSNs == nil {
		slaveDSNs = []string{}
	}

	if masterDSNs == nil {
		masterDSNs = []string{}
	}

	// default cluster options
	opts := &clusterOptions{
		isWsrep:         false,
		readQuerySource: ReadQuerySourceAll,
	}

	for _, optFn := range options {
		optFn(opts)
	}

	nMaster := len(masterDSNs)
	nSlave := len(slaveDSNs)
	nAll := nMaster + nSlave

	errResult := make([]error, nAll)
	dbs := &DBs{
		driverName:      driverName,
		readQuerySource: opts.readQuerySource,

		masters:  newBalancer(context.Background(), nMaster>>2, nMaster, opts.isWsrep),
		_masters: make([]*wrapper, nMaster),

		slaves:  newBalancer(context.Background(), nSlave>>2, nSlave, opts.isWsrep),
		_slaves: make([]*wrapper, nSlave),

		all:  newBalancer(context.Background(), nAll>>2, nAll, opts.isWsrep),
		_all: make([]*wrapper, nAll),
	}

	// channel to sync routines
	c := make(chan byte, len(errResult))

	// Concurrency connect to master
	n := 0
	for i := range masterDSNs {
		go func(mId, eId int) {
			dbConn, err := sqlx.Open(driverName, masterDSNs[mId])

			// set logger if possible
			if opts.logger != nil {
				db := sqldblogger.OpenDriver(
					masterDSNs[mId],
					dbConn.Driver(),
					opts.logger,
					opts.loggerOpts...,
				)
				dbConn = sqlx.NewDb(db, driverName)
			}

			dbs._masters[mId], errResult[eId] = &wrapper{db: dbConn, dsn: masterDSNs[mId]}, err
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
			dbConn, err := sqlx.Open(driverName, slaveDSNs[sId])
			dbs._slaves[sId], errResult[eId] = &wrapper{db: dbConn, dsn: slaveDSNs[sId]}, err
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
