package mssqlx

import (
	"context"
	"database/sql"
)

// Tx wraps over sql.Tx
type Tx struct {
	*sql.Tx
}

// Commit commits the transaction.
func (t *Tx) Commit() (err error) {
	_, err = retryFunc("tx_commit", func() (interface{}, error) {
		return nil, t.Tx.Commit()
	})
	return
}

// Exec executes a query that doesn't return rows. For example: an INSERT and UPDATE.
func (t *Tx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	return t.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query that doesn't return rows. For example: an INSERT and UPDATE.
func (t *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	r, err := retryFunc("tx_exec", func() (interface{}, error) {
		return t.Tx.ExecContext(ctx, query, args...)
	})
	if err == nil {
		result = r.(sql.Result)
	}
	return
}

// Prepare creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed when the transaction has been committed or rolled back.
func (t *Tx) Prepare(query string) (*Stmt, error) {
	return t.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed when the transaction has been committed or rolled back.
//
// The provided context will be used for the preparation of the context, not for the execution of the returned statement. The returned statement will run in the transaction context.
func (t *Tx) PrepareContext(ctx context.Context, query string) (result *Stmt, err error) {
	r, err := retryFunc("tx_prepare", func() (interface{}, error) {
		return t.Tx.PrepareContext(ctx, query)
	})
	if err == nil {
		result = &Stmt{
			Stmt: r.(*sql.Stmt),
		}
	}
	return
}

// Query executes a query that returns rows, typically a SELECT.
func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (t *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (result *sql.Rows, err error) {
	r, err := retryFunc("tx_query", func() (interface{}, error) {
		return t.Tx.QueryContext(ctx, query)
	})
	if err == nil {
		result = r.(*sql.Rows)
	}
	return
}
