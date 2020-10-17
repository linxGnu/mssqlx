package mssqlx

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Txx wraps std sqlx.Tx
type Txx struct {
	*sqlx.Tx
}

// Commit commits the transaction.
func (t *Txx) Commit() (err error) {
	_, err = retryFunc("tx_commit", func() (interface{}, error) {
		return nil, t.Tx.Commit()
	})
	return
}

// Exec executes a query that doesn't return rows. For example: an INSERT and UPDATE.
func (t *Txx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	return t.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query that doesn't return rows. For example: an INSERT and UPDATE.
func (t *Txx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
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
func (t *Txx) Prepare(query string) (*Stmt, error) {
	return t.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed when the transaction has been committed or rolled back.
//
// The provided context will be used for the preparation of the context, not for the execution of the returned statement. The returned statement will run in the transaction context.
func (t *Txx) PrepareContext(ctx context.Context, query string) (result *Stmt, err error) {
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

// Preparex creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed when the transaction has been committed or rolled back.
func (t *Txx) Preparex(query string) (*Stmtx, error) {
	return t.PreparexContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed when the transaction has been committed or rolled back.
//
// The provided context will be used for the preparation of the context, not for the execution of the returned statement. The returned statement will run in the transaction context.
func (t *Txx) PreparexContext(ctx context.Context, query string) (result *Stmtx, err error) {
	r, err := retryFunc("tx_prepare", func() (interface{}, error) {
		return t.Tx.PreparexContext(ctx, query)
	})
	if err == nil {
		result = &Stmtx{
			Stmt: r.(*sqlx.Stmt),
		}
	}
	return
}

// Query executes a query that returns rows, typically a SELECT.
func (t *Txx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (t *Txx) QueryContext(ctx context.Context, query string, args ...interface{}) (result *sql.Rows, err error) {
	r, err := retryFunc("tx_query", func() (interface{}, error) {
		return t.Tx.QueryContext(ctx, query)
	})
	if err == nil {
		result = r.(*sql.Rows)
	}
	return
}

// Queryx executes a query that returns rows, typically a SELECT.
func (t *Txx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return t.QueryxContext(context.Background(), query, args...)
}

// QueryxContext executes a query that returns rows, typically a SELECT.
func (t *Txx) QueryxContext(ctx context.Context, query string, args ...interface{}) (result *sqlx.Rows, err error) {
	r, err := retryFunc("tx_query", func() (interface{}, error) {
		return t.Tx.QueryxContext(ctx, query)
	})
	if err == nil {
		result = r.(*sqlx.Rows)
	}
	return
}
