package mssqlx

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type Stmtx struct {
	*sqlx.Stmt
}

// Exec executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.
func (s *Stmtx) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// ExecContext executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.
func (s *Stmtx) ExecContext(ctx context.Context, args ...interface{}) (result sql.Result, err error) {
	r, err := retryFunc("stmt_exec", func() (interface{}, error) {
		return s.Stmt.ExecContext(ctx, args...)
	})
	if err == nil {
		result = r.(sql.Result)
	}
	return
}

// Query executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmtx) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmtx) QueryContext(ctx context.Context, args ...interface{}) (result *sql.Rows, err error) {
	r, err := retryFunc("stmt_query", func() (interface{}, error) {
		return s.Stmt.QueryContext(ctx, args...)
	})
	if err == nil {
		result = r.(*sql.Rows)
	}
	return
}

// Queryx executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmtx) Queryx(args ...interface{}) (*sqlx.Rows, error) {
	return s.QueryxContext(context.Background(), args...)
}

// QueryxContext executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmtx) QueryxContext(ctx context.Context, args ...interface{}) (result *sqlx.Rows, err error) {
	r, err := retryFunc("stmt_query", func() (interface{}, error) {
		return s.Stmt.QueryxContext(ctx, args...)
	})
	if err == nil {
		result = r.(*sqlx.Rows)
	}
	return
}
