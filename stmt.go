package mssqlx

import (
	"context"
	"database/sql"
)

type Stmt struct {
	*sql.Stmt
}

// Exec executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.
func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// ExecContext executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (result sql.Result, err error) {
	r, err := retryFunc("stmt_exec", func() (interface{}, error) {
		return s.Stmt.ExecContext(ctx, args...)
	})
	if err == nil {
		result = r.(sql.Result)
	}
	return
}

// Query executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with the given arguments and returns the query results as a *Rows.
func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (result *sql.Rows, err error) {
	r, err := retryFunc("stmt_query", func() (interface{}, error) {
		return s.Stmt.QueryContext(ctx, args...)
	})
	if err == nil {
		result = r.(*sql.Rows)
	}
	return
}
