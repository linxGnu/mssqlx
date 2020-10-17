package mssqlx

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	_, err := retryFunc("", func() (interface{}, error) {
		return nil, sql.ErrConnDone
	})
	require.Equal(t, sql.ErrConnDone, err)
}
