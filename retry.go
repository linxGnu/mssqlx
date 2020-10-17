package mssqlx

import (
	"database/sql"
	"time"
)

func retryFunc(query string, f func() (interface{}, error)) (result interface{}, err error) {
	for retry := 0; retry < 50; retry++ {
		if result, err = f(); err == nil {
			return
		}

		switch err {
		case sql.ErrConnDone:

		case sql.ErrTxDone, sql.ErrNoRows:
			return

		default:
			if isErrBadConn(err) || IsDeadlock(err) {
				time.Sleep(5 * time.Millisecond)
			} else {
				return
			}
		}
	}

	if err == sql.ErrConnDone || isErrBadConn(err) {
		reportError(query, err)
	}

	return
}
