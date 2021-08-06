package mssqlx

import (
	"time"
)

func retryFunc(query string, f func() (interface{}, error)) (result interface{}, err error) {
	for retry := 0; retry < 50; retry++ {
		result, err = f()
		if err == nil || isStdErr(err) {
			return
		}

		if isErrBadConn(err) || IsDeadlock(err) {
			time.Sleep(5 * time.Millisecond)
			continue
		} else {
			break
		}
	}

	if isErrBadConn(err) {
		reportError(query, err)
	}

	return
}
