package mssqlx

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
)

// check bad connection
func isErrBadConn(err error) bool {
	if err != nil {
		if errors.Is(err, driver.ErrBadConn) ||
			errors.Is(err, mysql.ErrInvalidConn) ||
			errors.Is(err, pq.ErrChannelNotOpen) {
			return true
		}

		// Postgres/Mysql Driver returns default driver.ErrBadConn
		// Mysql Driver ("github.com/go-sql-driver/mysql") is not
		s := strings.ToLower(err.Error())
		return s == "invalid connection" || s == "bad connection"
	}
	return false
}

// IsDeadlock ERROR 1213: Deadlock found when trying to get lock
func IsDeadlock(err error) bool {
	return isErrCode(err, 1213)
}

// IsWsrepNotReady ERROR 1047: WSREP has not yet prepared node for application use
func IsWsrepNotReady(err error) bool {
	return isErrCode(err, 1047)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}

	switch mErr := err.(type) {
	case *mysql.MySQLError:
		return mErr.Number == uint16(code)

	default:
		se := strings.ToLower(err.Error())
		return strings.HasPrefix(se, fmt.Sprintf("error %d:", code))
	}
}

func isStdErr(err error) bool {
	return errors.Is(err, sql.ErrNoRows) ||
		errors.Is(err, sql.ErrTxDone) ||
		errors.Is(err, sql.ErrConnDone)
}

func reportError(v string, err error) {
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("mssqlx;;%s;;%s;;%s;;%s\n", time.Now().Format("2006-01-02 15:04:05"), hostName, v, err.Error()))
	}
}
