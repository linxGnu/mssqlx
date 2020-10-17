package mssqlx

import (
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ERROR 1213: Deadlock found when trying to get lock
func isDeadlock(err error) bool {
	result := isErrCode(err, 1213)
	if result {
		fmt.Println("xxxxxxxxxxxxxxxxx")
	}
	return result
}

// ERROR 1047: WSREP has not yet prepared node for application use
func isWsrepNotReady(err error) bool {
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

// check bad connection
func isErrBadConn(err error) (v bool) {
	if err != nil {
		// Postgres/Mysql Driver returns default driver.ErrBadConn
		// Mysql Driver ("github.com/go-sql-driver/mysql") is not
		s := err.Error()
		v = err == driver.ErrBadConn || s == "invalid connection" || s == "bad connection"
	}
	return
}

func parseError(w *wrapper, err error) error {
	if err == nil {
		return nil
	}

	if w != nil && ping(w) != nil {
		return ErrNetwork
	}

	return err
}

func reportError(v string, err error) {
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("mssqlx;;%s;;%s;;%s;;%s\n", time.Now().Format("2006-01-02 15:04:05"), hostName, v, err.Error()))
	}
}
