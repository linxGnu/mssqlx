package mssqlx

import (
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"time"
)

// ERROR 1213: Deadlock found when trying to get lock
func isDeadlock(err error) (v bool) {
	if err != nil {
		se := err.Error()
		v = strings.HasPrefix(se, "Error 1213:") || strings.HasPrefix(se, "ERROR 1213:")
	}
	return
}

// ERROR 1047: WSREP has not yet prepared node for application use
func isWsrepNotReady(err error) (v bool) {
	if err != nil {
		se := err.Error()
		v = strings.HasPrefix(se, "Error 1047:") || strings.HasPrefix(se, "ERROR 1047:")
	}
	return
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
