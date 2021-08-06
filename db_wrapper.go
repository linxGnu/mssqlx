package mssqlx

import (
	"github.com/jmoiron/sqlx"
)

type wrapper struct {
	db  *sqlx.DB
	dsn string
}

func (w *wrapper) checkWsrepReady() bool {
	type wsrepVariable struct {
		VariableName string `db:"Variable_name"`
		Value        string `db:"Value"`
	}

	var v wsrepVariable

	if err := w.db.Get(&v, "SHOW VARIABLES LIKE 'wsrep_on'"); err != nil {
		reportError("SHOW VARIABLES LIKE 'wsrep_on'", err)
		return false
	}

	if v.Value != "ON" {
		return true
	}

	if err := w.db.Get(&v, "SHOW STATUS LIKE 'wsrep_ready'"); err != nil || v.Value != "ON" {
		reportError("SHOW STATUS LIKE 'wsrep_ready'", err)
		return false
	}

	return true
}
