// +build go1.8

// The following environment variables, if set, will be used:
//
//	* SQLX_SQLITE_DSN
//	* SQLX_POSTGRES_DSN
//	* SQLX_MYSQL_DSN
//
// Set any of these variables to 'skip' to skip them.  Note that for MySQL,
// the string '?parseTime=True' will be appended to the DSN if it's not there
// already.
//
package mssqlx

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/linxGnu/mssqlx"
	_ "github.com/mattn/go-sqlite3"
)

func StressTest(t *testing.T) {
	pgdsn := os.Getenv("SQLX_POSTGRES_DSN")
	mydsn := os.Getenv("SQLX_MYSQL_DSN")
	sqdsn := os.Getenv("SQLX_SQLITE_DSN")

	TestPostgres = pgdsn != "skip"
	TestMysql = mydsn != "skip"
	TestSqlite = sqdsn != "skip"

	if !strings.Contains(mydsn, "parseTime=true") {
		mydsn += "?parseTime=true"
	}

	ds, driver := "", ""

	if TestPostgres {
		dsn, driver = pgdsn, "postgres"

	}
	if TestMysql {
		dsn, driver = mydsn, "mysql"
	}
	if TestSqlite {
		dsn, driver = sqdsn, "sqlite3"
	}

	if dsn == "" || driver == "" {
		return
	}

	masterDSNs := []string{dsn, dsn, dsn}
	slaveDSNs := []string{dsn, dsn}

	dbs, _ := mssqlx.ConnectMasterSlaves(driver, masterDSNs, slaveDSNs)

	for ti := 0; ti <= 100; ti++ {
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			for i := 0; i <= 10000; i++ {
				var count int
				if err := dbs.Get(&count, "SELECT count(*) FROM test"); err != nil {
					fmt.Println("select", err)
				}
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			for i := 0; i <= 100; i++ {
				if _, err := dbs.Exec("INSERT INTO test (id) VALUES (?)", i); err != nil {
					fmt.Println("insert", err)
				}
			}
			wg.Done()
		}()

		wg.Wait()

		if _, err := dbs.Exec("DELETE FROM test"); err != nil {
			fmt.Println("delete", err)
		}

		time.Sleep(2 * time.Second)
	}
}
