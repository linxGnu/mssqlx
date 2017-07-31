package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/linxGnu/mssqlx"
)

func StressTest(t *testing.T) {
	dsn := "root:123@(%s:3306)/test?charset=utf8&collation=utf8_general_ci&parseTime=true"

	masterDSNs := []string{fmt.Sprintf(dsn, "172.31.25.233"), fmt.Sprintf(dsn, "172.31.25.234"), fmt.Sprintf(dsn, "172.31.25.235")}
	slaveDSNs := []string{fmt.Sprintf(dsn, "172.31.25.233"), fmt.Sprintf(dsn, "172.31.25.234"), fmt.Sprintf(dsn, "172.31.25.235")}

	dbs, _ := mssqlx.ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs)

	for {
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
