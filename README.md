#mssqlx

[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/jmoiron/sqlx/master/LICENSE)

mssqlx is capable of doing queries to master-slave (or master-master) database structure, provides very similar APIs compared to sqlx.

Major concepts are:

* Try to keep 100% API compatible to sqlx. All additional layers/codes are within mssqlx.go file. All other files belongs to sqlx. My work is only on mssqlx.go.
* Provide HA solution for select-query. 
* Simple and lightweight round-robin balancer with auto health checking, distributes workloads across slaves. When one downs, mssqlx moves its client to "failure-zone", avoids querying over this client. As the slave ups again, mssqlx puts (automatically) the client back for upcoming queries.
* Modify data query (INSERT/DELETE/UPDATE) should be done on only one master for various reason but automatically switch to other if this master fails.

Notices:
* Obviously, you should call delete, insert, update query on master. 
* APIs supports executing query on Master-only or Slave-only (or boths). Function name for querying on Master-only has suffix "OnMaster", querying on Slaves-only has suffix "OnSlave".

## install

    go get github.com/linxGnu/mssqlx

## usage

Below is an example which shows some common use cases for mssqlx.


```go
package main

import (
    "database/sql"
	
    _ "github.com/lib/pq"
    "github.com/linxGnu/mssqlx/types"
    "github.com/linxGnu/mssqlx"

    "log"
)

var schema = `
CREATE TABLE person (
    first_name text,
    last_name text,
    email text
);

CREATE TABLE place (
    country text,
    city text NULL,
    telcode integer
)`

type SpaceCow struct {
	Id                   uint
	Data                 types.JSONText
	Created_at           time.Time
	Updated_at           time.Time
}

type Person struct {
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
    Email     string
}

type Place struct {
    Country string
    City    sql.NullString
    TelCode int
}

func main() {
    masterDSN := "user=foo dbname=bar sslmode=disable"
    slaveDSNs := []string{"user=readonly dbname=bar sslmode=disable"}

    db, err := mssqlx.ConnectMasterSlaves("postgres", masterDSN, slaveDSNs)
    if err != nil {
        log.Fatalln(err)
    }

    db.SetMaxIdleConns(20) // set max idle connections to all nodes
    // db.SetMasterMaxIdleConns(20) // set max idle connections to master nodes
    // db.SetSlaveMaxIdleConns(20) // set max idle connections to slave nodes

    db.SetMaxOpenConns(50) // set max open connections to all nodes
    // db.SetMasterMaxOpenConns(50) 
    // db.SetSlaveMaxOpenConns(50)

    db.SetHealthCheckPeriod(1000) // if nodes fail, checking healthy in a period (in milliseconds) for auto reconnect. Default is 500.
    // db.SetMasterHealthCheckPeriod(1000)
    // db.SetSlaveHealthCheckPeriod(1000)

    // Recommended write transaction this way
    master, total := db.GetMaster()
    if total > 0 && master != nil {
	tx, e := master.GetDB().Begin()
	if e != nil {
		return e
	}

	shouldAutoRollBack := true
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			tx.Rollback()
		} else if err != nil && shouldAutoRollBack {
			tx.Rollback()
		}
	}()
			
	if _, err = tx.Exec(........); err != nil {
		return
	}
			
	if err = tx.Commit(); err != nil {
		shouldAutoRollBack = false
	}
    }

    // Query all master and slave databases, storing results in a []Person (wrapped in []interface{})
    people := []Person{}
    db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
    // or select from slaves only: db.SelectOnSlave(&people, "SELECT * FROM person ORDER BY first_name ASC")
    // or select from master only: db.SelectOnMaster(&people, "SELECT * FROM person ORDER BY first_name ASC")
    jason, john := people[0], people[1]

    fmt.Printf("%#v\n%#v", jason, john)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}
    // Person{FirstName:"John", LastName:"Doe", Email:"johndoeDNE@gmail.net"}

    // You can also get a single result, a la QueryRow
    jason = Person{}
    err = db.Get(&jason, "SELECT * FROM person WHERE first_name=$1", "Jason")
    // or get from slaves only: db.GetOnSlave(&people, "SELECT * FROM person ORDER BY first_name ASC")
    // or get from master only: db.GetOnMaster(&people, "SELECT * FROM person ORDER BY first_name ASC")

    // Error handling
    if err == mssqlx.ErrNoRecord {
        fmt.Println("Jason not found")
    } else if err != nil {
        fmt.Println("Error: %v", err)
    } else {
        fmt.Printf("%#v\n", jason)
    }

    // if you have null fields and use SELECT *, you must use sql.Null* in your struct
    places := []Place{}
    err = db.Select(&places, "SELECT * FROM place ORDER BY telcode ASC")
    if err != nil {
        fmt.Println(err)
        return
    }
    usa, singsing, honkers := places[0], places[1], places[2]
    
    fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}

    // Loop through rows using only one struct
    place := Place{}
    rows, err := db.Queryx("SELECT * FROM place") 
    // or db.QueryxOnMaster(...)
    // or db.QueryxOnSlave(...)

    for rows.Next() {
        err := rows.StructScan(&place)
        if err != nil {
            log.Fatalln(err)
        } 
        fmt.Printf("%#v\n", place)
    }
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}

    // Named queries, using `:name` as the bindvar.  Automatic bindvar support
    // which takes into account the dbtype based on the driverName on mssqlx.Connect
    _, err = db.NamedExecOnMaster(`INSERT INTO person (first_name,last_name,email) VALUES (:first,:last,:email)`, 
        map[string]interface{}{
            "first": "Bin",
            "last": "Smuth",
            "email": "bensmith@allblacks.nz",
    })

    // Selects Mr. Smith from the database
    rows, err = db.NamedQuery(`SELECT * FROM person WHERE first_name=:fn`, map[string]interface{}{"fn": "Bin"})

    // Named queries can also use structs.  Their bind names follow the same rules
    // as the name -> db mapping, so struct fields are lowercased and the `db` tag
    // is taken into consideration.
    rows, err = db.NamedQuery(`SELECT * FROM person WHERE first_name=:first_name`, jason)
}
```

