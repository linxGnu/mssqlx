# mssqlx

[![Build Status](https://travis-ci.org/linxGnu/mssqlx.svg?branch=master)](https://travis-ci.org/linxGnu/mssqlx)
[![Go Report Card](https://goreportcard.com/badge/github.com/linxGnu/mssqlx)](https://goreportcard.com/report/github.com/linxGnu/mssqlx)
[![Coverage Status](https://coveralls.io/repos/github/linxGnu/mssqlx/badge.svg?branch=master)](https://coveralls.io/github/linxGnu/mssqlx?branch=master)
[![godoc](https://img.shields.io/badge/docs-GoDoc-green.svg)](https://godoc.org/github.com/linxGnu/mssqlx)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/jmoiron/sqlx/master/LICENSE)

Embeddable, high avaiable, performance and lightweight database client library for any golang applications.

Features and concepts are:

* Builtin layer/extension to [sqlx](http://jmoiron.github.io/sqlx).
* Auto proxy for any master-slave, master-master databases. Compatible with Wsrep, Galera Cluster and others.
* Auto and lightweight round-robin balancer of `select/show queries` on slaves (by defaults) or masters. 
* `update/delete/insert queries` are executed on a chosen master at a time. 
* Builtin error handling for Wsrep, Galera and some database drivers.
* Auto health checking.

## Notices

* APIs supports executing query on master-only or slave-only (or boths). Function name for querying on master-only has suffix `OnMaster`, querying on slaves-only has suffix `OnSlave`.
* Default `select/show queries` are balanced on slaves.
* Default `update/delete/insert queries` are on only one master at a time. If this one failed (wsrep not ready, master down, etc), `update/delete/insert queries` would be switched to other master. New chosen master is used for further data modification query.

## In production

* [iParking](https://iparking.vn) : a large car parking system of Ha Noi with heavy workload.
* [Trusting Social](https://trustingsocial.com) : most backend components will be powered by mssqlx soon.

## Install

    go get github.com/linxGnu/mssqlx

## Usage

Below is an example which shows some common use cases for mssqlx.


```go
package main

import (
    "database/sql"
	
    _ "github.com/go-sql-driver/mysql"
    "github.com/linxGnu/mssqlx"
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
    dsn := "root:123@(%s:3306)/test?charset=utf8&collation=utf8_general_ci&parseTime=true"

    masterDSNs := []string{fmt.Sprintf(dsn, "172.31.25.233"), fmt.Sprintf(dsn, "172.31.25.234"), fmt.Sprintf(dsn, "172.31.25.235")}
    slaveDSNs := []string{fmt.Sprintf(dsn, "172.31.25.233"), fmt.Sprintf(dsn, "172.31.25.234"), fmt.Sprintf(dsn, "172.31.25.235")}

    db, _ := mssqlx.ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs)
    // db, _ := mssqlx.ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs, true) -- indicates Galera/Wsrep Replication

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
    // or without naming: db.Get(&jason, "SELECT * FROM person WHERE first_name = ?", "Jason")
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
