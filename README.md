# mssqlx

[![Build Status](https://travis-ci.org/linxGnu/mssqlx.svg?branch=master)](https://travis-ci.org/linxGnu/mssqlx)
[![Go Report Card](https://goreportcard.com/badge/github.com/linxGnu/mssqlx)](https://goreportcard.com/report/github.com/linxGnu/mssqlx)
[![Coverage Status](https://coveralls.io/repos/github/linxGnu/mssqlx/badge.svg?branch=master)](https://coveralls.io/github/linxGnu/mssqlx?branch=master)
[![godoc](https://img.shields.io/badge/docs-GoDoc-green.svg)](https://godoc.org/github.com/linxGnu/mssqlx)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/jmoiron/sqlx/master/LICENSE)

Embeddable, high availability, performance and lightweight database client library for any golang applications.

Features and concepts are:

* Builtin layer/extension to [sqlx](http://jmoiron.github.io/sqlx).
* Auto proxy for any master-slave, master-master databases. Compatible with Wsrep, Galera Cluster and others.
* Auto and lightweight round-robin balancer of `select/show queries` on slaves (by defaults) or masters. 
* `update/delete/insert queries` are executed on a chosen master at a time. 
* Builtin error handling for Wsrep, Galera and some database drivers.
* Auto health checking.

## In production

* [iParking](https://iparking.vn) : a large car parking system of Ha Noi with heavy workload.

## Install

    go get -u github.com/linxGnu/mssqlx

## Connecting to Databases

mssqlx is compatible to all kind of databases which `database/sql` supports. Below code is `mysql` usage:

```go
import (
    _ "github.com/go-sql-driver/mysql"
    "github.com/linxGnu/mssqlx"
)

dsn := "root:123@(%s:3306)/test?charset=utf8&collation=utf8_general_ci&parseTime=true"
masterDSNs := []string{
    fmt.Sprintf(dsn, "172.31.25.233"), // address of master 1
    fmt.Sprintf(dsn, "172.31.24.233"), // address of master 2 if have
    fmt.Sprintf(dsn, "172.31.23.233"), // address of master 3 if have
}
slaveDSNs := []string{
    fmt.Sprintf(dsn, "172.31.25.234"), // address of slave 1
    fmt.Sprintf(dsn, "172.31.25.235"), // address of slave 2
    fmt.Sprintf(dsn, "172.31.25.236"), // address of slave 3
}

db, _ := mssqlx.ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs)
```

## Configuration

It's highly recommended to setup configuration before querying.

```go
db.SetMaxIdleConns(20) // set max idle connections to all nodes
// db.SetMasterMaxIdleConns(20) // set max idle connections to master nodes
// db.SetSlaveMaxIdleConns(20) // set max idle connections to slave nodes

db.SetMaxOpenConns(50) // set max open connections to all nodes
// db.SetMasterMaxOpenConns(50) 
// db.SetSlaveMaxOpenConns(50)
    
// if nodes fail, checking healthy in a period (in milliseconds) for auto reconnect. Default is 500.
db.SetHealthCheckPeriod(1000) 
// db.SetMasterHealthCheckPeriod(1000)
// db.SetSlaveHealthCheckPeriod(1000)
```

## Select

```go
type Person struct {
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
    Email     string
    Data      []byte
}

var people []Person
db.Select(&people, "SELECT * FROM person WHERE id > ? and id < ? ORDER BY first_name ASC", 1, 1000)
```

## Get

```go
var person Person
db.Get(&person, "SELECT * FROM person WHERE id = ?", 1)
```

## Queryx

```go
// Loop through rows using only one struct
var person Person

rows, err := db.Queryx("SELECT * FROM person") // or db.QueryxOnMaster(...)
for rows.Next() {
    if err := rows.StructScan(&person); err != nil {
        log.Fatalln(err)
    } 
    fmt.Printf("%#v\n", person)
}
```

## Named query

```go
// Loop through rows using only one struct
var person Person

rows, err := db.NamedQuery(`SELECT * FROM person WHERE first_name = :fn`, map[string]interface{}{"fn": "Bin"}) // or db.NamedQueryOnMaster(...)
for rows.Next() {
    if err := rows.StructScan(&person); err != nil {
        log.Fatalln(err)
    } 
    fmt.Printf("%#v\n", person)
}
```

## Exec (insert/update/delete/etc...)

```go
result, err := db.Exec("DELETE FROM person WHERE id < ?", 100)
```

## Transaction

```go
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
			
    if _, err = tx.Exec("INSERT INTO person(first_name, last_name, email, data) VALUES (?,?,?,?)", "Jon", "Dow", "jon@gmail", []byte{1, 2}); err != nil {
        return
    }
    
    if _, err = tx.Exec("INSERT INTO person(first_name, last_name, email, data) VALUES (?,?,?,?)", "Jon", "Snow", "snow@gmail", []byte{1}); err != nil {
        return
    }
			
    if err = tx.Commit(); err != nil {
        shouldAutoRollBack = false
    }
}
```

## Notices

* APIs supports executing query on master-only or slave-only (or boths). Function name for querying on master-only has suffix `OnMaster`, querying on slaves-only has suffix `OnSlave`.
* Default `select/show queries` are balanced on slaves.
* Default `update/delete/insert queries` are on only one master at a time. If this one failed (wsrep not ready, master down, etc), `update/delete/insert queries` would be switched to other master. New chosen master is used for further data modification query.