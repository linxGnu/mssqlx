// The following environment variables, if set, will be used:
//
//	* MSSQLX_POSTGRES_DSN
//	* MSSQLX_MYSQL_DSN
//	* MSSQLX_SQLITE_DSN
//
// Set any of these variables to 'skip' to skip them.  Note that for MySQL,
// the string '?parseTime=True' will be appended to the DSN if it's not there
// already.
//
package mssqlx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var TestWPostgres = false // test with postgres?
var TestWSqlite = false   // test with sqlite?
var TestWMysql = false    // test with mysql?

var myDBs *DBs
var pgDBs *DBs
var sqDBs *DBs

func init() {
	ConnectMasterSlave()
}

func ConnectMasterSlave() {
	pgdsn := os.Getenv("MSSQLX_POSTGRES_DSN")
	mydsn := os.Getenv("MSSQLX_MYSQL_DSN")
	sqdsn := os.Getenv("MSSQLX_SQLITE_DSN")

	TestWPostgres = pgdsn != "skip" && pgdsn != ""
	TestWMysql = mydsn != "skip" && mydsn != ""
	TestWSqlite = sqdsn != "skip" && sqdsn != ""

	if !strings.Contains(mydsn, "parseTime=true") {
		mydsn += "?parseTime=true"
	}

	if !TestWPostgres {
		fmt.Println("Disabling Postgres tests.")
	}

	if !TestWMysql {
		fmt.Println("Disabling MySQL tests.")
	}

	if !TestWSqlite {
		fmt.Println("Disabling SQLite tests.")
	}

	if TestWPostgres {
		masterDSNs := []string{pgdsn, pgdsn}
		slaveDSNs := []string{pgdsn, pgdsn}
		pgDBs, _ = ConnectMasterSlaves("postgres", masterDSNs, slaveDSNs)
		pgDBs.SetMaxIdleConns(5)
		pgDBs.SetMaxOpenConns(10)
		pgDBs.SetQueryRetryTimeWhenBadConn(3, 10)
	}

	if TestWMysql {
		masterDSNs := []string{mydsn, mydsn}
		slaveDSNs := []string{mydsn, mydsn}
		myDBs, _ = ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs)
		myDBs.SetMaxIdleConns(5)
		myDBs.SetMaxOpenConns(10)
		myDBs.SetQueryRetryTimeWhenBadConn(3, 10)
	}

	if TestWSqlite {
		masterDSNs := []string{sqdsn, sqdsn}
		slaveDSNs := []string{sqdsn, sqdsn}
		sqDBs, _ = ConnectMasterSlaves("sqlite3", masterDSNs, slaveDSNs)
		sqDBs.SetMaxIdleConns(5)
		sqDBs.SetMaxOpenConns(10)
		sqDBs.SetQueryRetryTimeWhenBadConn(3, 10)
	}
}

type Schema struct {
	create string
	drop   string
}

func (s Schema) Postgres() (string, string) {
	return s.create, s.drop
}

func (s Schema) MySQL() (string, string) {
	return strings.Replace(s.create, `"`, "`", -1), s.drop
}

func (s Schema) Sqlite3() (string, string) {
	return strings.Replace(s.create, `now()`, `CURRENT_TIMESTAMP`, -1), s.drop
}

var defaultSchema = Schema{
	create: `
CREATE TABLE person (
	first_name text,
	last_name text,
	email text,
	added_at timestamp default now()
);
CREATE TABLE place (
	country text,
	city text NULL,
	telcode integer
);
CREATE TABLE capplace (
	"COUNTRY" text,
	"CITY" text NULL,
	"TELCODE" integer
);
CREATE TABLE nullperson (
    first_name text NULL,
    last_name text NULL,
    email text NULL
);
CREATE TABLE employees (
	name text,
	id integer,
	boss_id integer
);
`,
	drop: `
drop table person;
drop table place;
drop table capplace;
drop table nullperson;
drop table employees;
`,
}

type Person struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string
	AddedAt   time.Time `db:"added_at"`
}

type Person2 struct {
	FirstName sql.NullString `db:"first_name"`
	LastName  sql.NullString `db:"last_name"`
	Email     sql.NullString
}

type Place struct {
	Country string
	City    sql.NullString
	TelCode int
}

type PlacePtr struct {
	Country string
	City    *string
	TelCode int
}

type PersonPlace struct {
	Person
	Place
}

type PersonPlacePtr struct {
	*Person
	*Place
}

type EmbedConflict struct {
	FirstName string `db:"first_name"`
	Person
}

type SliceMember struct {
	Country   string
	City      sql.NullString
	TelCode   int
	People    []Person `db:"-"`
	Addresses []Place  `db:"-"`
}

func MultiExec(e Execer, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.Exec(s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

// Note that because of field map caching, we need a new type here
// if we've used Place already somewhere in sqlx
type CPlace Place

func _RunWithSchema(schema Schema, t *testing.T, test func(db *DBs, t *testing.T)) {
	runner := func(db *DBs, t *testing.T, create, drop string) {
		defer func() {
			MultiExec(db, drop)
		}()

		MultiExec(db, create)
		test(db, t)
	}

	if TestWPostgres {
		create, drop := schema.Postgres()
		runner(pgDBs, t, create, drop)
	}
	if TestWSqlite {
		create, drop := schema.Sqlite3()
		runner(sqDBs, t, create, drop)
	}
	if TestWMysql {
		create, drop := schema.MySQL()
		runner(myDBs, t, create, drop)
	}
}

func _loadDefaultFixture(db *DBs, t *testing.T) {
	master, num := db.GetMaster()
	if num > 0 {
		tx := master.GetDB().MustBegin()
		tx.MustExec(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "Jason", "Moiron", "jmoiron@jmoiron.net")
		tx.MustExec(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoeDNE@gmail.net")
		tx.MustExec(tx.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
		tx.MustExec(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")
		tx.MustExec(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
		if db.DriverName() == "mysql" {
			tx.MustExec(tx.Rebind("INSERT INTO capplace (`COUNTRY`, `TELCODE`) VALUES (?, ?)"), "Sarf Efrica", "27")
		} else {
			tx.MustExec(tx.Rebind("INSERT INTO capplace (\"COUNTRY\", \"TELCODE\") VALUES (?, ?)"), "Sarf Efrica", "27")
		}
		tx.MustExec(tx.Rebind("INSERT INTO employees (name, id) VALUES (?, ?)"), "Peter", "4444")
		tx.MustExec(tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Joe", "1", "4444")
		tx.MustExec(tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Martin", "2", "4444")
		tx.Commit()
	}
}

func TestParseError(t *testing.T) {
	err := parseError(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	errT := fmt.Errorf("abc")
	if err = parseError(nil, errT); err != errT {
		t.Fatal(err)
	}

	db, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")

	errT = fmt.Errorf("abc")
	if err = parseError(db, errT); err != ErrNetwork {
		t.Fatal(err)
	}
}

func TestDbLinkListNode(t *testing.T) {
	db1, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	llNode1 := &dbLinkListNode{db: db1}
	if llNode1.GetDB() != db1 {
		t.Fatal("dbLinkListNode is not set properly")
	}

	db2, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	llNode2 := &dbLinkListNode{db: db2}

	db3, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	llNode3 := &dbLinkListNode{db: db3}

	db4, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	llNode4 := &dbLinkListNode{db: db4}

	ll := dbLinkList{}

	if ll.next() != nil || ll.prev() != nil {
		t.Fatal("dbLinkList is not set properly: next function fail")
	}

	// test add
	ll.add(llNode1)
	ll.add(llNode2)
	ll.add(nil)
	ll.add(&dbLinkListNode{})
	if ll.current != llNode1 || ll.size != 2 || ll.getCurrentNode() != llNode1 {
		t.Fatal("dbLinkList is not set properly")
	}

	if ll.next() != llNode2 || ll.prev() != llNode2 {
		t.Fatal("dbLinkList is not set properly: next/prev function fail")
	}

	ll.add(llNode3)
	if ll.next() != llNode2 || ll.prev() != llNode3 {
		t.Fatal("dbLinkList is not set properly: next/prev function fail")
	}

	// test move next
	if ll.moveNext() != llNode1 {
		t.Fatal("dbLinkList is not set properly: moveNext function fail")
	}
	if ll.current != llNode2 || ll.getCurrentNode() != llNode2 {
		t.Fatal("dbLinkList is not set properly: moveNext function fail")
	}

	// test move prev
	if ll.moveNext() != llNode2 {
		t.Fatal("dbLinkList is not set properly: moveNext function fail")
	}

	if ll.movePrev() != llNode3 {
		t.Fatal("dbLinkList is not set properly: movePrev function fail")
	}

	if ll.getCurrentNode() != llNode2 {
		t.Fatal("dbLinkList is not set properly: movePrev function fail")
	}

	// now try to remove
	ll.add(llNode4)

	if ll.remove(llNode2) {
		if ll.getCurrentNode() != llNode3 {
			t.Fatal("dbLinkList is not set properly: remove function fail")
		}
	} else {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	if ll.remove(nil) {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	if ll.remove(llNode1) {
		if ll.getCurrentNode() != llNode3 {
			t.Fatal("dbLinkList is not set properly: remove function fail")
		}
	} else {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	if ll.remove(llNode3) {
		if ll.getCurrentNode() != llNode4 || ll.head != llNode4 || ll.tail != llNode4 {
			t.Fatal("dbLinkList is not set properly: remove function fail")
		}
	} else {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	ll.add(llNode3)
	if ll.remove(llNode3) {
		if ll.getCurrentNode() != llNode4 || ll.head != llNode4 || ll.tail != llNode4 {
			t.Fatal("dbLinkList is not set properly: remove function fail")
		}
	} else {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	if ll.remove(llNode4) {
		if ll.current != nil || ll.head != nil || ll.tail != nil {
			t.Fatal("dbLinkList is not set properly: remove function fail")
		}
	} else {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}

	ll.add(llNode1)
	ll.clear()
	if ll.current != nil || ll.head != nil || ll.tail != nil {
		t.Fatal("dbLinkList is not set properly: remove function fail")
	}
}

func TestDbBalancer(t *testing.T) {
	dbB := dbBalancer{}

	dbB.init(-1, 12, true)
	if dbB.numberOfHealthChecker != 2 {
		t.Fatal("DbBalancer init fail")
	}

	dbB.init(0, 12, true)
	if dbB.numberOfHealthChecker != 2 {
		t.Fatal("DbBalancer: init fail")
	}

	dbB.init(4, 12, true)
	if dbB.numberOfHealthChecker != 4 {
		t.Fatal("DbBalancer: init fail")
	}

	if dbB.setHealthCheckPeriod(200); dbB.healthCheckPeriod != 200 {
		t.Fatal("DbBalancer: setHealthCheckPeriod fail")
	}

	db1, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db2, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db3, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db4, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")

	dbB.add(nil)
	dbB.add(db1)
	dbB.add(db2)
	dbB.add(db3)
	dbB.add(db4)
	if dbB.dbs.size != 4 {
		t.Fatal("DbBalancer: add fail")
	}

	if x := dbB.get(true); x.db != db1 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(false); x.db != db2 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(true); x.db != db2 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(false); x.db != db3 {
		t.Fatal("DbBalancer: get fail")
	} else {
		dbB.failure(x)

		if dbB.dbs.size != 3 || dbB.checkWsrepReady(db3) {
			t.Fatal("DbBalancer: failure fail")
		}
	}

	dbB.destroy()
	if dbB.dbs.size != 0 {
		t.Fatal("DbBalancer: destroy fail")
	}
}

func TestConnectMasterSlave(t *testing.T) {
	dsn, driver := "user=test1 dbname=test1 sslmode=disable", "postgres"

	masterDSNs := []string{dsn, dsn, dsn}
	slaveDSNs := []string{dsn, dsn}

	db, _ := ConnectMasterSlaves(driver, masterDSNs, slaveDSNs)
	if db.DriverName() != "postgres" {
		t.Fatal("DriverName fail")
	}

	// test ping
	if errs := db.Ping(); errs == nil || len(errs) != 5 {
		t.Fatal("Ping fail")
	}
	if errs := db.PingMaster(); errs == nil || len(errs) != 3 {
		t.Fatal("Ping fail")
	}
	if errs := db.PingSlave(); errs == nil || len(errs) != 2 {
		t.Fatal("Ping fail")
	}

	// test SetHealthCheckPeriod
	db.SetHealthCheckPeriod(200)
	if db.masters.healthCheckPeriod != 200 || db.slaves.healthCheckPeriod != 200 {
		t.Fatal("SetHealthCheckPeriod fail")
	}
	db.SetMasterHealthCheckPeriod(300)
	if db.masters.healthCheckPeriod != 300 || db.slaves.healthCheckPeriod != 200 {
		t.Fatal("SetMasterHealthCheckPeriod fail")
	}
	db.SetSlaveHealthCheckPeriod(400)
	if db.masters.healthCheckPeriod != 300 || db.slaves.healthCheckPeriod != 400 {
		t.Fatal("SetSlaveHealthCheckPeriod fail")
	}

	// test set idle connection
	db.SetMaxIdleConns(12)
	db.SetMasterMaxIdleConns(13)
	db.SetSlaveMaxIdleConns(14)

	// test set max open connection
	db.SetMaxOpenConns(16)
	db.SetMasterMaxOpenConns(12)
	db.SetSlaveMaxOpenConns(19)

	// set connection max life time
	db.SetConnMaxLifetime(16 * time.Second)
	db.SetMasterConnMaxLifetime(-1)
	db.SetSlaveConnMaxLifetime(20 * time.Second)

	// get stats
	if stats := db.Stats(); stats == nil || len(stats) != 5 {
		t.Fatal("Stats fail")
	}
	if stats := db.StatsMaster(); stats == nil || len(stats) != 3 {
		t.Fatal("StatsMaster fail")
	}
	if stats := db.StatsSlave(); stats == nil || len(stats) != 2 {
		t.Fatal("StatsSlave fail")
	}

	if slaves, c := db.GetAllSlaves(); c != 2 {
		t.Fatal("GetAllSlaves fail")
	} else {
		if errs := _ping(slaves); errs == nil || len(errs) != 2 {
			t.Fatal("_ping fail")
		}
	}
	if _, c := db.GetMaster(); c != 3 {
		t.Fatal("GetMaster fail")
	}

	// test destroy master / slave
	if errs := db.DestroyMaster(); errs == nil || len(errs) != 3 {
		t.Fatal("DestroyMaster fail")
	}
	if errs := db.DestroySlave(); errs == nil || len(errs) != 2 {
		t.Fatal("DestroySlave fail")
	}

	db, _ = ConnectMasterSlaves(driver, masterDSNs, slaveDSNs, true)
	if _, c := db.GetMaster(); c != 3 {
		t.Fatal("Initialize master slave fail")
	}
	if _, c := db.GetAllSlaves(); c != 2 {
		t.Fatal("Initialize master slave fail")
	}

	// test destroy all
	if errs := db.Destroy(); errs == nil || len(errs) != 5 {
		t.Fatal("Destroy fail")
	}

	db, _ = ConnectMasterSlaves(driver, nil, slaveDSNs, true)
	if _, c := db.GetMaster(); c != 0 {
		t.Fatal("Initialize master slave fail")
	}

	db, _ = ConnectMasterSlaves(driver, nil, nil, true)
	if _, c := db.GetAllSlaves(); c != 0 {
		t.Fatal("Initialize master slave fail")
	}
}

func TestGlobalFunc(t *testing.T) {
	db1, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db2, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db3, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")
	db4, _ := sqlx.Open("postgres", "user=test1 dbname=test1 sslmode=disable")

	dbB := &dbBalancer{}
	dbB.init(-1, 4, true)
	dbB.add(db1)
	dbB.add(db2)
	dbB.add(db3)
	dbB.add(db4)
	if _, err := _exec(nil, "SELECT 1"); err != ErrNoConnection {
		t.Fatal("_exec fail")
	}

	if _, err := _exec(dbB, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_exec fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	if _, _, err := _query(dbB, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_query fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	if rowx := _queryRowx(dbB, "SELECT 1"); rowx.Err() != ErrNoConnectionOrWsrep {
		t.Fatal("_queryRowx fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 4, true)
	dbB.add(db1)
	dbB.add(db2)
	dbB.add(db3)
	dbB.add(db4)
	if _, err := _queryx(dbB, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_queryx fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	tmp := 1
	if err := _get(dbB, &tmp, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_get fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	ano := []*Person{}
	if err := _select(dbB, &ano, "SELECT * FROM test"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_select fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	if _, err := _namedExec(dbB, "DELETE FROM person WHERE first_name=:first_name", &Person{FirstName: "123"}); err != ErrNoConnectionOrWsrep {
		t.Fatal("_namedExec fail")
	}
	dbB.destroy()

	dbB = &dbBalancer{}
	dbB.init(-1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	ano = []*Person{}
	if _, err := _namedQuery(dbB, "SELECT * FROM person WHERE first_name=:first_name", &Person{FirstName: "123"}); err != ErrNoConnectionOrWsrep {
		t.Fatal("_namedQuery fail")
	}
	dbB.destroy()

	// check mapper func to see panic occuring
	_mapperFunc([]*sqlx.DB{db1, db2}, nil)

	// check ping
	if errs := _ping(nil); errs != nil {
		t.Fatal("Ping fail")
	}
	if errs := _ping([]*sqlx.DB{}); errs != nil {
		t.Fatal("Ping fail")
	}

	// check close
	if errs := _close([]*sqlx.DB{db1, db2}); errs == nil || len(errs) != 2 {
		t.Fatal("_close fail")
	}
	if errs := _close(nil); errs != nil {
		t.Fatal("_close fail")
	}
	if errs := _close([]*sqlx.DB{}); errs != nil {
		t.Fatal("_close fail")
	}

	// check set max idle conns
	_setMaxIdleConns([]*sqlx.DB{}, 12)
	_setMaxIdleConns(nil, 12)
	_setMaxIdleConns([]*sqlx.DB{db3, db4}, 12)

	// check set max conns
	_setMaxOpenConns([]*sqlx.DB{}, 16)
	_setMaxOpenConns(nil, 16)
	_setMaxOpenConns([]*sqlx.DB{db3, db4}, 16)

	// check setConnMaxLifetime
	_setConnMaxLifetime([]*sqlx.DB{}, 16*time.Second)
	_setConnMaxLifetime(nil, 16*time.Second)
	_setConnMaxLifetime([]*sqlx.DB{db3, db4}, 16*time.Second)

	// check stats
	if stats := _stats(nil); stats != nil {
		t.Fatal("_stats fail")
	}
	if stats := _stats([]*sqlx.DB{}); stats != nil {
		t.Fatal("_stats fail")
	}
	if stats := _stats([]*sqlx.DB{db1, db2, db3, db4}); stats == nil || len(stats) != 4 {
		t.Fatal("_stats fail")
	}
}

// Test a new backwards compatible feature, that missing scan destinations
// will silently scan into sql.RawText rather than failing/panicing
func TestMissingName(t *testing.T) {
	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)
		type PersonPlus struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
			Email     string
			//AddedAt time.Time `db:"added_at"`
		}

		// test Select first
		pps := []PersonPlus{}
		// pps lacks added_at destination
		err := db.Select(&pps, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected missing name from Select to fail, but it did not.")
		}

		// test Get
		pp := PersonPlus{}
		err = db.Get(&pp, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rows, err := db.Queryx("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()
	})
}

func TestEmbeddedStruct(t *testing.T) {
	type Loop1 struct{ Person }
	type Loop2 struct{ Loop1 }
	type Loop3 struct{ Loop2 }

	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)
		peopleAndPlaces := []PersonPlace{}
		err := db.Select(
			&peopleAndPlaces,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlaces {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test embedded structs with StructScan
		rows, err := db.Queryx(
			`SELECT person.*, place.* FROM
         person natural join place`)
		if err != nil {
			t.Error(err)
		}

		perp := PersonPlace{}
		rows.Next()
		err = rows.StructScan(&perp)
		if err != nil {
			t.Error(err)
		}

		if len(perp.Person.FirstName) == 0 {
			t.Errorf("Expected non zero lengthed first name.")
		}
		if len(perp.Place.Country) == 0 {
			t.Errorf("Expected non zero lengthed country.")
		}

		rows.Close()

		// test the same for embedded pointer structs
		peopleAndPlacesPtrs := []PersonPlacePtr{}
		err = db.Select(
			&peopleAndPlacesPtrs,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlacesPtrs {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test "deep nesting"
		l3s := []Loop3{}
		err = db.Select(&l3s, `select * from person`)
		if err != nil {
			t.Fatal(err)
		}
		for _, l3 := range l3s {
			if len(l3.Loop2.Loop1.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
		}

		// test "embed conflicts"
		ec := []EmbedConflict{}
		err = db.Select(&ec, `select * from person`)
		// I'm torn between erroring here or having some kind of working behavior
		// in order to allow for more flexibility in destination structs
		if err != nil {
			t.Errorf("Was not expecting an error on embed conflicts.")
		}
	})
}

func TestJoinQueries(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)

		var employees []struct {
			Employee
			Boss `db:"boss"`
		}

		err := db.Select(
			&employees,
			`SELECT employees.*, boss.id "boss.id", boss.name "boss.name" FROM employees
			  JOIN employees AS boss ON employees.boss_id = boss.id`)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Employee.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Employee.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestJoinQueryNamedPointerStruct(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)

		var employees []struct {
			Emp1  *Employee `db:"emp1"`
			Emp2  *Employee `db:"emp2"`
			*Boss `db:"boss"`
		}

		err := db.Select(
			&employees,
			`SELECT emp.name "emp1.name", emp.id "emp1.id", emp.boss_id "emp1.boss_id",
			 emp.name "emp2.name", emp.id "emp2.id", emp.boss_id "emp2.boss_id",
			 boss.id "boss.id", boss.name "boss.name" FROM employees AS emp
			  JOIN employees AS boss ON emp.boss_id = boss.id
			  `)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Emp1.Name) == 0 || len(em.Emp2.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Emp1.BossID.Int64 != em.Boss.ID || em.Emp2.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestSelectSliceMapTimes(t *testing.T) {
	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)
		rows, err := db.Queryx("SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			_, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
		}

		rows, err = db.Queryx("SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			m := map[string]interface{}{}
			err := rows.MapScan(m)
			if err != nil {
				t.Error(err)
			}
		}

	})
}

func TestNilReceivers(t *testing.T) {
	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)
		var p *Person
		err := db.Get(p, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected error when getting into nil struct ptr.")
		}
		var pp *[]Person
		err = db.Select(pp, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
	})
}

func TestNamedQueries(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE place (
				id integer PRIMARY KEY,
				name text NULL
			);
			CREATE TABLE person (
				first_name text NULL,
				last_name text NULL,
				email text NULL
			);
			CREATE TABLE placeperson (
				first_name text NULL,
				last_name text NULL,
				email text NULL,
				place_id integer NULL
			);
			CREATE TABLE jsperson (
				"FIRST" text NULL,
				last_name text NULL,
				"EMAIL" text NULL
			);`,
		drop: `
			drop table person;
			drop table jsperson;
			drop table place;
			drop table placeperson;
			`,
	}

	_RunWithSchema(schema, t, func(db *DBs, t *testing.T) {
		type Person struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
		}

		p := Person{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q1 := `INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)`
		_, err := db.NamedExec(q1, p)
		if err != nil {
			log.Fatal(err)
		}

		p2 := &Person{}
		rows, err := db.NamedQuery("SELECT * FROM person WHERE first_name=:first_name", p)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(p2)
			if err != nil {
				t.Error(err)
			}
			if p2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + p2.FirstName.String)
			}
			if p2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + p2.LastName.String)
			}
		}

		type JSONPerson struct {
			FirstName sql.NullString `json:"FIRST"`
			LastName  sql.NullString `json:"last_name"`
			Email     sql.NullString
		}

		// Test nested structs
		type Place struct {
			ID   int            `db:"id"`
			Name sql.NullString `db:"name"`
		}
		type PlacePerson struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
			Place     Place `db:"place"`
		}

		pl := Place{
			Name: sql.NullString{String: "myplace", Valid: true},
		}

		pp := PlacePerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q2 := `INSERT INTO place (id, name) VALUES (1, :name)`
		_, err = db.NamedExec(q2, pl)
		if err != nil {
			log.Fatal(err)
		}

		id := 1
		pp.Place.ID = id

		q3 := `INSERT INTO placeperson (first_name, last_name, email, place_id) VALUES (:first_name, :last_name, :email, :place.id)`
		_, err = db.NamedExec(q3, pp)
		if err != nil {
			log.Fatal(err)
		}

		pp2 := &PlacePerson{}
		rows, err = db.NamedQuery(`
			SELECT
				first_name,
				last_name,
				email,
				place.id AS "place.id",
				place.name AS "place.name"
			FROM placeperson
			INNER JOIN place ON place.id = placeperson.place_id
			WHERE
				place.id=:place.id`, pp)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(pp2)
			if err != nil {
				t.Error(err)
			}
			if pp2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + pp2.FirstName.String)
			}
			if pp2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + pp2.LastName.String)
			}
			if pp2.Place.Name.String != "myplace" {
				t.Error("Expected place name of `myplace`, got " + pp2.Place.Name.String)
			}
			if pp2.Place.ID != pp.Place.ID {
				t.Errorf("Expected place name of %v, got %v", pp.Place.ID, pp2.Place.ID)
			}
		}
	})
}

func TestNilInsert(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE tt (
				id integer,
				value text NULL DEFAULT NULL
			);`,
		drop: "drop table tt;",
	}

	_RunWithSchema(schema, t, func(db *DBs, t *testing.T) {
		type TT struct {
			ID    int
			Value *string
		}
		var v, v2 TT
		r := db.Rebind

		db.Exec(r(`INSERT INTO tt (id) VALUES (1)`))
		db.Get(&v, r(`SELECT * FROM tt`))
		if v.ID != 1 {
			t.Errorf("Expecting id of 1, got %v", v.ID)
		}
		if v.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}

		v.ID = 2
		// NOTE: this incidentally uncovered a bug which was that named queries with
		// pointer destinations would not work if the passed value here was not addressable,
		// as reflectx.FieldByIndexes attempts to allocate nil pointer receivers for
		// writing.  This was fixed by creating & using the reflectx.FieldByIndexesReadOnly
		// function.  This next line is important as it provides the only coverage for this.
		db.NamedExec(`INSERT INTO tt (id, value) VALUES (:id, :value)`, v)

		db.Get(&v2, r(`SELECT * FROM tt WHERE id=2`))
		if v.ID != v2.ID {
			t.Errorf("%v != %v", v.ID, v2.ID)
		}
		if v2.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}
	})
}

func TestScanErrors(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE kv (
				k text,
				v integer
			);`,
		drop: `drop table kv;`,
	}

	_RunWithSchema(schema, t, func(db *DBs, t *testing.T) {
		type WrongTypes struct {
			K int
			V string
		}
		_, err := db.Exec(db.Rebind("INSERT INTO kv (k, v) VALUES (?, ?)"), "hi", 1)
		if err != nil {
			t.Error(err)
		}

		rows, err := db.Queryx("SELECT * FROM kv")
		if err != nil {
			t.Error(err)
		}
		for rows.Next() {
			var wt WrongTypes
			err := rows.StructScan(&wt)
			if err == nil {
				t.Errorf("%s: Scanning wrong types into keys should have errored.", db.DriverName())
			}
		}
	})
}

// FIXME: this function is kinda big but it slows things down to be constantly
// loading and reloading the schema..

func TestUsages(t *testing.T) {
	_RunWithSchema(defaultSchema, t, func(db *DBs, t *testing.T) {
		_loadDefaultFixture(db, t)
		slicemembers := []SliceMember{}
		err := db.Select(&slicemembers, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		people := []Person{}

		err = db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
		if err != nil {
			t.Fatal(err)
		}

		jason, john := people[0], people[1]
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting FirstName of Jason, got %s", jason.FirstName)
		}
		if jason.LastName != "Moiron" {
			t.Errorf("Expecting LastName of Moiron, got %s", jason.LastName)
		}
		if jason.Email != "jmoiron@jmoiron.net" {
			t.Errorf("Expecting Email of jmoiron@jmoiron.net, got %s", jason.Email)
		}
		if john.FirstName != "John" || john.LastName != "Doe" || john.Email != "johndoeDNE@gmail.net" {
			t.Errorf("John Doe's person record not what expected:  Got %v\n", john)
		}

		jason = Person{}
		err = db.Get(&jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason")

		if err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		err = db.Get(&jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Foobar")
		if err == nil {
			t.Errorf("Expecting an error, got nil\n")
		}
		if err != sql.ErrNoRows {
			t.Errorf("Expected sql.ErrNoRows, got %v\n", err)
		}

		places := []*Place{}
		err = db.Select(&places, "SELECT telcode FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers := places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		placesptr := []PlacePtr{}
		err = db.Select(&placesptr, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Error(err)
		}
		//fmt.Printf("%#v\n%#v\n%#v\n", placesptr[0], placesptr[1], placesptr[2])

		// if you have null fields and use SELECT *, you must use sql.Null* in your struct
		// this test also verifies that you can use either a []Struct{} or a []*Struct{}
		places2 := []Place{}
		err = db.Select(&places2, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers = &places2[0], &places2[1], &places2[2]

		// this should return a type error that &p is not a pointer to a struct slice
		p := Place{}
		err = db.Select(&p, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice")
		}

		// this should be an error
		pl := []Place{}
		err = db.Select(pl, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice, not a slice.")
		}

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		rows, err := db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		place := Place{}
		for rows.Next() {
			err = rows.StructScan(&place)
			if err != nil {
				t.Fatal(err)
			}
		}

		rows, err = db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]interface{}{}
		for rows.Next() {
			err = rows.MapScan(m)
			if err != nil {
				t.Fatal(err)
			}
			_, ok := m["country"]
			if !ok {
				t.Errorf("Expected key `country` in map but could not find it (%#v)\n", m)
			}
		}

		rows, err = db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			s, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
			if len(s) != 3 {
				t.Errorf("Expected 3 columns in result, got %d\n", len(s))
			}
		}

		// test advanced querying
		// test that NamedExec works with a map as well as a struct
		_, err = db.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		// ensure that NamedQuery works with a map[string]interface{}
		_rows, err := db.NamedQuery("SELECT * FROM person WHERE first_name=:first", map[string]interface{}{"first": "Bin"})
		if err != nil {
			t.Fatal(err)
		}

		ben := &Person{}
		for _rows.Next() {
			err = _rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Bin" {
				t.Fatal("Expected first name of `Bin`, got " + ben.FirstName)
			}
			if ben.LastName != "Smuth" {
				t.Fatal("Expected first name of `Smuth`, got " + ben.LastName)
			}
		}

		ben.FirstName = "Ben"
		ben.LastName = "Smith"
		ben.Email = "binsmuth@allblacks.nz"

		// Insert via a named query using the struct
		_, err = db.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", ben)

		if err != nil {
			t.Fatal(err)
		}

		_rows, err = db.NamedQuery("SELECT * FROM person WHERE first_name=:first_name", ben)
		if err != nil {
			t.Fatal(err)
		}
		for _rows.Next() {
			err = _rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Ben" {
				t.Fatal("Expected first name of `Ben`, got " + ben.FirstName)
			}
			if ben.LastName != "Smith" {
				t.Fatal("Expected first name of `Smith`, got " + ben.LastName)
			}
		}
		// ensure that Get does not panic on emppty result set
		person := &Person{}
		err = db.Get(person, "SELECT * FROM person WHERE first_name=$1", "does-not-exist")
		if err == nil {
			t.Fatal("Should have got an error for Get on non-existant row.")
		}

		// test name mapping
		// THIS USED TO WORK BUT WILL NO LONGER WORK.
		db.MapperFunc(strings.ToUpper)
		rsa := CPlace{}
		err = db.Get(&rsa, "SELECT * FROM capplace;")
		if err != nil {
			t.Error(err, "in db:", db.DriverName())
		}
		db.MapperFunc(strings.ToLower)

		err = db.Get(&rsa, "SELECT * FROM cappplace;")
		if err == nil {
			t.Error("Expected no error, got ", err)
		}

		// test base type slices
		var sdest []string
		rows, err = db.Queryx("SELECT email FROM person ORDER BY email ASC;")
		if err != nil {
			t.Error(err)
		}
		err = scanAll(rows, &sdest, false)
		if err != nil {
			t.Error(err)
		}

		// test Get with base types
		var count int
		err = db.Get(&count, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if count != len(sdest) {
			t.Errorf("Expected %d == %d (count(*) vs len(SELECT ..)", count, len(sdest))
		}

		// test Get and Select with time.Time, #84
		var addedAt time.Time
		err = db.Get(&addedAt, "SELECT added_at FROM person LIMIT 1;")
		if err != nil {
			t.Error(err)
		}

		var addedAts []time.Time
		err = db.Select(&addedAts, "SELECT added_at FROM person;")
		if err != nil {
			t.Error(err)
		}

		// test it on a double pointer
		var pcount *int
		err = db.Get(&pcount, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if *pcount != count {
			t.Errorf("expected %d = %d", *pcount, count)
		}

		// test Select...
		sdest = []string{}
		err = db.Select(&sdest, "SELECT first_name FROM person ORDER BY first_name ASC;")
		if err != nil {
			t.Error(err)
		}
		expected := []string{"Ben", "Bin", "Jason", "John"}
		for i, got := range sdest {
			if got != expected[i] {
				t.Errorf("Expected %d result to be %s, but got %s", i, expected[i], got)
			}
		}

		var nsdest []sql.NullString
		err = db.Select(&nsdest, "SELECT city FROM place ORDER BY city ASC")
		if err != nil {
			t.Error(err)
		}
		for _, val := range nsdest {
			if val.Valid && val.String != "New York" {
				t.Errorf("expected single valid result to be `New York`, but got %s", val.String)
			}
		}
	})
}

func Test_EmbeddedLiterals(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE x (
				k text
			);`,
		drop: `drop table x;`,
	}

	_RunWithSchema(schema, t, func(db *DBs, t *testing.T) {
		type t1 struct {
			K *string
		}
		type t2 struct {
			Inline struct {
				F string
			}
			K *string
		}

		db.Exec(db.Rebind("INSERT INTO x (k) VALUES (?), (?), (?);"), "one", "two", "three")

		target := t1{}
		err := db.Get(&target, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target.K != "one" {
			t.Error("Expected target.K to be `one`, got ", target.K)
		}

		target2 := t2{}
		err = db.Get(&target2, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target2.K != "one" {
			t.Errorf("Expected target2.K to be `one`, got `%v`", target2.K)
		}
	})
}
