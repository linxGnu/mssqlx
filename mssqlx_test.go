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
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
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
		pgDBs.SetMaxIdleConns(2)
		pgDBs.SetMaxOpenConns(10)
		pgDBs.SetConnMaxLifetime(3 * time.Millisecond)
	}

	if TestWMysql {
		masterDSNs := []string{mydsn, mydsn}
		slaveDSNs := []string{mydsn, mydsn}
		myDBs, _ = ConnectMasterSlaves("mysql", masterDSNs, slaveDSNs)
		myDBs.SetMaxIdleConns(2)
		myDBs.SetMaxOpenConns(10)
		myDBs.SetConnMaxLifetime(3 * time.Millisecond)
	}

	if TestWSqlite {
		masterDSNs := []string{sqdsn, sqdsn}
		slaveDSNs := []string{sqdsn, sqdsn}
		sqDBs, _ = ConnectMasterSlaves("sqlite3", masterDSNs, slaveDSNs)
		sqDBs.SetMaxIdleConns(2)
		sqDBs.SetMaxOpenConns(10)
		pgDBs.SetConnMaxLifetime(3 * time.Millisecond)
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
	f1 := strings.Replace(s.create, `"`, "`", -1)
	f2 := strings.Replace(f1, "date default now()", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", -1)
	return f2, s.drop
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
	added_at date default now()
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

func MultiExec(e sqlx.Execer, query string) {
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
	db.MustExec(db.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "Jason", "Moiron", "jmoiron@jmoiron.net")
	db.MustExecContext(context.Background(), db.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoeDNE@gmail.net")
	db.MustExecOnSlave(db.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
	db.MustExecContextOnSlave(context.Background(), db.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")

	tx := db.MustBeginx()
	tx.MustExec(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
	if db.DriverName() == "mysql" {
		tx.MustExec(tx.Rebind("INSERT INTO capplace (`COUNTRY`, `TELCODE`) VALUES (?, ?)"), "Sarf Efrica", "27")
	} else {
		tx.MustExec(tx.Rebind("INSERT INTO capplace (\"COUNTRY\", \"TELCODE\") VALUES (?, ?)"), "Sarf Efrica", "27")
	}

	tx.Commit()

	tx = db.MustBeginTx(context.Background(), nil)
	tx.MustExec(tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Joe", "1", "4444")
	tx.MustExec(tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Martin", "2", "4444")
	tx.Commit()

	txx := db.MustBegin()
	txx.Exec(db.Rebind("INSERT INTO employees (name, id) VALUES (?, ?)"), "Peter", "4444")
	if e := txx.Commit(); e != nil {
		panic(e)
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

	db, _ := sqlx.Open("postgres", "user=test1 dbname=test2 sslmode=disable")

	errT = fmt.Errorf("abc")
	if err = parseError(&wrapper{db: db, dsn: "user=test1 dbname=test2 sslmode=disable"}, errT); err != ErrNetwork {
		t.Fatal(err)
	}
}

func TestDbBalancer(t *testing.T) {
	dbB := newBalancer(nil, 0, 4, true)

	if dbB.numberOfHealthChecker != 2 {
		t.Fatal("DbBalancer init fail")
	}

	if dbB.setHealthCheckPeriod(0); dbB.getHealthCheckPeriod() != DefaultHealthCheckPeriodInMilli {
		t.Fatal("DbBalancer: setHealthCheckPeriod fail")
	}

	if dbB.setHealthCheckPeriod(200); dbB.getHealthCheckPeriod() != 200 {
		t.Fatal("DbBalancer: setHealthCheckPeriod fail")
	}

	dsn := "user=test1 dbname=test1 sslmode=disable"
	db1, _ := sqlx.Open("postgres", dsn)
	db2, _ := sqlx.Open("postgres", dsn)
	db3, _ := sqlx.Open("postgres", dsn)
	db4, _ := sqlx.Open("postgres", dsn)

	dbB.add(nil)
	dbB.add(&wrapper{db: db1, dsn: dsn})
	dbB.add(&wrapper{db: db2, dsn: dsn})
	dbB.add(&wrapper{db: db3, dsn: dsn})
	dbB.add(&wrapper{db: db4, dsn: dsn})

	if dbB.size() != 4 {
		t.Fatal("DbBalancer: add fail")
	}

	if x := dbB.get(true); x.db != db2 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(false); x.db != db2 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(true); x.db != db3 {
		t.Fatal("DbBalancer: get fail")
	}

	if x := dbB.get(false); x.db != db3 {
		t.Fatal("DbBalancer: get fail")
	} else {
		dbB.failure(x)
		if dbB.size() != 3 || (&wrapper{db: db3, dsn: dsn}).checkWsrepReady() {
			t.Fatal("DbBalancer: failure fail")
		}

		dbB.failure(nil)
		if dbB.size() != 3 || (&wrapper{db: db3, dsn: dsn}).checkWsrepReady() {
			t.Fatal("DbBalancer: failure fail")
		}
	}

	dbB.destroy()
	if dbB.size() != 0 {
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

	// ensure no nil dbs
	for _, v := range db._all {
		if v.db == nil {
			t.Fatal("Nil DB in list")
		}
	}

	// test another ping
	for _, v := range db._all {
		if e := ping(v); e != nil && e.Error() != "pq: role \"test1\" does not exist" {
			t.Fatal(e)
		}
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
	db.SetSlaveHealthCheckPeriod(311)
	if db.slaves.healthCheckPeriod != 311 {
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

	if _, c := db.GetAllSlaves(); c != 2 {
		t.Fatal("GetAllSlaves fail")
	}

	if _, c := db.GetAllMasters(); c != 3 {
		t.Fatal("GetAllMasters fail")
	}

	// test destroy master / slave
	if errs := db.DestroyMaster(); errs == nil || len(errs) != 3 {
		t.Fatal("DestroyMaster fail")
	}
	if errs := db.DestroySlave(); errs == nil || len(errs) != 2 {
		t.Fatal("DestroySlave fail")
	}

	db, _ = ConnectMasterSlaves(driver, masterDSNs, slaveDSNs, true)
	if _, c := db.GetAllMasters(); c != 3 {
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
	if _, c := db.GetAllMasters(); c != 0 {
		t.Fatal("Initialize master slave fail")
	}

	db, _ = ConnectMasterSlaves(driver, nil, nil, true)
	if _, c := db.GetAllSlaves(); c != 0 {
		t.Fatal("Initialize master slave fail")
	}
}

func TestGlobalFunc(t *testing.T) {
	// test set mapper func
	_mapperFunc(nil, nil)
	_mapperFunc([]*wrapper{}, nil)
	dbs := DBs{}
	dbs.MapperFunc(nil)
	dbs.MapperFuncMaster(nil)
	dbs.MapperFuncSlave(nil)

	// rebind
	if dbs.Rebind("SELECT * FROM test") != "" {
		t.Fatal("Test rebind fail")
	}
	dbs._all = nil
	if rb := dbs.Rebind("SELECT * FROM test"); rb != "" {
		t.Fatal("Test rebind fail", rb)
	}
	dbs._all = []*wrapper{nil}
	if rb := dbs.Rebind("SELECT * FROM test"); rb != "" {
		t.Fatal("Test rebind fail", rb)
	}

	// bindname
	dbs._all = nil
	if _, _, e := dbs.BindNamed("DELETE FROM person WHERE first_name=:first_name", "John"); e != ErrNoConnection {
		t.Fatal("Test BindNamed failed")
	}
	dbs._all = []*wrapper{}
	if _, _, e := dbs.BindNamed("DELETE FROM person WHERE first_name=:first_name", "John"); e != ErrNoConnection {
		t.Fatal("Test BindNamed failed")
	}
	dbs._all = []*wrapper{nil}
	if _, _, e := dbs.BindNamed("DELETE FROM person WHERE first_name=:first_name", "John"); e != ErrNoConnection {
		t.Fatal("Test BindNamed failed")
	}

	dsn := "user=test1 dbname=test2 sslmode=disable"
	_db1, _ := sqlx.Open("postgres", dsn)
	db1 := &wrapper{db: _db1, dsn: dsn}
	_db2, _ := sqlx.Open("postgres", dsn)
	db2 := &wrapper{db: _db2, dsn: dsn}
	_db3, _ := sqlx.Open("postgres", dsn)
	db3 := &wrapper{db: _db3, dsn: dsn}
	_db4, _ := sqlx.Open("postgres", dsn)
	db4 := &wrapper{db: _db4, dsn: dsn}

	dbB := newBalancer(nil, -1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	if _, _, err := _query(context.Background(), dbB, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_query fail", err)
	}
	dbB.destroy()

	dbB = newBalancer(nil, -1, 2, true)
	dbB.add(db1)
	dbB.add(db2)
	tmp := 1
	if _, err := _get(context.Background(), dbB, &tmp, "SELECT 1"); err != ErrNoConnectionOrWsrep {
		t.Fatal("_get fail")
	}
	dbB.destroy()

	// check mapper func to see panic occurring
	_mapperFunc([]*wrapper{db1, db2}, nil)

	// check ping
	if errs := _ping(nil); errs != nil {
		t.Fatal("Ping fail")
	}
	if errs := _ping([]*wrapper{}); errs != nil {
		t.Fatal("Ping fail")
	}

	// check close
	if errs := _close([]*wrapper{db1, db2}); errs == nil || len(errs) != 2 {
		t.Fatal("_close fail")
	}
	if errs := _close(nil); errs != nil {
		t.Fatal("_close fail")
	}
	if errs := _close([]*wrapper{}); errs != nil {
		t.Fatal("_close fail")
	}

	// check set max idle conns
	_setMaxIdleConns([]*wrapper{}, 12)
	_setMaxIdleConns(nil, 12)
	_setMaxIdleConns([]*wrapper{db3, db4}, 12)

	// check set max conns
	_setMaxOpenConns([]*wrapper{}, 16)
	_setMaxOpenConns(nil, 16)
	_setMaxOpenConns([]*wrapper{db3, db4}, 16)

	// check setConnMaxLifetime
	_setConnMaxLifetime([]*wrapper{}, 16*time.Second)
	_setConnMaxLifetime(nil, 16*time.Second)
	_setConnMaxLifetime([]*wrapper{db3, db4}, 16*time.Second)

	// check stats
	if stats := _stats(nil); stats != nil {
		t.Fatal("_stats fail")
	}
	if stats := _stats([]*wrapper{}); stats != nil {
		t.Fatal("_stats fail")
	}
	if stats := _stats([]*wrapper{db1, db2, db3, db4}); stats == nil || len(stats) != 4 {
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
		if err = db.Get(&pp, "SELECT * FROM person LIMIT 1"); err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}
		if err = db.GetOnMaster(&pp, "SELECT * FROM person LIMIT 1"); err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rows, err := db.Query("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = sqlx.StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		pps = []PersonPlus{}
		rows, err = db.QueryContext(context.Background(), "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = sqlx.StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		pps = []PersonPlus{}
		rows, err = db.QueryOnMaster("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = sqlx.StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		pps = []PersonPlus{}
		rows, err = db.QueryContextOnMaster(context.Background(), "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = sqlx.StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		pps = []PersonPlus{}
		rows1, err := db.Queryx("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows1.Next()
		err = sqlx.StructScan(rows1, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows1.Close()

		pps = []PersonPlus{}
		rows1, err = db.QueryxContext(context.Background(), "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows1.Next()
		err = sqlx.StructScan(rows1, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows1.Close()

		pps = []PersonPlus{}
		rows1, err = db.QueryxOnMaster("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows1.Next()
		err = sqlx.StructScan(rows1, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows1.Close()

		pps = []PersonPlus{}
		rows1, err = db.QueryxContextOnMaster(context.Background(), "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows1.Next()
		err = sqlx.StructScan(rows1, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows1.Close()

		_, nstmt, err := db.PrepareNamed(`SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		pps = []PersonPlus{}
		err = nstmt.Select(&pps, map[string]interface{}{"name": "Jason"})
		if err == nil {
			t.Fatal("missing destination name added_at")
		}
		nstmt.Close()

		_, nstmt, err = db.PrepareNamedOnSlave(`SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		pps = []PersonPlus{}
		err = nstmt.Select(&pps, map[string]interface{}{"name": "Jason"})
		if err == nil {
			t.Fatal("missing destination name added_at")
		}
		nstmt.Close()

		_, nstmt, err = db.PrepareNamedContext(context.Background(), `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		pps = []PersonPlus{}
		err = nstmt.Select(&pps, map[string]interface{}{"name": "Jason"})
		if err == nil {
			t.Fatal("missing destination name added_at")
		}
		nstmt.Close()

		_, nstmt, err = db.PrepareNamedContextOnSlave(context.Background(), `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		pps = []PersonPlus{}
		err = nstmt.Select(&pps, map[string]interface{}{"name": "Jason"})
		if err == nil {
			t.Fatal("missing destination name added_at")
		}
		nstmt.Close()
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
		rows, err := db.Query("SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			_, err := sqlx.SliceScan(rows)
			if err != nil {
				t.Error(err)
			}
		}

		rows, err = db.Query("SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			m := map[string]interface{}{}
			err := sqlx.MapScan(rows, m)
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

		if _, err = db.QueryRow("SELECT * FROM person LIMIT 1"); err != nil {
			t.Error("Fail query row")
		}

		if r, err := db.QueryRowOnMaster("SELECT * FROM person LIMIT 2"); err != nil || r == nil {
			t.Error("Fail query row")
		}

		if _, err = db.QueryRowContext(context.Background(), "SELECT * FROM person LIMIT 1"); err != nil {
			t.Error("Fail query row")
		}

		if r, err := db.QueryRowContextOnMaster(context.Background(), "SELECT * FROM person LIMIT 2"); err != nil || r == nil {
			t.Error("Fail query row")
		}

		if _, err = db.QueryRowx("SELECT * FROM person LIMIT 1"); err != nil {
			t.Error("Fail query row")
		}

		if r, err := db.QueryRowxOnMaster("SELECT * FROM person LIMIT 2"); err != nil || r == nil {
			t.Error("Fail query row")
		}

		if _, err = db.QueryRowxContext(context.Background(), "SELECT * FROM person LIMIT 1"); err != nil {
			t.Error("Fail query row")
		}

		if r, err := db.QueryRowxContextOnMaster(context.Background(), "SELECT * FROM person LIMIT 2"); err != nil || r == nil {
			t.Error("Fail query row")
		}

		var pp *[]Person
		if err = db.Select(pp, "SELECT * FROM person"); err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
		p1 := []Person{}
		if err = db.Select(&p1, "SELECT * FROM person"); err != nil {
			t.Error("Fail select")
		}
		if err = db.SelectOnMaster(pp, "SELECT * FROM person"); err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
		if err = db.SelectOnMaster(&p1, "SELECT * FROM person"); err != nil {
			t.Error("Fail select")
		}

		if err = db.SelectContext(context.Background(), pp, "SELECT * FROM person"); err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
		if err = db.SelectContext(context.Background(), &p1, "SELECT * FROM person"); err != nil {
			t.Error("Fail select")
		}
		if err = db.SelectContextOnMaster(context.Background(), pp, "SELECT * FROM person"); err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
		if err = db.SelectContextOnMaster(context.Background(), &p1, "SELECT * FROM person"); err != nil {
			t.Error("Fail select")
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
			AddedAt   *time.Time `db:"added_at"`
		}

		// BindNamed
		p := Person{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}
		if _, is, e := db.BindNamed("DELETE FROM person WHERE first_name=:first_name", p); e != nil {
			t.Fatal("Test BindNamed failed")
		} else if len(is) == 0 {
			t.Fatal("Test BindNamed failed")
		} else if nullString, ok := is[0].(sql.NullString); !ok {
			t.Fatal("Test BindNamed failed")
		} else if nullString.String != "ben" || !nullString.Valid {
			t.Fatal("Test BindNamed failed")
		}

		// Insert
		p = Person{
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
		rows.Close()

		p2 = &Person{}
		rows, err = db.NamedQueryContext(context.Background(), "SELECT * FROM person WHERE first_name=:first_name", p)
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
		rows.Close()

		p3 := &Person{}
		rows, err = db.NamedQueryOnMaster("SELECT * FROM person WHERE first_name=:first_name", p)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(p3)
			if err != nil {
				t.Error(err)
			}
			if p3.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + p3.FirstName.String)
			}
			if p3.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + p3.LastName.String)
			}
		}
		rows.Close()

		p3 = &Person{}
		rows, err = db.NamedQueryContextOnMaster(context.Background(), "SELECT * FROM person WHERE first_name=:first_name", p)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(p3)
			if err != nil {
				t.Error(err)
			}
			if p3.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + p3.FirstName.String)
			}
			if p3.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + p3.LastName.String)
			}
		}
		rows.Close()

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
		pl1 := Place{
			ID:   13,
			Name: sql.NullString{String: "myplace1", Valid: true},
		}

		pp := PlacePerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}
		pp1 := PlacePerson{
			FirstName: sql.NullString{String: "john", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "john@doe.com", Valid: true},
		}

		q2 := `INSERT INTO place (id, name) VALUES (1, :name)`
		if _, err = db.NamedExec(q2, pl); err != nil {
			log.Fatal(err)
		}
		if _, err = db.NamedExecContextOnSlave(context.Background(), `INSERT INTO place (id, name) VALUES (:id, :name)`, pl1); err != nil {
			log.Fatal(err)
		}

		id := 1
		pp.Place.ID = id

		q3 := `INSERT INTO placeperson (first_name, last_name, email, place_id) VALUES (:first_name, :last_name, :email, :place.id)`
		_, err = db.NamedExecOnSlave(q3, pp)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = db.NamedExecContextOnSlave(context.Background(), q3, pp1); err != nil {
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
		rows.Close()
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

		_, err = db.ExecOnSlave(db.Rebind("INSERT INTO kv (k, v) VALUES (?, ?)"), "world", 2)
		if err != nil {
			t.Error(err)
		}

		rows, err := db.Query("SELECT * FROM kv")
		if err != nil {
			t.Error(err)
		}
		for rows.Next() {
			var wt WrongTypes
			err := sqlx.StructScan(rows, &wt)
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
		if err = db.Get(&jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason"); err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		jason = Person{}
		if err = db.GetContext(context.Background(), &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason"); err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		jason = Person{}
		if err = db.GetContextOnMaster(context.Background(), &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason"); err != nil {
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

		// test advanced querying
		// test that NamedExec works with a map as well as a struct
		_, err = db.NamedExecContext(context.Background(), "INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "John",
			"last":  "Smith",
			"email": "johnsmith@allblacks.nz",
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
			t.Fatal("Should have got an error for Get on non-existent row.")
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
		err = db.Select(&sdest, "SELECT email FROM person ORDER BY email ASC;")
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
		expected := []string{"Ben", "Bin", "Jason", "John", "John"}
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

		dbx, stmt2, err := db.Preparex(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err := dbx.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 := tx.Stmtx(stmt2)
		row2 := tstmt2.QueryRowx("Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		dbx, stmt2, err = db.PreparexContext(context.Background(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err = dbx.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 = tx.Stmtx(stmt2)
		row2 = tstmt2.QueryRowx("Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		dbx, stmt2, err = db.PreparexOnSlave(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err = dbx.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 = tx.Stmtx(stmt2)
		row2 = tstmt2.QueryRowx("Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		dbx, stmt2, err = db.PreparexContextOnSlave(context.Background(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err = dbx.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 = tx.Stmtx(stmt2)
		row2 = tstmt2.QueryRowx("Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		dbx1, stmt3, err := db.Prepare(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx1, err := dbx1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tstmt3 := tx1.Stmt(stmt3)
		row3 := tstmt3.QueryRow("Jason")
		err = row3.Scan(&jason.FirstName, &jason.LastName, &jason.Email, &jason.AddedAt)
		if err != nil {
			t.Error(err)
		}
		tx1.Commit()

		dbx1, stmt3, err = db.PrepareContext(context.Background(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx1, err = dbx1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tstmt3 = tx1.Stmt(stmt3)
		row3 = tstmt3.QueryRow("Jason")
		err = row3.Scan(&jason.FirstName, &jason.LastName, &jason.Email, &jason.AddedAt)
		if err != nil {
			t.Error(err)
		}
		tx1.Commit()

		dbx1, stmt3, err = db.PrepareOnSlave(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx1, err = dbx1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tstmt3 = tx1.Stmt(stmt3)
		row3 = tstmt3.QueryRow("Jason")
		err = row3.Scan(&jason.FirstName, &jason.LastName, &jason.Email, &jason.AddedAt)
		if err != nil {
			t.Error(err)
		}
		tx1.Commit()
		isSlave := false
		for _, v := range db._slaves {
			if v.db == dbx1 {
				isSlave = true
				break
			}
		}
		if !isSlave {
			t.Error("Fail to prepare on slave")
		}

		dbx1, stmt3, err = db.PrepareContextOnSlave(context.Background(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx1, err = dbx1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tstmt3 = tx1.Stmt(stmt3)
		row3 = tstmt3.QueryRow("Jason")
		err = row3.Scan(&jason.FirstName, &jason.LastName, &jason.Email, &jason.AddedAt)
		if err != nil {
			t.Error(err)
		}
		tx1.Commit()
		isSlave = false
		for _, v := range db._slaves {
			if v.db == dbx1 {
				isSlave = true
				break
			}
		}
		if !isSlave {
			t.Error("Fail to prepare on slave")
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

func Test_ErrBadConnChecker(t *testing.T) {
	if isErrBadConn(nil) {
		t.Fatal("Check err bad conn failed")
	}

	if err := fmt.Errorf("bad connection"); !isErrBadConn(err) {
		t.Fatal("Check err bad conn failed")
	}

	if err := fmt.Errorf("invalid connection"); !isErrBadConn(err) {
		t.Fatal("Check err bad conn failed")
	}

	if err := fmt.Errorf("invalid connections"); isErrBadConn(err) {
		t.Fatal("Check err bad conn failed")
	}
}

func TestStressQueries(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE stress (
				k text,
				v integer
			);`,
		drop: `drop table stress;`,
	}

	_RunWithSchema(schema, t, func(db *DBs, t *testing.T) {
		ch := make(chan struct{}, 100)

		type StressType struct {
			K string
			V int
		}

		var wg sync.WaitGroup
		worker := func(wg *sync.WaitGroup) {
			defer wg.Done()

			for range ch {
				if _, err := db.Exec(db.Rebind("INSERT INTO stress VALUES (?, ?)"), "a", 12); err != nil {
					t.Fatal(err)
				}

				time.Sleep(time.Millisecond)

				if _, err := db.ExecContext(context.Background(), db.Rebind("INSERT INTO stress VALUES (?, ?)"), "a", 12); err != nil {
					t.Log(err)
				}

				time.Sleep(time.Millisecond)

				if _, err := db.ExecContextOnSlave(context.Background(), db.Rebind("INSERT INTO stress VALUES (?, ?)"), "a", 12); err != nil {
					t.Log(err)
				}

				time.Sleep(time.Millisecond)

				var x []StressType
				if err := db.Select(&x, "SELECT * FROM stress"); err != nil {
					t.Log(err)
				}

				var y StressType
				if err := db.Get(&y, "SELECT * FROM stress limit 1"); err != nil {
					t.Log(err)
				}

				if _, err := db.Exec(db.Rebind("DELETE FROM stress WHERE k = ?"), "c"); err != nil {
					t.Log(err)
				}

				tx, e := db.Begin()
				if e != nil {
					t.Log(e)
				} else {
					tx.Exec(db.Rebind("INSERT INTO stress VALUES (?, ?)"), "b", 13)
					tx.Exec(db.Rebind("DELETE FROM stress WHERE k = ?"), "a")
					if e = tx.Commit(); e != nil {
						tx.Rollback()
						t.Log(e)
					}
				}

				txx, e := db.BeginTxx(context.Background(), nil)
				if e != nil {
					t.Log(e)
				} else {
					txx.Exec(db.Rebind("INSERT INTO stress VALUES (?, ?)"), "c", 13)
					txx.Exec(db.Rebind("DELETE FROM stress WHERE k = ?"), "b")
					if e = txx.Commit(); e != nil {
						txx.Rollback()
						t.Log(e)
					}
				}
			}
		}

		limit := 16
		if db == sqDBs {
			limit = 2
		}

		wg.Add(limit)
		for i := 0; i < limit; i++ {
			go worker(&wg)
		}

		// notify workers
		for i := 0; i < 1000; i++ {
			ch <- struct{}{}
		}
		close(ch)

		wg.Wait()
	})
}
