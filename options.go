package mssqlx

import "database/sql"

// ReadQuerySource enums.
type ReadQuerySource int

const (
	// ReadQuerySourceSlaves setting indicates: read-queries will be distributed only among slaves.
	//
	// Note: there is no option for Master. One could use functions like `QueryMaster`, etc
	// to query from masters only.
	ReadQuerySourceSlaves ReadQuerySource = iota

	// ReadQuerySourceAll setting indicates: read-queries will be distributed among both masters and slaves.
	//
	// Note: this is default setting.
	ReadQuerySourceAll
)

type clusterOptions struct {
	instantiate     Instantiate
	readQuerySource ReadQuerySource
	isWsrep         bool
}

// Option setter.
type Option func(*clusterOptions)

// Instantiate db.
type Instantiate func(driverName, dsn string) (*sql.DB, error)

// WithWsrep indicates galera/wsrep cluster
func WithWsrep() Option {
	return func(o *clusterOptions) {
		o.isWsrep = true
	}
}

// WithReadQuerySource sets default sources for read-queries.
func WithReadQuerySource(source ReadQuerySource) Option {
	return func(o *clusterOptions) {
		o.readQuerySource = source
	}
}

// WithDBInstantiate overwrite instantiate for db conn.
func WithDBInstantiate(f Instantiate) Option {
	return func(o *clusterOptions) {
		o.instantiate = f
	}
}
