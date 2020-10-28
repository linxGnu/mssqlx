package mssqlx

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
	isWsrep         bool
	readQuerySource ReadQuerySource
}

// Option setter.
type Option func(*clusterOptions)

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
