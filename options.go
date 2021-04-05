package mssqlx

import (
	"github.com/rs/zerolog"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/zapadapter"
	"github.com/simukti/sqldb-logger/logadapter/zerologadapter"
	"go.uber.org/zap"
)

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

	logger     sqldblogger.Logger
	loggerOpts []sqldblogger.Option
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

// WithLogger sets logger for dbs
func WithZapLogger(logger *zap.Logger, opts ...sqldblogger.Option) Option {
	return func(o *clusterOptions) {
		o.logger = zapadapter.New(logger)
		o.loggerOpts = opts
	}
}

func WithZeroLogger(logger zerolog.Logger, opts ...sqldblogger.Option) Option {
	return func(o *clusterOptions) {
		o.logger = zerologadapter.New(logger)
		o.loggerOpts = opts
	}
}
