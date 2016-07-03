package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
	"time"
)

const dummyNode = "dummy"

var _ SQLExecutor = &ResilientDB{}

// ResilientConf tweak Resilient SQL configuration.
type ResilientConf struct {
	ReadBulkhead        *BulkheadConf
	ReadCircuitBreaker  *CircuitBreakerConf
	WriteBulkhead       *BulkheadConf
	WriteCircuitBreaker *CircuitBreakerConf
}

type CircuitBreakerConf struct {
	SuccessiveFailThreshold uint
	TrippedBaseTime         time.Duration
	TrippedTimeoutMax       time.Duration
	TrippedBackOff          slb.BackOff
}

type BulkheadConf struct {
	ActiveReqThreshold   uint64
	ActiveReqCountWindow time.Duration
}

// ResilientDB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type ResilientDB struct {
	*rsql.DB
	readLb  *slb.LoadBalancer
	writeLb *slb.LoadBalancer
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a
// database name and connection information.
func Open(driverName, dataSourceName string, conf ResilientConf) (*ResilientDB, error) {
	rOpt := []func(lb *slb.LoadBalancer){}
	if conf.ReadCircuitBreaker != nil {
		rOpt = append(rOpt, slb.WithCircuitBreaker(
			conf.ReadCircuitBreaker.SuccessiveFailThreshold,
			conf.ReadCircuitBreaker.TrippedBaseTime,
			conf.ReadCircuitBreaker.TrippedTimeoutMax,
			conf.ReadCircuitBreaker.TrippedBackOff,
		))
	}
	if conf.ReadBulkhead != nil {
		rOpt = append(rOpt, slb.WithBulkhead(
			conf.ReadBulkhead.ActiveReqThreshold,
			conf.ReadBulkhead.ActiveReqCountWindow,
		))
	}
	wOpt := []func(lb *slb.LoadBalancer){}
	if conf.WriteCircuitBreaker != nil {
		wOpt = append(wOpt, slb.WithCircuitBreaker(
			conf.WriteCircuitBreaker.SuccessiveFailThreshold,
			conf.WriteCircuitBreaker.TrippedBaseTime,
			conf.WriteCircuitBreaker.TrippedTimeoutMax,
			conf.WriteCircuitBreaker.TrippedBackOff,
		))
	}
	if conf.WriteBulkhead != nil {
		wOpt = append(wOpt, slb.WithBulkhead(
			conf.WriteBulkhead.ActiveReqThreshold,
			conf.WriteBulkhead.ActiveReqCountWindow,
		))
	}
	rlb := slb.NewLoadBalancer([]string{dummyNode}, wOpt...)
	wlb := slb.NewLoadBalancer([]string{dummyNode}, wOpt...)
	db, err := rsql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &ResilientDB{DB: db, readLb: rlb, writeLb: wlb}, nil
}

// Begin starts a transaction. The isolation level is dependent on
// the driver.
func (d ResilientDB) Begin() (rtx *ResilientTx, err error) {
	err = d.writeLb.Submit(func(_ *slb.Node) error {
		tx, err := d.DB.Begin()
		if err != nil {
			return err
		}
		rtx = newResilientTx(tx, d.writeLb)
		return nil
	})
	return
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (d *ResilientDB) Exec(query string, args ...interface{}) (rsql.Result, error) {
	rawResult, err := newResilientExecutor(d.DB, d.readLb, d.writeLb).Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return newResilientResult(rawResult, d.writeLb), nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (d *ResilientDB) Query(query string, args ...interface{}) (*rsql.Rows, error) {
	return newResilientExecutor(d.DB, d.readLb, d.writeLb).Query(query, args...)
}
