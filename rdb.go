package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
	"time"
)

const dummyNode = "dummy"

var _ Executor = &ResilientDB{}

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

type ResilientDB struct {
	*rsql.DB
	readLb  *slb.LoadBalancer
	writeLb *slb.LoadBalancer
}

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

func (d *ResilientDB) Exec(query string, args ...interface{}) (rsql.Result, error) {
	return newResilientExecutor(d.DB, d.readLb, d.writeLb).Exec(query, args...)
}

func (d *ResilientDB) Query(query string, args ...interface{}) (*rsql.Rows, error) {
	return newResilientExecutor(d.DB, d.readLb, d.writeLb).Query(query, args...)
}
