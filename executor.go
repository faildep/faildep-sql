package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
)

type Executor interface {
	Query(query string, args ...interface{}) (*rsql.Rows, error)
	Exec(query string, args ...interface{}) (rsql.Result, error)
}

var _ Executor = &resilientMySQLExecutor{}

type resilientMySQLExecutor struct {
	executor Executor
	readLb   *slb.LoadBalancer
	writeLb  *slb.LoadBalancer
}

func newResilientExecutor(executor Executor, readLb *slb.LoadBalancer, writeLb *slb.LoadBalancer) Executor {
	return &resilientMySQLExecutor{executor: executor, readLb: readLb, writeLb: writeLb}
}

func (e *resilientMySQLExecutor) Exec(query string, args ...interface{}) (result rsql.Result, err error) {
	err = e.writeLb.Submit(func(_ *slb.Node) error {
		result, err = e.executor.Exec(query, args...)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (e *resilientMySQLExecutor) Query(query string, args ...interface{}) (rows *rsql.Rows, err error) {
	err = e.readLb.Submit(func(_ *slb.Node) error {
		rows, err = e.executor.Query(query, args...)
		if err != nil {
			return err
		}
		return nil
	})
	return
}
