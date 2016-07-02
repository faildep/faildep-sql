package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
)

var _ Executor = &ResilientTx{}

type ResilientTx struct {
	*rsql.Tx
	writeLb *slb.LoadBalancer
}

func newResilientTx(tx *rsql.Tx, writeLb *slb.LoadBalancer) *ResilientTx {
	return &ResilientTx{
		Tx:      tx,
		writeLb: writeLb,
	}
}

func (t *ResilientTx) Exec(query string, args ...interface{}) (rsql.Result, error) {
	return newResilientExecutor(t.Tx, t.writeLb, t.writeLb).Exec(query, args...)
}

func (t *ResilientTx) Query(query string, args ...interface{}) (*rsql.Rows, error) {
	return newResilientExecutor(t.Tx, t.writeLb, t.writeLb).Query(query, args...)
}

func (t *ResilientTx) Commit() error {
	return t.writeLb.Submit(func(_ *slb.Node) error {
		return t.Tx.Commit()
	})
}

func (t *ResilientTx) Rollback() error {
	return t.writeLb.Submit(func(_ *slb.Node) error {
		return t.Tx.Rollback()
	})
}
