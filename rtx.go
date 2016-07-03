package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
)

var _ SQLExecutor = &ResilientTx{}

// ResilientTx is an in-progress database transaction.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the
// transaction fail with ErrTxDone.
//
// The statements prepared for a transaction by calling
// the transaction's Prepare or Stmt methods are closed
// by the call to Commit or Rollback.
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

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (t *ResilientTx) Exec(query string, args ...interface{}) (rsql.Result, error) {
	rawResult, err := newResilientExecutor(t.Tx, t.writeLb, t.writeLb).Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return newResilientResult(rawResult, t.writeLb), nil
}

// Query executes a query that returns rows, typically a SELECT.
func (t *ResilientTx) Query(query string, args ...interface{}) (*rsql.Rows, error) {
	return newResilientExecutor(t.Tx, t.writeLb, t.writeLb).Query(query, args...)
}

// Commit commits the transaction.
func (t *ResilientTx) Commit() error {
	return t.writeLb.Submit(func(_ *slb.Node) error {
		return t.Tx.Commit()
	})
}

// Rollback aborts the transaction.
func (t *ResilientTx) Rollback() error {
	return t.writeLb.Submit(func(_ *slb.Node) error {
		return t.Tx.Rollback()
	})
}
