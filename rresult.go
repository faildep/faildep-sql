package sql

import (
	rsql "database/sql"
	"github.com/lysu/slb"
)

type resilientResult struct {
	rsql.Result
	writeLb *slb.LoadBalancer
}

func newResilientResult(result rsql.Result, writeLb *slb.LoadBalancer) *resilientResult {
	return &resilientResult{Result: result, writeLb: writeLb}
}

func (r *resilientResult) LastInsertId() (lastID int64, err error) {
	err = r.writeLb.Submit(func(_ *slb.Node) error {
		id, err := r.Result.LastInsertId()
		if err != nil {
			return err
		}
		lastID = id
		return nil
	})
	return
}

func (r *resilientResult) RowsAffected() (affectedRows int64, err error) {
	err = r.writeLb.Submit(func(_ *slb.Node) error {
		r2, err := r.Result.RowsAffected()
		if err != nil {
			return err
		}
		affectedRows = r2
		return nil
	})
	return
}
