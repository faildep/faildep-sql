package sql

import (
	rsql "database/sql"
	"github.com/faildep/faildep"
)

type resilientResult struct {
	rsql.Result
	writeFd *faildep.FailDep
}

func newResilientResult(result rsql.Result, writeFd *faildep.FailDep) *resilientResult {
	return &resilientResult{Result: result, writeFd: writeFd}
}

func (r *resilientResult) LastInsertId() (lastID int64, err error) {
	err = r.writeFd.Do(func(_ *faildep.Resource) error {
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
	err = r.writeFd.Do(func(_ *faildep.Resource) error {
		r2, err := r.Result.RowsAffected()
		if err != nil {
			return err
		}
		affectedRows = r2
		return nil
	})
	return
}
