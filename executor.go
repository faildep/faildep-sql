package sql

import (
	rsql "database/sql"
	"github.com/faildep/faildep"
)

// SQLExecutor is abstract for executable `sql`
// and try to treat sql.DB and sql.Tx as same way.
type SQLExecutor interface {
	Query(query string, args ...interface{}) (*rsql.Rows, error)
	Exec(query string, args ...interface{}) (rsql.Result, error)
}

var _ SQLExecutor = &resilientMySQLExecutor{}

type resilientMySQLExecutor struct {
	executor SQLExecutor
	readFd   *faildep.FailDep
	writeFd  *faildep.FailDep
}

func newResilientExecutor(executor SQLExecutor, readFd *faildep.FailDep, writeFd *faildep.FailDep) SQLExecutor {
	return &resilientMySQLExecutor{executor: executor, readFd: readFd, writeFd: writeFd}
}

func (e *resilientMySQLExecutor) Exec(query string, args ...interface{}) (result rsql.Result, err error) {
	err = e.writeFd.Do(func(_ *faildep.Resource) error {
		result, err = e.executor.Exec(query, args...)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (e *resilientMySQLExecutor) Query(query string, args ...interface{}) (rows *rsql.Rows, err error) {
	err = e.readFd.Do(func(_ *faildep.Resource) error {
		rows, err = e.executor.Query(query, args...)
		if err != nil {
			return err
		}
		return nil
	})
	return
}
