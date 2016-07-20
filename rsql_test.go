package sql_test

import (
	"testing"
	"time"
	"github.com/faildep/faildep-sql"
)

func TestInit(t *testing.T) {
	sql.Open("mysql", "xxx", sql.ResilientConf{
		ReadBulkhead: &sql.BulkheadConf{
			ActiveReqThreshold:   1000,
			ActiveReqCountWindow: 1 * time.Second,
		},
	})

}
