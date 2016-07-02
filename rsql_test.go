package sql_test

import (
	"github.com/lysu/go-resilient-mysql"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	sql.Open("mysql", "xxx", sql.ResilientConf{
		ReadBulkhead: &sql.BulkheadConf{
			ActiveReqThreshold:   1000,
			ActiveReqCountWindow: 1 * time.Second,
		},
	})

}
