package sql_test

import (
	"fmt"
	"github.com/lysu/go-resilient-sql"
	"log"
)

func Example_goResilientSql() {
	// Create a Resilient configuration
	config := sql.ResilientConf{}

	// Open a new db with resilient configuration
	rdb, err := sql.Open("mysql", "root:@tcp(0.0.0.0:3306)/test?parseTime=true&loc=Local&timeout=200ms", config)
	if err != nil {
		log.Fatal(err)
	}

	// Just Do Some Query like normal..
	rows, err := rdb.Query("select 1 from test where id = 1")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	fmt.Println(rows.Next())

	// Just Start Tx and Do some update like Normal..
	tx, err := rdb.Begin()
	if err != nil {
		log.Fatal(err)
	}
	result, err := tx.Exec("update set name = 'ab' where id = 1")
	if err != nil {
		log.Fatal(err)
	}
	effectRow, err := result.RowsAffected()
	if err != nil {
		log.Fatal(effectRow)
	}
	fmt.Println(effectRow)
}
