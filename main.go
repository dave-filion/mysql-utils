package mysql_utils

import (
	"database/sql"
	"github.com/jimsmart/schema"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"fmt"
)

func main() {
	// username:password@protocol(address)/dbname?param=value
	db, err := sql.Open("mysql", "user:password@/dbname")
	if err != nil {
		log.Fatal(err)
	}

	table := "example_table"

	tnames, err := schema.TableNames(db)
	if err != nil {
		log.Fatal(err)
	}

	tcols, err := schema.Table(db, table)
	for i := range tcols {
		fmt.Printf("Column: %v %v", tcols[i].Name(), tcols[i].DatabaseTypeName())
		// can use .ScanType to know which Go type to scan in as
	}

	for i := range tnames {
		fmt.Printf("Table: %v\n", tnames[i])
	}

	row := db.QueryRow("SELECT COUNT(*) FROM ?", table)

	var numRows int
	err = row.Scan(&numRows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("num rows:", numRows)
}
