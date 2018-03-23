package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"fmt"
	"os"
	"bufio"
	"time"
)

var Delimiter = '|'
var NewLine = '\n'
var NullString = "NULL"

// https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html#data-types-storage-reqs-innodb
var DataTypeToByte = map[string]int{
	"TINYINT":   1,
	"SMALLINT":  2,
	"MEDIUMINT": 3,
	"INT":       4,
	"INTEGER":   4,
	"BIGINT":    8,
	"FLOAT":     4,
	"DOUBLE":    8,
	"DATE":      3,
	"TIME":      4,
	"DATETIME":  8,
	"TIMESTAMP": 4,
	"TEXT":      128, // this can be variable
	"VARCHAR":   128,
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%v took %v", name, elapsed)
}

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var WriteHeader = true


func main() {
	defer timeTrack(time.Now(), "mysql write to file")
	db, err := sqlx.Open("mysql", "CONNECT_STRING")
	if err != nil {
		log.Fatal(err)
	}

	table := os.Args[1]

	fp, err := os.OpenFile("output.csv", os.O_CREATE | os.O_TRUNC | os.O_RDWR, 0666)
	fatalOnErr(err)
	writer := bufio.NewWriterSize(fp, 4096)

	rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM %v", table))
	fatalOnErr(err)

	cols, _ := rows.Columns()
	if WriteHeader {
		for i, col := range cols {
			writer.WriteString(col)
			// don't write delimiter after last column
			if i < len(cols) - 1 {
				writer.WriteRune(Delimiter)
			}
		}
		writer.WriteRune(NewLine)
	}

	for rows.Next() {
		results := make(map[string]interface{})
		err := rows.MapScan(results)
		fatalOnErr(err)

		for i, col := range cols {
			colData := results[col]
			if colData == nil {
				writer.WriteString(NullString)
			} else {
				bs := colData.([]byte)
				for _, b := range bs {
					// strip out newline characters from string
					// TODO strip out other characters here
					if b != '\n' {
						writer.WriteByte(b)
					}
				}
			}
			if i < len(cols) - 1{
				writer.WriteRune(Delimiter)
			}
		}
		writer.WriteRune(NewLine)
	}

	// clear buffer
	writer.Flush()
	fmt.Println("Done!")
}
