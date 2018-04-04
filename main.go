package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"fmt"
	"os"
	"bufio"
	"time"
	"gopkg.in/ini.v1"
	"os/exec"
	"io/ioutil"
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


func makeConnectString(config DBConfig) string {
	return fmt.Sprintf("%v:%v@tcp(%v)/%v", config.user, config.pass, config.host, config.dbname)
}

type DBConfig struct {
	host string
	user string
	pass string
	dbname string
}

func dumpFromMysql(config DBConfig, table string, query string) {
	fmt.Println("dumping from mysql..")
	defer timeTrack(time.Now(), fmt.Sprintf("CLI dump: %v", table))
	outputFile := "dump_output.csv"
	cmd := exec.Command(
		"mysql",
		fmt.Sprintf("-h%v", config.host),
		fmt.Sprintf("-u%v", config.user),
		fmt.Sprintf("-p%v", config.pass),
		fmt.Sprintf("-A"),
		fmt.Sprintf("-q"),
		fmt.Sprintf("-e%v", query))
	output, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	ioutil.WriteFile(outputFile, []byte(output), 0666)
}

func dumpFromCursor(config DBConfig, table string, query string) {
	fmt.Println("Dumping from cursor..")
	connectString := makeConnectString(config)
	defer timeTrack(time.Now(), fmt.Sprintf("Cursor Dump: %v", table))

	db, err := sqlx.Open("mysql", connectString)
	if err != nil {
		log.Fatal(err)
	}

	fp, err := os.OpenFile("cursor_dump.csv", os.O_CREATE | os.O_TRUNC | os.O_RDWR, 0666)
	fatalOnErr(err)
	writer := bufio.NewWriterSize(fp, 4096)

	rows, err := db.Queryx(query)
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
}

func main() {
	// TODO: have this point to the real config file
	configFile := "config.ini"
	cfg, err := ini.Load(configFile)
	if err != nil {
		log.Fatal(err)
	}

	section := cfg.Section("mysql")
	host := section.Key("host").String()
	user := section.Key("user").String()
	pass := section.Key("password").String()
	dbname := section.Key("dbname").String()

	dbconfig := DBConfig{
		host,
		user,
		pass,
		dbname,
	}
	table := "rtr_prod0808.uc_orders"
	query :=fmt.Sprintf("SELECT * FROM %v", table)

	dumpFromMysql(dbconfig, table, query)
	dumpFromCursor(dbconfig, table, query)

}
