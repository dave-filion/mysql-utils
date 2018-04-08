package main
//TODO make this mysql package

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"fmt"
	"time"
	"strconv"
	"os"
	"bufio"
	pb2 "gopkg.in/cheggaaa/pb.v1"
	"bytes"
	"strings"
	"gopkg.in/ini.v1"
	"path"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws"
)


var Delimiter = '|'
var NewLine = '\n'
var NullString = "NULL"
var OutputDir = "./output"

var S3Bucket = "mybucket"

type Field struct {
	FieldName string
	DataType string
}

type CreateStatement struct {
	TableName string
	Fields []Field
}

func makeS3Key(table Table, basefile string) string {
	return fmt.Sprintf("tablecopy/%v/%v", table.FullName(), basefile	)
}

func (cs *CreateStatement) toString() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE ")
	buf.WriteString(cs.TableName)
	buf.WriteString(" (\n")

	var contents []string
	for _, f := range cs.Fields {
		contents = append(contents, fmt.Sprintf(" %v %v", f.FieldName, f.DataType))
	}
	buf.WriteString(strings.Join(contents, ",\n"))
	buf.WriteString("\n)")
	return buf.String()
}

type Table struct {
	Schema    string
	TableName string
}

func NewTable(fullname string) Table {
	pieces := strings.Split(fullname, ".")
	return Table{
		Schema:pieces[0],
		TableName:pieces[1],
	}
}

func (t *Table) FullName() string {
	return fmt.Sprintf("%v.%v", t.Schema, t.TableName)
}

func (t *Table) Filename(timestamp bool) string {
	return fmt.Sprintf("%v.csv", t.FullName())
}

type TableSize struct {
	SizeMB  float64
	NumRows int
}

type TableSizeDB struct {
	SizeMBString string `db:"size"`
	NumRows      string `db:"table_rows"`
}

// Converts strings in tablesize db to numerical values
func (t *TableSizeDB) toTableSize() *TableSize {
	numRows, err := strconv.Atoi(t.NumRows)
	if err != nil {
		log.Fatal(err)
	}

	sizeMb, err := strconv.ParseFloat(t.SizeMBString, 64)
	if err != nil {
		log.Fatal(err)
	}

	return &TableSize{
		NumRows: numRows,
		SizeMB:  sizeMb,
	}
}



func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%v took %v\n", name, elapsed)
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
	host   string
	user   string
	pass   string
	dbname string
}

func getMySQLTableSize(db *sqlx.DB, table Table) *TableSize {
	sizeQuery := fmt.Sprintf(`
		SELECT 
			round(((data_length + index_length) / 1024 / 1024), 2) size,
			table_rows
		FROM information_schema.TABLES WHERE table_schema = "%v"
		AND table_name = "%v"
	`, table.Schema, table.TableName)

	var tablesize TableSizeDB
	err := db.Get(&tablesize, sizeQuery)
	fatalOnErr(err)

	return tablesize.toTableSize()
}


func DumpTableToFile(db *sqlx.DB, table Table, outfile string) {
	defer timeTrack(time.Now(), fmt.Sprintf("Cursor Dump: %v", table))

	fmt.Printf("Dumping %v to %v\n", table.FullName(), outfile)

	tableSize := getMySQLTableSize(db, table)
	fmt.Printf("Table size: %.2fMB / %.2fGB\n", tableSize.SizeMB, (tableSize.SizeMB / 1024))
	fmt.Printf("Total rows: %v\n", tableSize.NumRows)

	fp, err := os.OpenFile(outfile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	fatalOnErr(err)

	writer := bufio.NewWriterSize(fp, 4096)

	rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM %v", table.FullName()))
	fatalOnErr(err)

	// TODO, could also call information schema if columnTypes isnt
	// enough data

	// get column metadata
	// TODO, map colTypes to snowflake data types
	// TODO: create snowflake create statement
	//colTypes, err := rows.ColumnTypes()
	//fatalOnErr(err)
	//for _, colType := range colTypes {
	//	colName, colType := colType.Name(), colType.DatabaseTypeName()
	//}

	cols, _ := rows.Columns()
	if WriteHeader {
		for i, col := range cols {
			writer.WriteString(col)
			// don't write delimiter after last column
			if i < len(cols)-1 {
				writer.WriteRune(Delimiter)
			}
		}
		writer.WriteRune(NewLine)
	}

	// make progress bar
	pb := pb2.New(tableSize.NumRows)
	pb.Start()

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
			if i < len(cols)-1 {
				writer.WriteRune(Delimiter)
			}
		}
		writer.WriteRune(NewLine)
		pb.Increment()
	}

	// clear buffer
	writer.Flush()
	pb.FinishPrint(fmt.Sprintf("Finished dumping %v to %v", table.FullName(), outfile))
}

func uploadToS3(file string, table Table) {
	defer timeTrack(time.Now(), "Uploading to s3")
	// TODO, this could be global?
	sess := session.Must(session.NewSession())
	uploader := s3manager.NewUploader(sess)

	key := makeS3Key(table, path.Base(file))
	fmt.Printf("S3 Key: ", key)

	f, err := os.Open(file)
	fatalOnErr(err)
	defer f.Close()

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3Bucket),
		Key: aws.String(key),
		Body: f,
	})
	fatalOnErr(err)

	fmt.Printf("file uploaded to, %s\n", result.Location)
}

func DumpTableLoop(db *sqlx.DB, pool chan Table) {
	for {

	}
}

func DumpTableList(db *sqlx.DB, outputDir string, tables []Table) {
	pool := make(chan Table, 4)

	// start async pool loop
	go DumpTableLoop(db, pool)

	for _, table := range tables {
		outfile := path.Join(outputDir, table.Filename(false))
		pool<- table
	}
}



func main() {
	defer timeTrack(time.Now(), "Total job")
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

	tableNames := []string {
	}

	var tables []Table
	for _, s := range tableNames {
		tables = append(tables, NewTable(s))
	}

	connectString := makeConnectString(dbconfig)
	db, err := sqlx.Open("mysql", connectString)
	if err != nil {
		log.Fatal(err)
	}

	DumpTableList(db, OutputDir, tables)


	//uploadToS3(outputFile, table)

	// copyIntoSnowflake

	// delete local file

}
