package database

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgresql driver
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/mattn/go-sqlite3" //sqlite3
	// _ "gopkg.in/mgo.v2" //Mongo
	// _ "github.com/go-sql-driver/mysql/" //MySql
	// _ "github.com/nakagami/firebirdsql" //Firebird Sql
	// _ "bitbucket.org/phiggins/db2cli" //DB2
)

func checkErr(err error) {
	if err != nil {
		fmt.Print("Error:", err)
		panic(err)
	}
}

// Database struct contains sql pointer
type Database struct {
	*sqlx.DB
}

// Dbases array of Dbase
type Dbases struct {
	DB []Dbase `json:"dbases"`
}

// Dbase for loading from json
type Dbase struct {
	Name     string   `json:"name"`
	Driver   string   `json:"driver"`
	Host     string   `json:"host"`
	Port     string   `json:"port"`
	Database string   `json:"database"`
	Schema   []string `json:"schema"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	PoolSize string   `json:"poolsize"`
}

// OpenDatabase open database
func OpenDatabase(driver string, dburi string) (*Database, error) {
	// fmt.Println(driver, dburi)
	var err error
	db := Database{}
	db.DB, err = sqlx.Open(driver, dburi)
	if err != nil {
		log.Printf("Open sql (%v): %v", dburi, err)
	}
	if err = db.Ping(); err != nil {
		log.Printf("Ping sql: %v", err)
	}
	return &db, err
}

//ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	fmt.Println(q)
	_, err := db.Exec(q)
	if err != nil {
		panic(err)
	}
}

// GetDB loads db config from json
func GetDB(configFile, name string, db *Dbase) (err error) {
	content, err := ioutil.ReadFile(configFile)
	checkErr(err)
	var conf Dbases
	err = json.Unmarshal(content, &conf)
	checkErr(err)
	for _, dbase := range conf.DB {
		// fmt.Println(dbase)
		if dbase.Name == name {
			*db = dbase
			err = nil
		}
	}
	return err
}

// GenURI generate db uri string
func GenURI(db *Dbase) (uri string) {
	// fmt.Println(db.Driver)
	if db.Driver == "postgres" {
		if db.Port == "" {
			uri = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + ":5432/" + db.Database + "?sslmode=disable"
		} else {
			uri = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + ":" + db.Port + "/" + db.Database + "?sslmode=disable"
		}
	}
	if db.Driver == "mssql" {
		uri = "server=" + db.Host + ";user id=" + db.Username + ";password=" + db.Password + ";database=" + db.Database + ";encrypt=disable;connection timeout=7200;keepAlive=30"
	}
	return uri
}

// Utilities

// function to reverse the given integer array
func reverse(numbers []int) []int {

	var length = len(numbers) // getting length of an array

	for i := 0; i < length/2; i++ {
		temp := numbers[i]
		numbers[i] = numbers[length-i-1]
		numbers[length-i-1] = temp
	}

	return numbers
}

func removeColumn(slice []Column, s int) []Column {
	return append(slice[:s], slice[s+1:]...)
}

func trimCols(cols []Column, pkey []PKey) []Column {
	var clist []int
	var ilist []int
	for k, c := range cols {
		clist = append(clist, k)
		for _, p := range pkey {
			if c.ColumnName == p.PKey {
				ilist = append(ilist, k)
			}
		}
	}
	// fmt.Println(clist)
	// fmt.Println(ilist)

	var collist []int
	for _, c := range clist {
		if func(e int, ee []int) bool {
			for _, i := range ee {
				if e == i {
					return false
				}
			}
			return true
		}(c, ilist) {
			collist = append(collist, c)
		}
	}
	// fmt.Println(collist)

	var columns []Column
	for _, c := range collist {
		columns = append(columns, cols[c])
	}
	// fmt.Println(columns)
	return columns
}
