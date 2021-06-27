package database

import (
	"fmt"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgresql driver
	"go.uber.org/zap"
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/mattn/go-sqlite3" //sqlite3
	// _ "gopkg.in/mgo.v2" //Mongo
	// _ "github.com/go-sql-driver/mysql/" //MySql
	// _ "github.com/nakagami/firebirdsql" //Firebird Sql
	// _ "bitbucket.org/phiggins/db2cli" //DB2
)

func (db *Database) checkErr(err error) {
	if err != nil {
		db.Log.Errorw(err.Error())
	}
}

// func (db *Database) fatalErr(err error) {
// 	if err != nil {
// 		db.Log.Fatalw(err.Error())
// 	}
// }

// Conn struct
type Conn struct {
	Source  *Database
	Dest    *Database
	SSchema string
	DSchema string
}

// Database struct contains sql pointer
type Database struct {
	Name     string   `json:"name,omitempty"`
	Driver   string   `json:"driver,omitempty"`
	Host     string   `json:"host,omitempty"`
	Port     string   `json:"port,omitempty"`
	Database string   `json:"database,omitempty"`
	Schema   []string `json:"schema,omitempty"`
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
	PoolSize string   `json:"poolsize,omitempty"`
	Drive    string   `json:"drive,omitempty"`
	SubDir   string   `json:"subdir,omitempty"`
	URI      string
	Log      *zap.SugaredLogger
	*sqlx.DB
}

// OpenDatabase open database
func OpenDatabase(db Database) (*Database, error) {
	// fmt.Println(driver, dburi)
	var err error
	db.GetURI()
	db.DB, err = sqlx.Open(db.Driver, db.URI)
	if err != nil {
		db.Log.Info("Open sql (%v): %v", db.URI, err)
	}
	if err = db.Ping(); err != nil {
		db.Log.Info("Ping sql: %v", err)
	}
	db.Log.Info("", "db", db)
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

// GenURI generate db uri string
func (db *Database) GetURI() {
	// fmt.Println(db.Driver)
	if db.Driver == "postgres" {
		if db.Port == "" {
			db.URI = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + ":5432/" + db.Database + "?sslmode=disable"
		} else {
			db.URI = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + ":" + db.Port + "/" + db.Database + "?sslmode=disable"
		}
	}
	if db.Driver == "mssql" {
		db.URI = "server=" + db.Host + ";user id=" + db.Username + ";password=" + db.Password + ";database=" + db.Database + ";encrypt=disable;connection timeout=7200;keepAlive=30"
	}
}

func GenSubDir(db *Database) (subdir string) {
	subdir = fmt.Sprintf("%s:\\%s", db.Drive, db.SubDir)
	return
}

// Utilities

// function to reverse the given integer array
// func reverse(numbers []int) []int {

// 	var length = len(numbers) // getting length of an array

// 	for i := 0; i < length/2; i++ {
// 		temp := numbers[i]
// 		numbers[i] = numbers[length-i-1]
// 		numbers[length-i-1] = temp
// 	}

// 	return numbers
// }

// func removeColumn(slice []Column, s int) []Column {
// 	return append(slice[:s], slice[s+1:]...)
// }

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
