package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/kr/pretty"
	dbc "github.com/ppreeper/db_copy/sqlx"
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/jmoiron/sqlx"
	// _ "github.com/lib/pq"
	// _ "github.com/mattn/go-sqlite3" //sqlite3
	// _ "gopkg.in/mgo.v2" //Mongo
	// _ "github.com/go-sql-driver/mysql/" //MySql
	// _ "github.com/nakagami/firebirdsql" //Firebird Sql
	// _ "bitbucket.org/phiggins/db2cli" //DB2
)

// Dbase for loading from json
// type Dbase struct {
// 	Name     string `json:"name"`
// 	Driver   string `json:"driver"`
// 	Host     string `json:"host"`
// 	Port     string `json:"port"`
// 	Database string `json:"database"`
// 	Schema   string `json:"schema"`
// 	Username string `json:"username"`
// 	Password string `json:"password"`
// }

// Dbases load
type Dbases struct {
	DB []dbc.Dbase `json:"dbases"`
}

var src dbc.Dbase
var dst dbc.Dbase

var tbl bool
var lnk bool
var all bool
var tableName string

var source string
var dest string

func init() {
	// Flags
	flag.StringVar(&source, "source", "", "source database")
	flag.StringVar(&dest, "dest", "", "destination database")

	flag.StringVar(&tableName, "t", "", "specific table")
	flag.BoolVar(&all, "a", false, "all tables")
	flag.BoolVar(&tbl, "g", false, "gen table sql")
	flag.BoolVar(&lnk, "l", false, "gen table link sql")

	flag.Parse()
}

func main() {
	pretty.Println("source:", source, "dest:", dest, "tableName:", strings.ToUpper(tableName), "tbl:", tbl, "lnk:", lnk)
	if source == "" {
		fmt.Println("No source specified")
		return
	}
	if dest == "" {
		fmt.Println("No source specified")
		return
	}

	err := getDB(source, &src)
	checkErr(err)
	srcuri := genURI(&src)
	sdb, err := dbc.OpenDatabase(src.Driver, srcuri)
	checkErr(err)
	defer sdb.Close()

	err = getDB(dest, &dst)
	checkErr(err)
	dsturi := genURI(&dst)
	ddb, err := dbc.OpenDatabase(dst.Driver, dsturi)
	checkErr(err)
	defer ddb.Close()

	if tableName == "" {
		fmt.Println("No table specified")
		return
	} else {
		scols, err := sdb.GetColumnDetail(dst, src, strings.ToUpper(tableName))
		checkErr(err)
		pcols, err := sdb.GetPKey(dst, src, strings.ToUpper(tableName))
		checkErr(err)
		if tbl == false && lnk == false {
			fmt.Println("Table generation not specified")
			return
		} else {
			if tbl {
				ddb.GenTable(dst, strings.ToUpper(tableName), scols, pcols)
			}
			if lnk {
				ddb.GenLink(dst, src, strings.ToUpper(tableName), scols, pcols)
			}
		}
	}

	// stables, err := sdb.GetTables(src)
	// checkErr(err)
	// pretty.Println(stables)

	// dtables, err := ddb.GetTables(dst)
	// checkErr(err)
	// pretty.Println(dtables)

	// for _, s := range stables {
	// 	pretty.Println(s.TableName)
	// }
}

func checkErr(err error) {
	if err != nil {
		fmt.Print("Error:", err)
		panic(err)
	}
}

func getDB(name string, db *dbc.Dbase) (err error) {
	content, err := ioutil.ReadFile("config.json")
	checkErr(err)
	var conf Dbases
	err = json.Unmarshal(content, &conf)
	checkErr(err)
	for _, dbase := range conf.DB {
		if dbase.Name == name {
			*db = dbase
			err = nil
		}
	}
	return err
}

// genURI generate db uri string
func genURI(db *dbc.Dbase) (uri string) {
	if db.Driver == "postgres" {
		uri = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + "/" + db.Database + "?sslmode=disable"
	}
	if db.Driver == "mssql" {
		uri = "server=" + db.Host + ";user id=" + db.Username + ";password=" + db.Password + ";database=" + db.Database + ";encrypt=disable;connection timeout=7200;keepAlive=30"
	}
	return uri
}
