package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os/user"
	"path"
	// dbc "github.com/ppreeper/db_copy/sqlx"
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

// Dbases load
type Dbases struct {
	DB []Dbase `json:"dbases"`
}

func main() {
	usr, err := user.Current()
	checkErr(err)

	var src Dbase
	var dst Dbase

	var tbl bool
	var lnk bool
	var dbg bool
	var upd bool
	var all bool
	var schemaName string
	var tableName string
	var configFile string

	var source string
	var dest string

	// Flags
	flag.StringVar(&source, "source", "", "source database")
	flag.StringVar(&dest, "dest", "", "destination database")
	flag.StringVar(&configFile, "config.file", path.Join(usr.HomeDir, ".local/share/database", "config.json"), "config file location")

	flag.StringVar(&schemaName, "s", "", "specific schema")
	flag.StringVar(&tableName, "t", "", "specific table")
	flag.BoolVar(&all, "a", false, "all tables")
	flag.BoolVar(&tbl, "g", false, "gen table sql")
	flag.BoolVar(&lnk, "l", false, "gen table link sql")
	flag.BoolVar(&upd, "u", false, "gen update procedure")
	flag.BoolVar(&dbg, "n", false, "list tables no exec")

	flag.Parse()
	if source == "" {
		fmt.Println("No source specified")
		return
	}
	if dest == "" {
		fmt.Println("No destination specified")
		return
	}
	if schemaName == "" {
		fmt.Println("No schema specified")
		return
	}
	if all && tableName != "" {
		fmt.Println("table and all cannot be selected at same time")
		return
	}

	err = getDB(configFile, source, &src)
	checkErr(err)
	srcuri := genURI(&src)
	sdb, err := OpenDatabase(src.Driver, srcuri)
	checkErr(err)
	defer sdb.Close()

	err = getDB(configFile, dest, &dst)
	checkErr(err)
	dsturi := genURI(&dst)
	ddb, err := OpenDatabase(dst.Driver, dsturi)
	checkErr(err)
	defer ddb.Close()

	if all {
		stables, err := sdb.GetTables(src, schemaName)
		checkErr(err)
		for _, s := range stables {
			// fmt.Println(s.TableName)
			getTable(sdb, src, ddb, dst, schemaName, s.TableName, tbl, lnk, upd, dbg)
		}
	} else {
		if tableName == "" {
			fmt.Println("No table specified")
		} else {
			// fmt.Println(tableName)
			getTable(sdb, src, ddb, dst, schemaName, tableName, tbl, lnk, upd, dbg)
		}
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Print("Error:", err)
		panic(err)
	}
}

func getDB(configFile, name string, db *Dbase) (err error) {
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

// genURI generate db uri string
func genURI(db *Dbase) (uri string) {
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
func getTable(sdb *Database, src Dbase, ddb *Database, dst Dbase, schemaName, tableName string, tbl, lnk, upd, dbg bool) {
	scols, err := sdb.GetColumnDetail(dst, src, schemaName, tableName)
	checkErr(err)
	pcols, err := sdb.GetPKey(dst, src, schemaName, tableName)
	checkErr(err)
	if tbl == false && lnk == false {
		fmt.Println("Table generation not specified")
	} else {
		if tbl {
			td, tc := ddb.GenTable(dst, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(td + "\n" + tc)
			} else {
				ddb.ExecProcedure(td)
				ddb.ExecProcedure(tc)
			}
		}
		if lnk {
			ld, lc := ddb.GenLink(dst, src, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(ld + "\n" + lc)
			} else {
				ddb.ExecProcedure(ld)
				ddb.ExecProcedure(lc)
			}
		}
		if upd {
			ud, uc := ddb.GenUpdate(dst, src, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(ud + "\n" + uc)
			} else {
				ddb.ExecProcedure(ud)
				ddb.ExecProcedure(uc)
			}
		}
	}
}
