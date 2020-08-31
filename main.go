package main

import (
	"flag"
	"fmt"
	"os/user"
	"path"
	"sync"

	dbc "github.com/ppreeper/db_copy/database"
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

func checkErr(err error) {
	if err != nil {
		fmt.Print("Error:", err)
		panic(err)
	}
}

func main() {
	usr, err := user.Current()
	checkErr(err)

	var configFile string

	var src dbc.Dbase
	var source string
	var dst dbc.Dbase
	var dest string

	var schemaName string
	var tableName string
	var viewName string
	var routineName string

	var scopy bool
	var tbl bool
	var lnk bool
	var dbg bool
	var upd bool
	var all bool

	// Flags
	flag.StringVar(&source, "source", "", "source database")
	flag.StringVar(&dest, "dest", "", "destination database")
	flag.StringVar(&configFile, "config.file", path.Join(usr.HomeDir, ".local/share/database", "config.json"), "config file location")

	flag.StringVar(&schemaName, "s", "", "specific schema")
	flag.StringVar(&tableName, "t", "", "specific table")
	flag.StringVar(&viewName, "v", "", "specific view")
	flag.StringVar(&routineName, "r", "", "specific routine")

	flag.BoolVar(&scopy, "c", false, "schema copy")
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
	// read db config
	err = dbc.GetDB(configFile, source, &src)
	checkErr(err)
	// generate connection uri
	srcuri := dbc.GenURI(&src)
	// open database connection
	sdb, err := dbc.OpenDatabase(src.Driver, srcuri)
	checkErr(err)
	defer sdb.Close()

	// generate connection uri
	var dsturi string
	var ddb *dbc.Database

	if all {
		if tableName != "" || viewName != "" || routineName != "" {
			fmt.Println("all tables flag and table, view, routine flags cannot be selected at same time")
			return
		}
	} else {
		if tableName == "" && viewName == "" && routineName == "" {
			fmt.Println("all tables flag or table, view, routine flags have to be selected")
			return
		}
	}

	var sschemas []dbc.Schema
	if scopy {
		if schemaName != "" {
			var s = dbc.Schema{Name: schemaName}
			sschemas = append(sschemas, s)
		} else {
			sschemas, err = sdb.GetSchemas(src)
			checkErr(err)
		}
		// fmt.Println(sschemas)

		if all {
			for _, s := range sschemas {
				stables, err := sdb.GetTableList(src, s.Name)
				checkErr(err)
				sem := make(chan int, 8)
				var wg sync.WaitGroup
				wg.Add(len(stables))
				for _, t := range stables {
					go func(sem chan int, wg *sync.WaitGroup, t string) {
						defer wg.Done()
						sem <- 1
						sdb.GetTableSchema(src, s.Name, t, dbg)
						<-sem
					}(sem, &wg, t.Name)
				}
				wg.Wait()
				sviews, err := sdb.GetViews(src, s.Name)
				checkErr(err)
				for _, v := range sviews {
					sdb.GetView(src, s.Name, v, dbg)
				}
				sroutines, err := sdb.GetRoutines(src, s.Name)
				checkErr(err)
				for _, r := range sroutines {
					sdb.GetRoutine(src, s.Name, r, dbg)
				}
			}
		} else {
			for _, s := range sschemas {
				if tableName != "" {
					sdb.GetTableSchema(src, s.Name, tableName, dbg)
				}
				if viewName != "" {
					v, err := sdb.GetViewSchema(src, s.Name, viewName)
					checkErr(err)
					sdb.GetView(src, s.Name, v, dbg)
				}
				if routineName != "" {
					r, err := sdb.GetRoutineSchema(src, s.Name, routineName)
					checkErr(err)
					sdb.GetRoutine(src, s.Name, r, dbg)
				}
			}
		}
	} else {
		if dest == "" {
			fmt.Println("No destination specified")
			return
		}
		// read db config
		err = dbc.GetDB(configFile, dest, &dst)
		checkErr(err)
		// generate connection uri
		dsturi = dbc.GenURI(&dst)
		// open database connection
		ddb, err = dbc.OpenDatabase(dst.Driver, dsturi)
		checkErr(err)
		defer ddb.Close()

		if schemaName == "" {
			fmt.Println("No schema specified")
			return
		}

		if all {
			stables, err := sdb.GetTableList(src, schemaName)
			checkErr(err)
			for _, s := range stables {
				dbc.GetTable(sdb, src, ddb, dst, schemaName, s.Name, tbl, lnk, upd, dbg)
			}
		} else {
			if tableName == "" {
				fmt.Println("No table specified")
			} else {
				dbc.GetTable(sdb, src, ddb, dst, schemaName, tableName, tbl, lnk, upd, dbg)
			}
		}
	}
	fmt.Printf("\n")
}
