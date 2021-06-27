package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"

	dbc "github.com/ppreeper/db_copy/database"
	"github.com/schollz/progressbar/v3"

	"go.uber.org/zap"
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

var log *zap.SugaredLogger

func checkErr(err error) {
	if err != nil {
		log.Errorw(err.Error())
	}
}

func fatalErr(err error) {
	if err != nil {
		log.Fatalw(err.Error())
	}
}

func dbOpen(d dbc.Database) *dbc.Database {
	db, err := dbc.OpenDatabase(dbc.Database{
		Name:     d.Name,
		Driver:   d.Driver,
		Host:     d.Host,
		Port:     d.Port,
		Database: d.Database,
		Schema:   d.Schema,
		Username: d.Username,
		Password: d.Password,
		PoolSize: d.PoolSize,
		Log:      log,
	})
	checkErr(err)
	return db
}

type Config struct {
	Source      string
	Dest        string
	SSchemaName string
	DSchemaName string
	TableName   string
	Table       bool
	ViewName    string
	View        bool
	RoutineName string
	Routine     bool
	IndexName   string
	Index       bool
	FilterDef   string
	Filter      *regexp.Regexp
	JobCount    int
	Link        bool
	Debug       bool
	Update      bool
	All         bool
}

func main() {
	logName := "dbcopy.log"
	_, err := os.Stat(logName)
	if os.IsNotExist(err) {
		file, err := os.Create(logName)
		fatalErr(err)
		defer file.Close()
	}
	err = os.Truncate(logName, 0)
	checkErr(err)
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{logName}
	logger, _ := cfg.Build()
	log = logger.Sugar()

	config := Config{
		JobCount: 8,
	}

	// Flags
	flag.StringVar(&config.Source, "source", "", "source database")
	flag.StringVar(&config.SSchemaName, "ss", "", "source schema")
	flag.StringVar(&config.Dest, "dest", "", "destination database or file:")
	flag.StringVar(&config.DSchemaName, "ds", "", "dest schema")

	flag.StringVar(&config.TableName, "table", "", "specific table")
	flag.BoolVar(&config.Table, "t", false, "gen table sql")

	flag.StringVar(&config.ViewName, "view", "", "specific view")
	flag.BoolVar(&config.View, "v", false, "gen view sql")

	flag.StringVar(&config.RoutineName, "routine", "", "specific routine")
	flag.BoolVar(&config.Routine, "r", false, "gen routine sql")

	flag.StringVar(&config.IndexName, "index", "", "specific index")
	flag.BoolVar(&config.Index, "i", false, "gen index sql")

	flag.BoolVar(&config.All, "all", false, "all tables")

	flag.BoolVar(&config.Link, "l", false, "gen table link sql")
	flag.BoolVar(&config.Update, "u", false, "gen update procedure")

	flag.StringVar(&config.FilterDef, "f", "", "regex filter")

	flag.BoolVar(&config.Debug, "n", false, "no-op debug")
	flag.IntVar(&config.JobCount, "j", 8, "job count")

	flag.Parse()

	log.Infow("start", "config", config)

	fmt.Println("Source: ", config.Source, "SSchemaName: ", config.SSchemaName, "Dest: ", config.Dest, "DSchemaName: ", config.DSchemaName)
	fmt.Println("Table: ", config.Table, "TableName: ", config.TableName)
	fmt.Println("View: ", config.View, "ViewName: ", config.ViewName)
	fmt.Println("Routine: ", config.Routine, "RoutineName: ", config.RoutineName)
	fmt.Println("Index: ", config.Index, "IndexName: ", config.IndexName)
	fmt.Println("All: ", config.All, "Link: ", config.Link, "Update: ", config.Update, "Debug: ", config.Debug)

	config.Filter = regexp.MustCompilePOSIX(config.FilterDef)

	// Config File
	var c Conf
	c.getConf("config.yml")

	src := c.getDB(config.Source)
	dst := c.getDB(config.Dest)

	//////////
	// check all or table,view,routine
	//////////
	if config.All && config.TableName != "" || config.ViewName != "" || config.RoutineName != "" || config.IndexName != "" {
		fmt.Println("all tables flag and table, view, routine, index, flags cannot be selected at same time")
		return

	}
	if !config.All && config.TableName == "" && config.ViewName == "" && config.RoutineName == "" && config.IndexName == "" {
		fmt.Println("all tables flag or table, view, routine, index, flags have to be selected")
		return
	}

	//////////
	// Source DB
	//////////
	if config.Source == "" {
		fmt.Println("No source specified")
		return
	}

	//////////
	// get schemas
	//////////

	// open source database connection
	sdb := dbOpen(src)
	defer sdb.Close()

	var sSchemas []dbc.Schema
	if config.SSchemaName != "" {
		var s = dbc.Schema{Name: config.SSchemaName}
		sSchemas = append(sSchemas, s)
	} else {
		sSchemas, err = sdb.GetSchemas()
		checkErr(err)
	}
	fmt.Println("schemas: ", sSchemas)

	//////////
	// dest DB
	//////////
	if config.Dest == "" {
		fmt.Println("No destination specified")
		return
	}

	var ddb *dbc.Database
	var DSchema string

	if config.Dest == "file:" {
		ddb = sdb
	} else {
		ddb = dbOpen(dst)
	}
	defer ddb.Close()

	for _, s := range sSchemas {
		if config.Dest == "file:" {
			DSchema = s.Name
		} else if config.Dest != "file:" && config.DSchemaName == "" {
			DSchema = s.Name
		} else {
			DSchema = config.DSchemaName
		}

		var data = dbc.Conn{
			Source:  sdb,
			Dest:    ddb,
			SSchema: s.Name,
			DSchema: DSchema,
		}

		if config.Table || config.TableName != "" {
			fmt.Println("table")
			getTables(&config, &data)
		}
		if config.View {
			fmt.Println("view")
			getViews(&config, &data)
		}
		if config.Routine {
			fmt.Println("routine")
			getRoutines(&config, &data)
		}
		if config.Index {
			fmt.Println("index")
			getIndexes(&config, &data)
		}
		if config.All && !config.Table && !config.View && !config.Routine && !config.Index {
			fmt.Println("all")
			getTables(&config, &data)
			getViews(&config, &data)
			getRoutines(&config, &data)
			getIndexes(&config, &data)
		}
	}
}

func getTables(config *Config, data *dbc.Conn) {
	var err error
	var sTables []dbc.Table

	if config.TableName != "" {
		sTables = []dbc.Table{{Name: config.TableName}}
	} else {
		sTables, err = data.Source.GetTables(data.SSchema)
		checkErr(err)
	}
	// fmt.Println("tables: ", len(sTables))

	var tbls []string
	for _, t := range sTables {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			tbls = append(tbls, t.Name)
		}
	}
	// fmt.Println("sTables: ", len(sTables), "tables: ", len(tbls))

	if len(tbls) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, tbls)
	}
}

func getViews(config *Config, data *dbc.Conn) {
	fmt.Println(config, data)
	var err error
	var sViews []dbc.ViewList

	if config.ViewName != "" {
		sViews = []dbc.ViewList{{Name: config.ViewName}}
	} else {
		sViews, err = data.Source.GetViews(data.SSchema)
		checkErr(err)
	}
	// fmt.Println("views: ", len(sViews))

	var views []string
	for _, t := range sViews {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			views = append(views, t.Name)
		}
	}
	// fmt.Println("sViews: ", len(sViews), "views: ", len(views))

	if len(views) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, views)
	}

}

func getRoutines(config *Config, data *dbc.Conn) {
	var err error
	var sRoutines []dbc.RoutineList

	if config.RoutineName != "" {
		sRoutines = []dbc.RoutineList{{Name: config.RoutineName}}
	} else {
		sRoutines, err = data.Source.GetRoutines(data.SSchema)
		checkErr(err)
	}
	// fmt.Println("routines: ", len(sRoutines))

	var routines []string
	for _, t := range sRoutines {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			routines = append(routines, t.Name)
		}
	}
	// fmt.Println("sRoutines: ", len(sRoutines), "tables: ", len(routines))

	if len(routines) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, routines)
	}
}

func getIndexes(config *Config, data *dbc.Conn) {
	var err error
	var sIndexes []dbc.IndexList

	if config.RoutineName != "" {
		sIndexes = []dbc.IndexList{{Name: config.IndexName}}
	} else {
		sIndexes, err = data.Source.GetIndexes(data.SSchema)
		checkErr(err)
	}
	// fmt.Println("routines: ", len(sIndexes))

	var indexes []string
	for _, t := range sIndexes {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			indexes = append(indexes, t.Name)
		}
	}
	// fmt.Println("sIndexes: ", len(sIndexes), "tables: ", len(indexes))

	if len(indexes) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, indexes)
	}
}

func backupTasker(config *Config, data *dbc.Conn, objects []string) {
	fmt.Println("backupTasker")
	sem := make(chan int, config.JobCount)
	var wg sync.WaitGroup
	wg.Add(len(objects))
	bar := progressbar.Default(int64(len(objects)))
	for _, object := range objects {
		go func(sem chan int, wg *sync.WaitGroup, bar *progressbar.ProgressBar, config *Config, data *dbc.Conn, object string) {
			defer bar.Add(1)
			defer wg.Done()
			sem <- 1

			if config.Table {
				dsql, csql := data.Source.GetTableSchema(data, object)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s", dsql, csql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(dsql)
					checkErr(err)
					_, err = data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			if config.Update {
				dsql, csql := data.Source.GetUpdateTableSchema(data, object)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__upd_%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s", dsql, csql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(dsql)
					checkErr(err)
					_, err = data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			if config.Link && data.Dest.Driver == "postgres" {
				dsql, csql := data.Source.GetForeignTableSchema(data, object)

				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__%sTEMP.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s", dsql, csql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(dsql)
					checkErr(err)
					_, err = data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			if config.View {
				vsql, err := data.Source.GetViewSchema(data.SSchema, object)
				checkErr(err)
				csql := ""
				if data.Dest.Driver == "postgres" {
					csql += fmt.Sprintf("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS\n", data.DSchema, vsql.Name)
				}
				csql += vsql.Definition

				if config.Debug {
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(csql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			if config.Routine {
				rsql, err := data.Source.GetRoutineSchema(data.SSchema, object)
				checkErr(err)
				csql := ""
				if data.Dest.Driver == "postgres" {
					csql = fmt.Sprintf("CREATE OR REPLACE %s \"%s\".\"%s\"", rsql.Type, data.DSchema, rsql.Name)
					csql += fmt.Sprintf("() \nLANGUAGE %s\nAS $%s$", rsql.ExternalLanguage, strings.ToLower(rsql.Type))
				}

				csql += rsql.Definition

				if data.Dest.Driver == "postgres" {
					csql += fmt.Sprintf("$%s$\n;", strings.ToLower(rsql.Type))
				}

				if config.Debug {
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(csql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			if config.Index {
				rsql, err := data.Source.GetIndexSchema(data.SSchema, object)
				checkErr(err)
				idx := "\"" + strings.Replace(strings.Replace(rsql.Table+`_`+rsql.Columns+"_idx", "\"", "", -1), ",", "_", -1) + "\""
				exists := ""
				notexists := ""
				if data.Dest.Driver == "postgres" {
					exists = "IF EXISTS "
					notexists = "IF NOT EXISTS "
				}

				dsql := `DROP INDEX ` + exists + `"` + rsql.Schema + `".` + idx + `;`
				csql := `CREATE INDEX ` + notexists + `` + idx + ` ON "` + rsql.Schema + `"."` + rsql.Table + `" (` + rsql.Columns + `);`

				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(dsql), 0666)
						checkErr(err)
						err = ioutil.WriteFile(fn, []byte(csql), 0666)
						checkErr(err)
					}
					_, err := data.Dest.Exec(dsql)
					checkErr(err)
					_, err = data.Dest.Exec(csql)
					checkErr(err)
				}
			}

			<-sem
		}(sem, &wg, bar, config, data, object)
	}
	wg.Wait()
}
