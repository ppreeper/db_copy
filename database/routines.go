package database

import (
	"fmt"
	"os"
)

//////////
// Routines
//////////

// DBRoutine list of routines (procedures, functions)
type DBRoutine struct {
	Name             string `db:"ROUTINE_NAME"`
	Type             string `db:"ROUTINE_TYPE"`
	Definition       string `db:"ROUTINE_DEFINITION"`
	DataType         string `db:"DATA_TYPE"`
	ExternalLanguage string `db:"EXTERNAL_LANGUAGE"`
}

// TODO get postgres routines, currently sql server definitions work

// GetRoutines returns table list
func (db *Database) GetRoutines(d Dbase, schema string) ([]DBRoutine, error) {
	q := ""
	if d.Driver == "postgres" {
		q += "SELECT ROUTINE_NAME \"ROUTINE_NAME\"" + "\n"
		q += ",ROUTINE_TYPE \"ROUTINE_TYPE\"" + "\n"
		q += ",ROUTINE_DEFINITION \"ROUTINE_DEFINITION\"" + "\n"
		q += ",CASE WHEN DATA_TYPE IS NULL THEN '' ELSE DATA_TYPE END \"DATA_TYPE\"" + "\n"
		q += ",CASE WHEN EXTERNAL_LANGUAGE IS NULL THEN '' ELSE EXTERNAL_LANGUAGE END \"EXTERNAL_LANGUAGE\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.ROUTINES" + "\n"
		q += "WHERE ROUTINE_SCHEMA = '" + schema + "'" + "\n"
		q += "AND ROUTINE_DEFINITION IS NOT NULL" + "\n"
		q += "ORDER BY ROUTINE_NAME" + "\n"
	} else if d.Driver == "mssql" {
		q += "select ROUTINE_NAME \"ROUTINE_NAME\"" + "\n"
		q += ",ROUTINE_TYPE \"ROUTINE_TYPE\"" + "\n"
		q += ",ROUTINE_DEFINITION \"ROUTINE_DEFINITION\"" + "\n"
		q += ",CASE WHEN DATA_TYPE IS NULL THEN '' ELSE DATA_TYPE END \"DATA_TYPE\"" + "\n"
		q += ",CASE WHEN EXTERNAL_LANGUAGE IS NULL THEN '' ELSE EXTERNAL_LANGUAGE END \"EXTERNAL_LANGUAGE\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.ROUTINES" + "\n"
		q += "WHERE ROUTINE_SCHEMA = '" + schema + "'" + "\n"
		q += "AND ROUTINE_DEFINITION IS NOT NULL" + "\n"
		q += "ORDER BY ROUTINE_NAME" + "\n"
	}
	// fmt.Println(q)
	rr := []DBRoutine{}
	if err := db.Select(&rr, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return rr, nil
}

// GetRoutine gets procedure definition
func (db *Database) GetRoutine(d Dbase, schema string, r DBRoutine) {
	fmt.Printf("\nROUTINE: %s.%s", schema, r.Name)
	fname := fmt.Sprintf("%s.%s.%s.%s.sql", d.Database, schema, r.Name, r.Type)
	f, err := os.Create(fname)
	checkErr(err)
	defer f.Close()
	q := ""
	if d.Driver == "postgres" {
		if r.Type == "PROCEDURE" {
			q += "DROP " + r.Type + " IF EXISTS " + schema + "." + r.Name + "();\n"
			q += "CREATE OR REPLACE " + r.Type + " " + schema + "." + r.Name + "()\n"
			q += "LANGUAGE sql\n"
			q += "AS $procedure$"
			q += r.Definition
			q += "$procedure$\n;"
		} else if r.Type == "FUNCTION" {
			q += r.Definition
		}
	} else if d.Driver == "mssql" {
		if r.Type == "PROCEDURE" {
			q += "DROP " + r.Type + " " + schema + "." + r.Name + ";\n"
			q += r.Definition + "\n"
		} else if r.Type == "FUNCTION" {
			q += "DROP " + r.Type + " " + schema + "." + r.Name + ";\n"
			q += r.Definition
		}

	}
	f.Write([]byte(q))
	return
}
