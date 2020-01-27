package database

import (
	"fmt"
	"os"
	"strings"
)

//////////
// Views
//////////

// View list of views
type View struct {
	Name       string `db:"TABLE_NAME"`
	Definition string `db:"VIEW_DEFINITION"`
}

// GetViews returns list of views and definitions
func (db *Database) GetViews(d Dbase, schema string) ([]View, error) {
	q := ""
	if d.Driver == "postgres" {
		q += "SELECT TABLE_NAME \"TABLE_NAME\", VIEW_DEFINITION \"VIEW_DEFINITION\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.VIEWS" + "\n"
		q += "WHERE TABLE_SCHEMA = '" + schema + "'" + "\n"
		q += "ORDER BY TABLE_NAME" + "\n"
	} else if d.Driver == "mssql" {
		q += "SELECT TABLE_NAME \"TABLE_NAME\", VIEW_DEFINITION \"VIEW_DEFINITION\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.VIEWS" + "\n"
		q += "WHERE TABLE_SCHEMA = '" + schema + "'" + "\n"
		q += "ORDER BY TABLE_NAME" + "\n"
	}
	// fmt.Println(q)
	vv := []View{}
	if err := db.Select(&vv, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return vv, nil
}

// GetViewSchema returns views and definition
func (db *Database) GetViewSchema(d Dbase, schema, view string) (View, error) {
	q := ""
	if d.Driver == "postgres" {
		q += "SELECT TABLE_NAME \"TABLE_NAME\", VIEW_DEFINITION \"VIEW_DEFINITION\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.VIEWS" + "\n"
		q += "WHERE TABLE_SCHEMA = '" + schema + "'" + "\n"
		q += "AND TABLE_NAME = '" + view + "'" + "\n"
		q += "ORDER BY TABLE_NAME" + "\n"
	} else if d.Driver == "mssql" {
		q += "SELECT TABLE_NAME \"TABLE_NAME\", VIEW_DEFINITION \"VIEW_DEFINITION\"" + "\n"
		q += "FROM INFORMATION_SCHEMA.VIEWS" + "\n"
		q += "WHERE TABLE_SCHEMA = '" + schema + "'" + "\n"
		q += "AND TABLE_NAME = '" + view + "'" + "\n"
		q += "ORDER BY TABLE_NAME" + "\n"
	}
	// fmt.Println(q)
	vv := View{}
	if err := db.Get(&vv, q); err != nil {
		return View{}, fmt.Errorf("Select: %v", err)
	}
	return vv, nil
}

// GetView gets view definition
func (db *Database) GetView(d Dbase, schema string, view View, dbg bool) {
	fmt.Printf("\n-- VIEW: %s.%s", schema, view.Name)
	q := ""
	if d.Driver == "postgres" {
		q += "DROP VIEW " + schema + "." + view.Name + ";\n"
		q += "CREATE VIEW " + schema + "." + view.Name + " AS \n"
		q += view.Definition
	} else if d.Driver == "mssql" {
		q += view.Definition + "\n"
	}

	if dbg {
		fmt.Printf("\n%v\n", q)
	} else {
		t := strings.Replace(view.Name, "/", "_", -1)
		fname := fmt.Sprintf("%s.%s.%s.VIEW.sql", d.Database, schema, t)
		f, err := os.Create(fname)
		checkErr(err)
		defer f.Close()
		f.Write([]byte(q))
	}
	return
}
