package database

import (
	"fmt"
	"strings"
)

//GenTable generate table craeation
func (db *Database) GenTable(dst Dbase, s, t string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	// fmt.Println(dst.Driver, s, t, pkey)
	clen := len(cols)
	plen := len(pkey)
	if dst.Driver == "postgres" {
		sqld += fmt.Sprintf("DROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n", s, t)
		sqlc += fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n", s, t)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					sqlc += c.Column + ",\n"
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += "\"" + p.PKey + "\""
						} else {
							sqlc += "\"" + p.PKey + "\","
						}
					}
					sqlc += ")" + "\n"
				} else {
					sqlc += c.Column + "\n"
				}
			} else {
				sqlc += c.Column + ",\n"
			}
		}
		sqlc += ")" + ";\n"
	} else if dst.Driver == "mssql" {
		sqld += fmt.Sprintf("DROP TABLE \"%s\".\"%s\";\n", s, t)
		sqlc += fmt.Sprintf("CREATE TABLE %s.%s (\n", s, t)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					sqlc += fmt.Sprintf("%s,\n", c.Column)
					sqlc += fmt.Sprintf("PRIMARY KEY (")
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += fmt.Sprintf("\"%s\"", p.PKey)
						} else {
							sqlc += fmt.Sprintf("\"%s\",", p.PKey)
						}
					}
					sqlc += fmt.Sprintf(")\n")
				} else {
					// q += c.Column + "\n"
					sqlc += fmt.Sprintf("%s\n", c.Column)
				}
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += fmt.Sprintf(")\n")
	}
	return sqld, sqlc
}

//GenLink generate table creation
func (db *Database) GenLink(dst Dbase, src Dbase, s, t string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	tmp := ""
	if t == strings.ToUpper(t) {
		tmp = "TEMP"
	} else {
		tmp = "temp"
	}
	clen := len(cols)
	if dst.Driver == "postgres" {
		sqld += fmt.Sprintf("DROP FOREIGN TABLE IF EXISTS \"%s\".\"%s%s\" CASCADE;\n", s, t, tmp)
		sqlc += fmt.Sprintf("CREATE FOREIGN TABLE IF NOT EXISTS \"%s\".\"%s%s\" (\n", s, t, tmp)
		for k, c := range cols {
			if k == clen-1 {
				sqlc += fmt.Sprintf("%s\n", c.Column)
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += fmt.Sprintf(")\n")
		sqlc += fmt.Sprintf("SERVER %s \nOPTIONS (", src.Name)
		sqlc += fmt.Sprintf("table_name '%s.%s', ", s, t)
		sqlc += fmt.Sprintf("row_estimate_method 'showplan_all', ")
		sqlc += fmt.Sprintf("match_column_names '0');")
	} else if dst.Driver == "mssql" {
		sqld += fmt.Sprintf("DROP VIEW \"%s\".\"%s%s\";\n", s, t, tmp)
		sqlc += fmt.Sprintf("CREATE VIEW \"%s\".\"%s%s\" AS\n", s, t, tmp)
		for k, c := range cols {
			collation := ""
			if c.DataType == "CHAR" ||
				c.DataType == "VARCHAR" ||
				c.DataType == "NCHAR" ||
				c.DataType == "NVARCHAR" {
				collation = "COLLATE database_default "
			}
			if k == clen-1 {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\"\n", c.ColumnName, collation, c.ColumnName)
			} else {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\",\n", c.ColumnName, collation, c.ColumnName)
			}
		}
		sqlc += fmt.Sprintf("FROM \"%s\".\"%s\".\"%s\".\"%s\";\n", src.Host, src.Database, s, t)
	}
	return sqld, sqlc
}

//GenUpdate generate update procedure
func (db *Database) GenUpdate(dst Dbase, src Dbase, s, t string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	columns := trimCols(cols, pkey)

	sqld, sqlc = tableUpdProcStart(dst.Driver, s, t)
	sqlc += tableIndexSQL(dst.Driver, t, pkey)
	sqlc += tableDeleteSQL(dst.Driver, s, t, pkey, cols)
	if len(pkey) != len(cols) {
		sqlc += tableUpdateSQL(dst.Driver, s, t, pkey, columns)
	}
	sqlc += tableInsertSQL(dst.Driver, s, t, pkey, cols)
	sqlc += tableUpdProcEnd(dst.Driver, t)
	return sqld, sqlc
}
