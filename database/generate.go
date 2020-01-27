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
		sqld += fmt.Sprintf("\nDROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n", s, t)
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
		sqld += fmt.Sprintf("\nDROP TABLE \"%s\".\"%s\";\n", s, t)
		sqlc += fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (\n", s, t)
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

func tableUpdProcStart(destDriver, schema, tableName string) (sqld, sqlc string) {
	// upd := ""
	tmp := ""
	if tableName == strings.ToUpper(tableName) {
		// upd = "UPD"
		tmp = "TEMP"
	} else {
		// upd = "upd"
		tmp = "temp"
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("CREATE OR REPLACE PROCEDURE \"%s\".\"upd_%s\"()\nLANGUAGE plpgsql\nAS $procedure$\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("DROP TABLE IF EXISTS \"temp_%s\";\n", tableName)
		sqlc += fmt.Sprintf("CREATE TEMPORARY TABLE \"temp_%s\" AS SELECT * FROM \"%s\".\"%s%s\";\n", tableName, schema, tableName, tmp)
	} else if destDriver == "mssql" {
		sqld += fmt.Sprintf("DROP PROCEDURE \"%s\".\"upd_%s\";", schema, tableName)
		sqlc += fmt.Sprintf("CREATE PROCEDURE \"%s\".\"upd_%s\" AS\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("SELECT * INTO #%s FROM \"%s\".\"%s%s\"\n", tableName, schema, tableName, tmp)
	}
	return sqld, sqlc
}

func tableUpdProcEnd(destDriver, tableName string) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("DROP TABLE IF EXISTS \"temp_%s\";\n", tableName)
		sqlc += fmt.Sprintf("END\n$procedure$;\n")
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("END;\n")
	}
	return sqlc
}

func tableIndexSQL(destDriver, tableName string, pkey []PKey) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON \"temp_%s\" (", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON #%s (", tableName, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\"%s\"", p.PKey)
		if k == plen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" {
		sqlc += ");\n"
	} else if destDriver == "mssql" {
		sqlc += ")\n"
	}
	return sqlc
}

func tableDeleteSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += "DELETE\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("DELETE \"%s\".\"%s\"\n", schema, tableName)
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("USING \"%s\".\"%s\" AS d\n", schema, tableName)
		sqlc += fmt.Sprintf("LEFT OUTER JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("LEFT JOIN #%s \"%stemp\" ON", tableName, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		if destDriver == "postgres" {
			sqlc += fmt.Sprintf("\nd.\"%s\" = \"%stemp\".\"%s\"", p.PKey, tableName, p.PKey)
		} else if destDriver == "mssql" {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		}
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += "WHERE"
	}
	if destDriver == "postgres" {
		for k, p := range pkey {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = d.\"%s\" ", tableName, p.PKey, p.PKey)
			if k == plen-1 {
				sqlc += "\n"
			} else {
				sqlc += " AND "
			}
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("AND \"%stemp\".\"%s\" IS NULL;\n", tableName, allColumns[0].ColumnName)

	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%stemp\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
	}
	return sqlc
}

func tableUpdateSQL(destDriver, schema, tableName string, pkey []PKey, columns []Column) (sqlc string) {
	sqlc += fmt.Sprintf("UPDATE \"%s\".\"%s\"\nSET", schema, tableName)
	plen := len(pkey)
	clen := len(columns)
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\" = \"%stemp\".\"%s\"", c.ColumnName, tableName, c.ColumnName)
		if k == clen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("\nFROM \"temp_%s\" \"%stemp\"", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nFROM #%s \"%stemp\"", tableName, tableName)
	}
	if destDriver == "postgres" {
		sqlc += "\nWHERE"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nJOIN \"%s\".\"%s\" ON", schema, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += "AND ("
	} else if destDriver == "mssql" {
		sqlc += "WHERE ("
	}
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" <> \"%stemp\".\"%s\"", tableName, c.ColumnName, tableName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += " OR "
		}
	}
	if destDriver == "postgres" {
		sqlc += ");\n"
	} else if destDriver == "mssql" {
		sqlc += ")\n"
	}
	return sqlc
}

func tableInsertSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
	plen := len(pkey)
	clen := len(allColumns)
	sqlc += fmt.Sprintf("INSERT INTO \"%s\".\"%s\"\n", schema, tableName)
	sqlc += "SELECT"
	for k, c := range allColumns {
		sqlc += fmt.Sprintf("\n\"%stemp\".\"%s\" \"%s\"", tableName, c.ColumnName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += ","
		}
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("RIGHT JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("RIGHT JOIN #%s \"%stemp\" ON", tableName, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL;\n", tableName, allColumns[0].ColumnName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
	}
	return sqlc
}
