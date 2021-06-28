package database

import (
	"fmt"
)

//////////
// Indexes
//////////

// Index list of Indexes
type Index struct {
	Schema     string `db:"schemaname"`
	Table      string `db:"tablename"`
	Name       string `db:"indexname"`
	Columns    string `db:"indexcolumns"`
	Definition string `db:"indexdef"`
}

type IndexList struct {
	Name string `db:"indexname"`
}

// GetIndexes returns list of Indexes and definitions
func (db *Database) GetIndexes(schema string) ([]IndexList, error) {
	q := ""
	if db.Driver == "postgres" {
		q += "select p.indexname\n"
		q += "from pg_catalog.pg_indexes p\n"
		q += "left join (\n"
		q += "SELECT CONSTRAINT_NAME\n"
		q += "FROM artg.INFORMATION_SCHEMA.TABLE_CONSTRAINTS\n"
		q += "where CONSTRAINT_TYPE = 'PRIMARY KEY'\n"
		q += ") c on p.indexname = c.constraint_name\n"
		q += "where p.schemaname not in ('information_schema','pg_catalog')\n"
		q += "and c.constraint_name is null\n"
		q += "and p.schemaname = '" + schema + "'"

	} else if db.Driver == "mssql" {
		q += `select i."name" "indexname"` + "\n"
		q += `from sys.objects t` + "\n"
		q += `inner join sys.indexes i on t.object_id = i.object_id` + "\n"
		q += `cross apply (` + "\n"
		q += `select col."name" + ', '` + "\n"
		q += `from sys.index_columns ic` + "\n"
		q += `inner join sys.columns col` + "\n"
		q += `on ic.object_id = col.object_id` + "\n"
		q += `and ic.column_id = col.column_id` + "\n"
		q += `where ic.object_id = t.object_id` + "\n"
		q += `and ic.index_id = i.index_id` + "\n"
		q += `order by col.column_id for xml path ('')` + "\n"
		q += `) D (column_names)` + "\n"
		q += `where t.is_ms_shipped <> 1 AND t."type" = 'U'` + "\n"
		q += `and index_id > 0 and i.is_primary_key <> 1` + "\n"
		q += `and schema_name(t.schema_id) = '` + schema + `'` + "\n"
		q += `order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	// fmt.Println(q)
	vv := []IndexList{}
	if err := db.Select(&vv, q); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}

// GetIndexeschema returns Indexes and definition
func (db *Database) GetIndexSchema(schema, index string) (Index, error) {
	q := ""
	if db.Driver == "postgres" {
		q += "select p.schemaname,p.tablename,p.indexname" + "\n"
		q += `,'"'||replace(replace(split_part(split_part(p.indexdef,'(',2),')',1),'"',''),',','","')||'"' as indexcolumns` + "\n"
		q += `,p.indexdef` + "\n"
		q += `from pg_catalog.pg_indexes p` + "\n"
		q += `left join (` + "\n"
		q += `SELECT CONSTRAINT_NAME` + "\n"
		q += `FROM artg.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + "\n"
		q += `) c on p.indexname = c.constraint_name` + "\n"
		q += `where p.schemaname not in ('information_schema','pg_catalog')` + "\n"
		q += `and c.constraint_name is null` + "\n"
		q += `and p.schemaname = '` + schema + `'` + "\n"
		q += `and p.indexname = '` + index + `'` + "\n"
		q += `order by schemaname,tablename`

	} else if db.Driver == "mssql" {
		q += `select schema_name(t.schema_id) "schemaname"` + "\n"
		q += `,t."name" "tablename"` + "\n"
		q += `,i."name" "indexname"` + "\n"
		q += `,'"'+replace(substring(column_names, 1, len(column_names)-1),', ','","')+'"' as indexcolumns` + "\n"
		q += `,'CREATE '+` + "\n"
		q += `case when i."type" = 1 then 'CLUSTERED'` + "\n"
		q += `when i."type" = 2 then 'NONCLUSTERED UNIQUE'` + "\n"
		q += `when i."type" = 3 then 'XML'` + "\n"
		q += `when i."type" = 4 then 'SPATIAL'` + "\n"
		q += `when i."type" = 5 then 'CLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 6 then 'NONCLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 7 then 'NONCLUSTERED HASH'` + "\n"
		q += `end + ' INDEX ' +i."name" + ' ON '` + "\n"
		q += `+ schema_name(t.schema_id) + '.' + t."name"` + "\n"
		q += `+ '('+substring(column_names, 1, len(column_names)-1)+');'` + "\n"
		q += `AS "indexdef"` + "\n"
		q += `from sys.objects t` + "\n"
		q += `inner join sys.indexes i on t.object_id = i.object_id` + "\n"
		q += `cross apply (` + "\n"
		q += `select col."name" + ', '` + "\n"
		q += `from sys.index_columns ic` + "\n"
		q += `inner join sys.columns col` + "\n"
		q += `on ic.object_id = col.object_id` + "\n"
		q += `and ic.column_id = col.column_id` + "\n"
		q += `where ic.object_id = t.object_id` + "\n"
		q += `and ic.index_id = i.index_id ` + "\n"
		q += `order by col.column_id for xml path ('') ` + "\n"
		q += `) D (column_names)` + "\n"
		q += `where t.is_ms_shipped <> 1 AND t."type" = 'U'` + "\n"
		q += `and index_id > 0 and i.is_primary_key <> 1` + "\n"
		q += `and schema_name(t.schema_id) = '` + schema + `'` + "\n"
		q += `and i."name" = '` + index + `'` + "\n"
		q += `order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	// fmt.Println(q)
	vv := Index{}
	if err := db.Get(&vv, q); err != nil {
		return Index{}, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}

// GetIndexeschema returns Indexes and definition
func (db *Database) GetTableIndexSchema(schema, table string) ([]Index, error) {
	q := ""
	if db.Driver == "postgres" {
		q += "select p.schemaname,p.tablename,p.indexname" + "\n"
		q += `,'"'||replace(replace(split_part(split_part(p.indexdef,'(',2),')',1),'"',''),',','","')||'"' as indexcolumns` + "\n"
		q += `,p.indexdef` + "\n"
		q += `from pg_catalog.pg_indexes p` + "\n"
		q += `left join (` + "\n"
		q += `SELECT CONSTRAINT_NAME` + "\n"
		q += `FROM artg.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + "\n"
		q += `) c on p.indexname = c.constraint_name` + "\n"
		q += `where p.schemaname not in ('information_schema','pg_catalog')` + "\n"
		q += `and c.constraint_name is null` + "\n"
		q += `and p.schemaname = '` + schema + `'` + "\n"
		q += `and p.tablename = '` + table + `'` + "\n"
		q += `order by schemaname,tablename,indexname`
	} else if db.Driver == "mssql" {
		q += `select schema_name(t.schema_id) "schemaname"` + "\n"
		q += `,t."name" "tablename"` + "\n"
		q += `,i."name" "indexname"` + "\n"
		q += `,'"'+replace(substring(column_names, 1, len(column_names)-1),', ','","')+'"' as indexcolumns` + "\n"
		q += `,'CREATE '+` + "\n"
		q += `case when i."type" = 1 then 'CLUSTERED'` + "\n"
		q += `when i."type" = 2 then 'NONCLUSTERED UNIQUE'` + "\n"
		q += `when i."type" = 3 then 'XML'` + "\n"
		q += `when i."type" = 4 then 'SPATIAL'` + "\n"
		q += `when i."type" = 5 then 'CLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 6 then 'NONCLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 7 then 'NONCLUSTERED HASH'` + "\n"
		q += `end + ' INDEX ' +i."name" + ' ON '` + "\n"
		q += `+ schema_name(t.schema_id) + '.' + t."name"` + "\n"
		q += `+ '('+substring(column_names, 1, len(column_names)-1)+');'` + "\n"
		q += `AS "indexdef"` + "\n"
		q += `from sys.objects t` + "\n"
		q += `inner join sys.indexes i on t.object_id = i.object_id` + "\n"
		q += `cross apply (` + "\n"
		q += `select col."name" + ', '` + "\n"
		q += `from sys.index_columns ic` + "\n"
		q += `inner join sys.columns col` + "\n"
		q += `on ic.object_id = col.object_id` + "\n"
		q += `and ic.column_id = col.column_id` + "\n"
		q += `where ic.object_id = t.object_id` + "\n"
		q += `and ic.index_id = i.index_id ` + "\n"
		q += `order by col.column_id for xml path ('') ` + "\n"
		q += `) D (column_names)` + "\n"
		q += `where t.is_ms_shipped <> 1 AND t."type" = 'U'` + "\n"
		q += `and index_id > 0 and i.is_primary_key <> 1` + "\n"
		q += `and schema_name(t.schema_id) = '` + schema + `'` + "\n"
		q += `and t."name" = '` + table + `'` + "\n"
		q += `order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	// fmt.Println(q)
	vv := []Index{}
	if err := db.Select(&vv, q); err != nil {
		return []Index{}, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}
