package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"

	goracle "gopkg.in/goracle.v2"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

func Main() error {
	var opts CompareOptions

	app := kingpin.New("schema-diff", "compare database scemas")
	compareCmd := app.Command("compare", "compare the given schemas").Default()
	localArg := compareCmd.Arg("local", "local database connection string").Default(os.Getenv("BRUNO_ID")).String()
	remoteArg := compareCmd.Arg("remote", "remote database connection string").String()
	compareCmd.Flag("type", "object types to compare").Default("TABLE").StringsVar(&opts.Types)
	compareCmd.Flag("pattern", "REGEXP_LIKE pattern to use").Default("^[RT]_").StringVar(&opts.Pattern)
	compareCmd.Flag("text", "text diff").Default("false").BoolVar(&opts.TextDiff)

	todo, err := app.Parse(os.Args[1:])
	if err != nil {
		return err
	}
	_ = todo

	localDB, err := sql.Open("goracle", *localArg)
	if err != nil {
		return errors.Wrap(err, *localArg)
	}
	defer localDB.Close()
	localDB.SetMaxOpenConns(8)
	localDB.SetMaxIdleConns(1)
	remoteDB, err := sql.Open("goracle", *remoteArg)
	if err != nil {
		return errors.Wrap(err, *remoteArg)
	}
	defer remoteDB.Close()
	remoteDB.SetMaxOpenConns(8)
	remoteDB.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return opts.Compare(ctx, localDB, remoteDB)
}

type CompareOptions struct {
	Types    []string
	Pattern  string
	TextDiff bool
}

// http://www.idevelopment.info/data/Oracle/DBA_scripts/Database_Administration/dba_compare_schemas.sql
func (O CompareOptions) Compare(ctx context.Context, localDB, remoteDB *sql.DB) error {
	types := ",TABLE,PACKAGE,SEQUENCE,SYNONYM,"
	if len(O.Types) > 0 {
		types = "," + strings.Join(O.Types, ",") + ","
	}
	pat := O.Pattern
	if pat == "" {
		pat = "."
	}
	const tblQry = `SELECT
    DECODE(   object_type
            , 'INDEX', DECODE(SUBSTR(object_name, 1, 5), 'SYS_C', 'SYS_C', object_name)
            , 'LOB',   DECODE(SUBSTR(object_name, 1, 7), 'SYS_LOB', 'SYS_LOB', object_name)
            , object_name) object_name
  , object_type
  FROM user_objects
  WHERE INSTR(:1, object_type) > 0 AND REGEXP_LIKE(object_name, :2)
  ORDER BY 1, 2`

	const colQry = `SELECT
    table_name
	, column_name
    , (CASE data_type WHEN 'DATE' THEN 'DATE'
	                  WHEN 'NUMBER' THEN 'NUMBER('||data_precision||','||data_scale||')'
					  ELSE data_type||'('||data_length||')' END)||
	   (CASE nullable WHEN 'N' THEN ' NOT NULL' ELSE '' END) data_type
  FROM user_tab_columns
  WHERE REGEXP_LIKE(table_name, :1)
  ORDER BY 1, 2`

	var local, remote []Object
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, todo := range []struct {
		DB     *sql.DB
		Schema string
		Dest   *[]Object
	}{
		{DB: localDB, Schema: "local", Dest: &local},
		{DB: remoteDB, Schema: "remote", Dest: &remote},
	} {
		todo := todo
		grp.Go(func() error {
			rows, err := todo.DB.QueryContext(grpCtx, tblQry, types, pat, goracle.FetchRowCount(512))
			if err != nil {
				return errors.Wrap(err, tblQry)
			}
			defer rows.Close()

			for rows.Next() {
				var o Object
				if err = rows.Scan(&o.Name, &o.Type); err != nil {
					return err
				}
				*todo.Dest = append(*todo.Dest, o)
			}
			return rows.Err()
		})
	}

	loCols := make(map[string][]Column)
	reCols := make(map[string][]Column)
	var colsMtx sync.Mutex
	grpCol, grpColCtx := errgroup.WithContext(ctx)
	for _, todo := range []struct {
		DB     *sql.DB
		Schema string
		Dest   map[string][]Column
	}{
		{DB: localDB, Schema: "local", Dest: loCols},
		{DB: remoteDB, Schema: "remote", Dest: reCols},
	} {
		todo := todo
		grpCol.Go(func() error {
			var cols []Column
			rows, err := todo.DB.QueryContext(grpColCtx, colQry, pat, goracle.FetchRowCount(8192))
			if err != nil {
				return errors.Wrap(err, colQry)
			}
			defer rows.Close()

			for rows.Next() {
				c := Column{Schema: todo.Schema}
				if err = rows.Scan(&c.Table, &c.Name, &c.Type); err != nil {
					return err
				}
				if len(cols) > 0 && cols[len(cols)-1].Table != c.Table {
					colsMtx.Lock()
					todo.Dest[cols[len(cols)-1].Table] = cols
					colsMtx.Unlock()
					cols = nil
				}
				cols = append(cols, c)
			}
			colsMtx.Lock()
			todo.Dest[cols[len(cols)-1].Table] = cols
			colsMtx.Unlock()
			return rows.Err()
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}

	n := len(remote)
	if n < len(local) {
		n = len(local)
	}
	var token struct{}
	type TD struct {
		Table, Diff string
	}
	colDiffs := make(chan TD, n)
	other := make(map[Object]struct{}, n)
	fmt.Println("\n---------------------------------------")
	fmt.Println("-- Objects missing from local schema --")
	fmt.Println("---------------------------------------")
	for _, o := range local {
		other[o] = token
	}
	grp, grpCtx = errgroup.WithContext(ctx)
	for _, o := range remote {
		if _, ok := other[o]; !ok {
			fmt.Println(o)
			continue
		}
		if o.Type != "TABLE" {
			continue
		}
		o := o
		grp.Go(func() error {
			<-grpColCtx.Done()
			diff := colCompare(loCols[o.Name], reCols[o.Name])
			if diff != "" {
				colDiffs <- TD{Table: o.Name, Diff: diff}
			}
			return nil
		})
	}
	if err := grpCol.Wait(); err != nil {
		return err
	}
	var err error
	go func() {
		err = grp.Wait()
		close(colDiffs)
	}()

	fmt.Println("\n----------------------------------------")
	fmt.Println("-- Extraneous objects in local schema --")
	fmt.Println("----------------------------------------")
	for o := range other {
		delete(other, o)
	}
	for _, o := range remote {
		other[o] = token
	}
	for _, o := range local {
		if _, ok := other[o]; ok {
			continue
		}
		fmt.Println(o)
	}

	fmt.Println("\n--------------------------------------------------------------------- ---")
	fmt.Println("-- Data type discrepancies for table columns that exist in both schemas --")
	fmt.Println("--------------------------------------------------------------------- ---")

	for td := range colDiffs {
		if td.Diff == "" {
			continue
		}
		fmt.Println("--", td.Table)
		fmt.Println(td.Diff)
	}

	return err
}

type Object struct {
	Name, Type string
}

type Column struct {
	Schema, Table, Name string
	Type                string
}

func (c Column) String() string { return c.Name + " " + c.Type }

func colCompare(local, remote []Column) string {
	n := len(local)
	if m := len(remote); m > n {
		n = m
	}
	localM := make(map[string]Column, len(local))
	for _, c := range local {
		localM[c.Name] = c
	}

	var diff strings.Builder
	for _, r := range remote {
		if l, ok := localM[r.Name]; !ok {
			fmt.Fprintf(&diff, "ALTER TABLE %s ADD %s %s;\n", r.Table, r.Name, r.Type)
		} else if l.Type == r.Type {
			continue
		} else {
			fmt.Fprintf(&diff, "ALTER TABLE %s MODIFY %s %s; --%s\n", r.Table, r.Name, r.Type, l.Type)
		}
	}

	remoteM := make(map[string]Column, len(remote))
	for _, c := range remote {
		remoteM[c.Name] = c
	}
	for _, l := range local {
		if _, ok := remoteM[l.Name]; ok {
			continue
		}
		fmt.Fprintf(&diff, "ALTER TABLE %s DROP %s;\n", l.Table, l.Name)
	}

	return diff.String()
}
