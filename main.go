package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"

	_ "gopkg.in/goracle.v2"
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

	var local, remote []Object
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, todo := range []struct {
		DB   *sql.DB
		Dest *[]Object
	}{
		{DB: localDB, Dest: &local},
		{DB: remoteDB, Dest: &remote},
	} {
		db, dest := todo.DB, todo.Dest
		grp.Go(func() error {
			rows, err := db.QueryContext(grpCtx, tblQry, types, pat)
			if err != nil {
				return errors.Wrap(err, tblQry)
			}
			defer rows.Close()
			for rows.Next() {
				var o Object
				if err = rows.Scan(&o.Name, &o.Type); err != nil {
					return err
				}
				*dest = append(*dest, o)
			}
			return rows.Err()
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	const colQry = `SELECT
    column_name
    , nullable
    , (CASE data_type WHEN 'DATE' THEN 'DATE'
	                  WHEN 'NUMBER' THEN 'NUMBER('||data_precision||','||data_scale||')'
					  ELSE data_type||'('||data_length||')' END) data_type
  FROM user_tab_columns
  WHERE table_name = :2
  ORDER BY 1`

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
			var local, remote []Column
			subGrp, subGrpCtx := errgroup.WithContext(grpCtx)
			for _, todo := range []struct {
				DB     *sql.DB
				Schema string
				Dest   *[]Column
			}{
				{DB: localDB, Schema: "local", Dest: &local},
				{DB: remoteDB, Schema: "remote", Dest: &remote},
			} {
				todo := todo
				name := o.Name
				subGrp.Go(func() error {
					rows, err := todo.DB.QueryContext(subGrpCtx, colQry, name)
					if err != nil {
						return errors.Wrap(err, colQry)
					}
					defer rows.Close()
					for rows.Next() {
						c := Column{Schema: todo.Schema, Table: name}
						var nullable string
						if err = rows.Scan(&c.Name, &nullable, &c.Type); err != nil {
							return err
						}
						c.Nullable = nullable == "Y"
						*todo.Dest = append(*todo.Dest, c)
					}
					return rows.Err()
				})
			}
			if err := subGrp.Wait(); err != nil {
				return errors.WithMessage(err, o.Name)
			}

			diff := colCompare(local, remote)
			if diff != "" {
				colDiffs <- TD{Table: o.Name, Diff: diff}
			}
			return nil
		})
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
	Nullable            bool
	Type                string
}

func (c Column) String() string { return c.Name + " " + c.Type }

func colCompare(local, remote []Column) string {
	var token struct{}
	cols := make(map[string]struct{}, 128)
	var diff strings.Builder
	for _, c := range local {
		cols[c.String()] = token
	}
	for _, c := range remote {
		s := c.String()
		if _, ok := cols[s]; ok {
			continue
		}
		fmt.Fprintln(&diff, "-", s)
	}

	for k := range cols {
		delete(cols, k)
	}
	for _, c := range remote {
		cols[c.String()] = token
	}
	for _, c := range local {
		s := c.String()
		if _, ok := cols[s]; ok {
			continue
		}
		fmt.Fprintln(&diff, "+", s)
	}

	return diff.String()
}
