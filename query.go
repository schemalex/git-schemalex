package gitschemalex

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
)

type query struct {
	stmt string
	args []interface{}
}

func (q *query) execute(db *sql.DB) error {
	_, err := db.Exec(q.stmt, q.args...)
	return errors.Wrap(err, `failed to execute query`)
}

func (q *query) dump(dst io.Writer) error {
	fmt.Fprintf(dst, "%s;", q.stmt)
	if len(q.args) > 0 {
		fmt.Fprintf(dst, "%v", q.args)
	}
	fmt.Fprintf(dst, "\n\n")
	return nil
}

type queryList []*query

func queryListFromString(stmts string) queryList {
	var l queryList
	for _, stmt := range strings.Split(stmts, ";") {
		stmt = strings.TrimSpace(stmt)
		if len(stmt) == 0 {
			continue
		}
		l.AppendStmt(stmt)
	}
	return l
}

func (l *queryList) AppendStmt(stmt string, args ...interface{}) {
	*l = append(*l, &query{
		stmt: stmt,
		args: args,
	})
}

func (l *queryList) dump(dst io.Writer) error {
	for i, q := range *l {
		if err := q.dump(dst); err != nil {
			return errors.Wrapf(err, `failed to dump query %d`, i+1)
		}
	}
	return nil
}

func (l *queryList) execute(db *sql.DB) error {
	for i, q := range *l {
		if err := q.execute(db); err != nil {
			return errors.Wrapf(err, `failed to execute query %d`, i+1)
		}
	}
	return nil
}
