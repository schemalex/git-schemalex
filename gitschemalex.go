package gitschemalex

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/soh335/schemalex"
)

var (
	ErrEqualVersion = errors.New("db version is equal to schema version")
)

type Query struct {
	Str  string
	Args []interface{}
}

type Runner struct {
	Workspace string
	Deploy    bool
	Dns       string
	Table     string
	Schema    string
}

func (r *Runner) Run() error {
	db, err := r.DB()

	if err != nil {
		return err
	}

	defer db.Close()

	schemaVersion, err := r.SchemaVersion()
	if err != nil {
		return err
	}

	dbVersion, err := r.DatabaseVersion(db)

	if err != nil {
		if !strings.Contains(err.Error(), "doesn't exist") {
			return err
		}
		return r.DeploySchema(db, schemaVersion)
	}

	if dbVersion == schemaVersion {
		return ErrEqualVersion
	}

	if err := r.UpgradeSchema(db, schemaVersion, dbVersion); err != nil {
		return err
	}

	return nil
}

func (r *Runner) DB() (*sql.DB, error) {
	return sql.Open("mysql", r.Dns)
}

func (r *Runner) DatabaseVersion(db *sql.DB) (version string, err error) {
	err = db.QueryRow(fmt.Sprintf("SELECT version FROM `%s`", r.Table)).Scan(&version)
	return
}

func (r *Runner) SchemaVersion() (string, error) {

	byt, err := r.execGitCmd("log", "-n", "1", "--pretty=format:%H", "--", r.Schema)
	if err != nil {
		return "", err
	}

	return string(byt), nil
}

func (r *Runner) DeploySchema(db *sql.DB, version string) error {
	var queries []Query
	content, err := r.schemaContent()
	if err != nil {
		return err
	}
	// TODO too casual...
	for _, stmt := range strings.Split(content, ";") {
		stmt = strings.TrimSpace(stmt)
		if len(stmt) == 0 {
			continue
		}
		queries = append(queries, Query{stmt, []interface{}{}})
	}
	queries = append(queries, Query{fmt.Sprintf("CREATE TABLE `%s` ( version VARCHAR(40) NOT NULL )", r.Table), []interface{}{}})
	queries = append(queries, Query{fmt.Sprintf("INSERT INTO `%s` (version) VALUES (?)", r.Table), []interface{}{version}})
	return r.execSql(db, queries)
}

func (r *Runner) UpgradeSchema(db *sql.DB, schemaVersion string, dbVersion string) error {
	commitSchema, err := r.schemaSpecificCommit(dbVersion)
	if err != nil {
		return err
	}

	stmts1, err := schemalex.NewParser(commitSchema).Parse()
	if err != nil {
		return err
	}

	content, err := r.schemaContent()
	if err != nil {
		return err
	}

	stmts2, err := schemalex.NewParser(content).Parse()
	if err != nil {
		return err
	}

	differ := &schemalex.Differ{filterCreateTableStatement(stmts1), filterCreateTableStatement(stmts2)}
	stmts := differ.DiffWithTransaction()

	var queries []Query

	for _, stmt := range stmts {
		queries = append(queries, Query{stmt, []interface{}{}})
	}

	queries = append(queries, Query{fmt.Sprintf("UPDATE %s SET version = ?", r.Table), []interface{}{schemaVersion}})

	return r.execSql(db, queries)
}

// private

func (r *Runner) schemaSpecificCommit(commit string) (string, error) {
	byt, err := r.execGitCmd("ls-tree", commit, "--", r.Schema)

	if err != nil {
		return "", err
	}

	fields := strings.Fields(string(byt))

	byt, err = r.execGitCmd("cat-file", "blob", fields[2])
	if err != nil {
		return "", err
	}

	return string(byt), nil
}

func (r *Runner) execSql(db *sql.DB, queries []Query) error {
	if !r.Deploy {
		for _, query := range queries {
			if len(query.Args) == 0 {
				fmt.Printf("%s;\n\n", query.Str)
			} else {
				fmt.Printf("%s; %v\n\n", query.Str, query.Args)
			}
		}
		return nil
	}

	for _, query := range queries {
		if _, err := db.Exec(query.Str, query.Args...); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) schemaContent() (string, error) {
	byt, err := ioutil.ReadFile(filepath.Join(r.Workspace, r.Schema))
	if err != nil {
		return "", err
	}
	return string(byt), nil
}

func (r *Runner) execGitCmd(args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	if r.Workspace != "" {
		cmd.Dir = r.Workspace
	}

	byt, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s got err:%s", cmd.Args, err)
	}

	return byt, nil
}

// util
func filterCreateTableStatement(stmts []schemalex.Stmt) []schemalex.CreateTableStatement {
	var createTableStatements []schemalex.CreateTableStatement
	for _, stmt := range stmts {
		switch t := stmt.(type) {
		case *schemalex.CreateTableStatement:
			createTableStatements = append(createTableStatements, *t)
		}
	}
	return createTableStatements
}
