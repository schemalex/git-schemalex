package gitschemalex

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/schemalex/schemalex"
	"github.com/schemalex/schemalex/diff"
)

var (
	ErrEqualVersion = errors.New("db version is equal to schema version")
)

type Runner struct {
	Workspace string
	Deploy    bool
	DSN       string
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
	return sql.Open("mysql", r.DSN)
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
	content, err := r.schemaContent()
	if err != nil {
		return err
	}
	queries := queryListFromString(content)
	queries.AppendStmt(fmt.Sprintf("CREATE TABLE `%s` ( version VARCHAR(40) NOT NULL )", r.Table))
	queries.AppendStmt(fmt.Sprintf("INSERT INTO `%s` (version) VALUES (?)", r.Table), version)
	return r.execSql(db, queries)
}

func (r *Runner) UpgradeSchema(db *sql.DB, schemaVersion string, dbVersion string) error {

	lastSchema, err := r.schemaSpecificCommit(dbVersion)
	if err != nil {
		return err
	}

	currentSchema, err := r.schemaContent()
	if err != nil {
		return err
	}
	stmts := &bytes.Buffer{}
	p := schemalex.New()
	err = diff.Strings(stmts, lastSchema, currentSchema, diff.WithTransaction(true), diff.WithParser(p))
	if err != nil {
		return err
	}

	queries := queryListFromString(stmts.String())
	queries.AppendStmt(fmt.Sprintf("UPDATE %s SET version = ?", r.Table), schemaVersion)

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

func (r *Runner) execSql(db *sql.DB, queries queryList) error {
	if !r.Deploy {
		return queries.dump(os.Stdout)
	}
	return queries.execute(db)
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
