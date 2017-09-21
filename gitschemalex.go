package gitschemalex

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	Commit    string
	Deploy    bool
	DSN       string
	Table     string
	Schema    string
}

func New() *Runner {
	return &Runner{
		Commit: "HEAD",
		Deploy: false,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	db, err := r.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	var schemaVersion string
	if err := r.SchemaVersion(ctx, &schemaVersion); err != nil {
		return err
	}

	var dbVersion string
	if err := r.DatabaseVersion(ctx, db, &dbVersion); err != nil {
		if !strings.Contains(err.Error(), "doesn't exist") {
			return err
		}
		return r.DeploySchema(ctx, db, schemaVersion)
	}

	if dbVersion == schemaVersion {
		return ErrEqualVersion
	}

	if err := r.UpgradeSchema(ctx, db, schemaVersion, dbVersion); err != nil {
		return err
	}

	return nil
}

func (r *Runner) DB() (*sql.DB, error) {
	return sql.Open("mysql", r.DSN)
}

func (r *Runner) DatabaseVersion(ctx context.Context, db *sql.DB, version *string) error {
	return db.QueryRowContext(ctx, fmt.Sprintf("SELECT version FROM `%s`", r.Table)).Scan(version)
}

func (r *Runner) SchemaVersion(ctx context.Context, version *string) error {
	// git rev-parse takes things like "HEAD" or commit hash, and gives
	// us the corresponding commit hash
	v, err := r.execGitCmd(ctx, "rev-parse", r.Commit)
	if err != nil {
		return err
	}

	*version = string(v)
	return nil
}

func (r *Runner) DeploySchema(ctx context.Context, db *sql.DB, version string) error {
	var content string
	if err := r.schemaSpecificCommit(ctx, version, &content); err != nil {
		return err
	}

	queries := queryListFromString(content)
	queries.AppendStmt(fmt.Sprintf("CREATE TABLE `%s` ( version VARCHAR(40) NOT NULL )", r.Table))
	queries.AppendStmt(fmt.Sprintf("INSERT INTO `%s` (version) VALUES (?)", r.Table), version)
	return r.execSql(ctx, db, queries)
}

func (r *Runner) UpgradeSchema(ctx context.Context, db *sql.DB, schemaVersion string, dbVersion string) error {
	var lastSchema string
	if err := r.schemaSpecificCommit(ctx, dbVersion, &lastSchema); err != nil {
		return err
	}

	var currentSchema string
	if err := r.schemaSpecificCommit(ctx, schemaVersion, &currentSchema); err != nil {
		return err
	}
	stmts := &bytes.Buffer{}
	p := schemalex.New()
	if err := diff.Strings(stmts, lastSchema, currentSchema, diff.WithTransaction(true), diff.WithParser(p)); err != nil {
		return err
	}

	queries := queryListFromString(stmts.String())
	queries.AppendStmt(fmt.Sprintf("UPDATE %s SET version = ?", r.Table), schemaVersion)

	return r.execSql(ctx, db, queries)
}

// private

func (r *Runner) schemaSpecificCommit(ctx context.Context, commit string, dst *string) error {
	// Old code used to do ls-tree and then cat-file, but I don't see why
	// you need to do this.
	// Doing
	// > fields := git ls-tree $commit -- $schema_file
	// And then taking fields[2] just gives us back $commit.
	// showing the contents at the point of commit using "git show" is much simpler
	v, err := r.execGitCmd(ctx, "show", fmt.Sprintf("%s:%s", commit, r.Schema))
	if err != nil {
		return err
	}

	*dst = string(v)
	return nil
}

func (r *Runner) execSql(ctx context.Context, db *sql.DB, queries queryList) error {
	if !r.Deploy {
		return queries.dump(os.Stdout)
	}
	return queries.execute(ctx, db)
}

func (r *Runner) execGitCmd(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if r.Workspace != "" {
		cmd.Dir = r.Workspace
	}

	byt, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s got err:%s", cmd.Args, err)
	}

	return byt, nil
}
