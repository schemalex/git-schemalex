package gitschemalex

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-test-mysqld"
)

func TestRunner(t *testing.T) {
	var dsn = "root:@tcp(127.0.0.1:3306)/mysql"
	if ok, _ := strconv.ParseBool(os.Getenv("TRAVIS")); !ok {
		mysqld, err := mysqltest.NewMysqld(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer mysqld.Stop()
		dsn = mysqld.DSN()
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS `test`"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("USE `test`"); err != nil {
		t.Fatal(err)
	}

	dir, err := ioutil.TempDir("", "gitschemalex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "init").Run(); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "config", "user.email", "hoge@example.com").Run(); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "config", "user.name", "hoge").Run(); err != nil {
		t.Fatal(err)
	}

	schema, err := os.Create(filepath.Join(dir, "schema.sql"))
	if err != nil {
		t.Fatal(err)
	}

	// first table

	if _, err := schema.WriteString("CREATE TABLE hoge ( `id` INTEGER NOT NULL, `c` VARCHAR(20) );\n"); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "add", "schema.sql").Run(); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "commit", "-m", "initial commit").Run(); err != nil {
		t.Fatal(err)
	}

	// This is a silly hack, but we need to change the DSN from "mysql" or
	// whatever to "test"
	re := regexp.MustCompile(`/[^/]+$`)
	dsn = re.ReplaceAllString(dsn, `/test`)
	r := &Runner{
		Workspace: dir,
		Deploy:    true,
		DSN:       dsn,
		Table:     "git_schemalex_version",
		Schema:    "schema.sql",
	}
	if err := r.Run(context.TODO()); err != nil {
		t.Fatal(err)
	}

	// deployed
	if _, err := db.Exec("INSERT INTO `hoge` (`id`, `c`) VALUES (1, '2')"); err != nil {
		t.Fatal(err)
	}

	// second table

	if _, err := schema.WriteString("CREATE TABLE fuga ( `id` INTEGER NOT NULL, `c` VARCHAR(20) );\n"); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("git", "add", "schema.sql").Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("git", "commit", "--author", "hoge <hoge@example.com>", "-m", "second commit").Run(); err != nil {
		t.Fatal(err)
	}

	if err := r.Run(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO `fuga` (`id`, `c`) VALUES (1, '2')"); err != nil {
		t.Fatal(err)
	}

	// equal version
	if err := r.Run(context.TODO()); err != ErrEqualVersion {
		t.Fatal("should %v got %v", err, ErrEqualVersion)
	}
}
