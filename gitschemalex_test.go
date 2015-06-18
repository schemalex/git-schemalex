package gitschemalex

import (
	"database/sql"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-test-mysqld"
)

func TestRunner(t *testing.T) {
	mysqld, err := mysqltest.NewMysqld(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mysqld.Stop()

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
	if err := exec.Command("git", "commit", "--author", "hoge <hoge@example.com>", "-m", "initial commit").Run(); err != nil {
		t.Fatal(err)
	}

	dns := mysqld.Datasource("", "", "", 0)
	r := &Runner{
		Workspace: dir,
		Deploy:    true,
		Dns:       dns,
		Table:     "git_schemalex_version",
		Schema:    "schema.sql",
	}
	if err := r.Run(); err != nil {
		t.Fatal(err)
	}

	// deployed

	db, err := sql.Open("mysql", dns)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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

	if err := r.Run(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO `fuga` (`id`, `c`) VALUES (1, '2')"); err != nil {
		t.Fatal(err)
	}

	// equal version

	if e, g := ErrEqualVersion, r.Run(); e != g {
		t.Fatal("should %v got %v", e, g)
	}
}
