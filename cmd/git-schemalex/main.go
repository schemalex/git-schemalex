package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/soh335/git-schemalex"
)

var (
	workspace = flag.String("workspace", "", "workspace of git")
	deploy    = flag.Bool("deploy", false, "deploy")
	dns       = flag.String("dns", "", "")
	table     = flag.String("table", "git_schemalex_version", "table of git revision")
	schema    = flag.String("schema", "", "path to schema file")
)

func main() {
	flag.Parse()
	if err := _main(); err != nil {
		log.Fatal(err)
	}
}

func _main() error {
	r := &gitschemalex.Runner{
		Workspace: *workspace,
		Deploy:    *deploy,
		Dns:       *dns,
		Table:     *table,
		Schema:    *schema,
	}
	err := r.Run()
	if err == gitschemalex.ErrEqualVersion {
		fmt.Println(err.Error())
		return nil
	}
	return err
}
