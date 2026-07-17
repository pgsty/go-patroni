package main

import (
	"os"

	"github.com/pgsty/go-patroni/cli"
)

func main() {
	os.Exit(cli.Execute(cli.Options{}))
}
