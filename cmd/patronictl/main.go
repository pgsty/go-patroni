package main

import (
	"os"

	"github.com/pgsty/go-patroni/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
