// Command compatgen regenerates go-patroni compatibility inventories from the
// pinned Patroni source checkout. Patroni Python is executed only as a
// compatibility-oracle process; it is never an SDK runtime dependency.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

var generatedFiles = []struct {
	kind string
	path string
}{
	{kind: "source", path: "compatibility/patroni-source.yaml"},
	{kind: "patronictl", path: "compatibility/patronictl.yaml"},
	{kind: "rest-api", path: "compatibility/rest-api.yaml"},
	{kind: "dcs", path: "compatibility/dcs.yaml"},
}

func main() {
	var source string
	var outputRoot string
	var check bool
	flag.StringVar(&source, "source", os.Getenv("PATRONI_SOURCE"), "path to the pinned Patroni source checkout")
	flag.StringVar(&outputRoot, "out", "", "repository root to receive generated files (default: locate go.mod)")
	flag.BoolVar(&check, "check", false, "verify generated files instead of writing them")
	flag.Parse()

	if source == "" {
		fatal(errors.New("Patroni source is required: pass -source or set PATRONI_SOURCE"))
	}
	var err error
	if outputRoot == "" {
		outputRoot, err = findRepositoryRoot()
		if err != nil {
			fatal(err)
		}
	}
	outputRoot, err = filepath.Abs(outputRoot)
	if err != nil {
		fatal(fmt.Errorf("resolve output root: %w", err))
	}
	source, err = filepath.Abs(source)
	if err != nil {
		fatal(fmt.Errorf("resolve Patroni source: %w", err))
	}

	script := filepath.Join(outputRoot, "test", "compat", "oracle", "extract_inventory.py")
	for _, file := range generatedFiles {
		content, err := generate(script, source, file.kind)
		if err != nil {
			fatal(err)
		}
		destination := filepath.Join(outputRoot, file.path)
		if check {
			current, err := os.ReadFile(destination)
			if err != nil {
				fatal(fmt.Errorf("read generated file %s: %w", file.path, err))
			}
			if !bytes.Equal(current, content) {
				fatal(fmt.Errorf("generated file is stale: %s (run make generate)", file.path))
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			fatal(fmt.Errorf("create destination for %s: %w", file.path, err))
		}
		if err := os.WriteFile(destination, content, 0o644); err != nil {
			fatal(fmt.Errorf("write %s: %w", file.path, err))
		}
	}
}

func generate(script, source, kind string) ([]byte, error) {
	cmd := exec.Command("python3", script, "--source", source, "--kind", kind)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	content, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("extract %s inventory: %w: %s", kind, err, stderr.String())
	}
	if !json.Valid(content) {
		return nil, fmt.Errorf("extract %s inventory: oracle returned invalid JSON", kind)
	}
	content = append(bytes.TrimSpace(content), '\n')
	return content, nil
}

func findRepositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not locate repository root containing go.mod")
		}
		dir = parent
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "compatgen:", err)
	os.Exit(1)
}
