package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadSQLFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.sql")
	if err := os.WriteFile(path, []byte("select 1;\nselect 2;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	query, err := ReadSQLFile(context.Background(), path, 64)
	if err != nil || query != "select 1;\nselect 2;\n" {
		t.Fatalf("SQL file read mismatch: query=%q err=%v", query, err)
	}
	_, err = ReadSQLFile(context.Background(), path, 3)
	var typed *Error
	if !errors.As(err, &typed) || typed.Kind != ErrorLimit || strings.Contains(err.Error(), path) {
		t.Fatalf("SQL file limit classification/redaction mismatch: %#v", err)
	}
}

func TestReadSQLFileRejectsInvalidInputs(t *testing.T) {
	directory := t.TempDir()
	invalid := filepath.Join(directory, "invalid.sql")
	if err := os.WriteFile(invalid, []byte{0xff, 0xfe}, 0o600); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		ctx  context.Context
		path string
		max  int64
		kind ErrorKind
	}{
		{name: "nil context", path: invalid, kind: ErrorConfiguration},
		{name: "canceled", ctx: canceled, path: invalid, kind: ErrorCanceled},
		{name: "negative limit", ctx: context.Background(), path: invalid, max: -1, kind: ErrorConfiguration},
		{name: "invalid UTF-8", ctx: context.Background(), path: invalid, max: 16, kind: ErrorConfiguration},
		{name: "missing", ctx: context.Background(), path: filepath.Join(directory, "protected-name.sql"), max: 16, kind: ErrorConfiguration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadSQLFile(test.ctx, test.path, test.max)
			var typed *Error
			if !errors.As(err, &typed) || typed.Kind != test.kind || strings.Contains(err.Error(), test.path) {
				t.Fatalf("SQL file error mismatch: %#v", err)
			}
		})
	}
}
