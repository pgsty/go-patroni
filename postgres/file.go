package postgres

import (
	"context"
	"errors"
	"io"
	"os"
	"unicode/utf8"
)

const defaultMaxSQLFileBytes = int64(8 << 20)

func ReadSQLFile(ctx context.Context, path string, maximumBytes int64) (sql string, returnedError error) {
	if ctx == nil {
		return "", configurationError("read-sql-file", "context is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", newError(ErrorTransport, "read-sql-file", err)
	}
	if maximumBytes < 0 {
		return "", configurationError("read-sql-file", "maximum bytes must not be negative")
	}
	if maximumBytes == 0 {
		maximumBytes = defaultMaxSQLFileBytes
	}
	file, err := os.Open(path)
	if err != nil {
		return "", newError(ErrorConfiguration, "read-sql-file", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			returnedError = errors.Join(returnedError, newError(ErrorTransport, "close-sql-file", closeErr))
		}
	}()
	data, err := io.ReadAll(io.LimitReader(file, maximumBytes+1))
	if err != nil {
		return "", newError(ErrorTransport, "read-sql-file", err)
	}
	if err := ctx.Err(); err != nil {
		clear(data)
		return "", newError(ErrorTransport, "read-sql-file", err)
	}
	if int64(len(data)) > maximumBytes {
		clear(data)
		return "", newError(ErrorLimit, "read-sql-file", errors.New("sql file exceeds limit"))
	}
	if !utf8.Valid(data) {
		clear(data)
		return "", newError(ErrorConfiguration, "read-sql-file", errors.New("sql file is not UTF-8"))
	}
	return string(data), nil
}
