package cli

import "bytes"

func trimLineEnding(value []byte) []byte {
	return bytes.TrimRight(value, "\r\n")
}
