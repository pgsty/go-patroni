//go:build !darwin && !linux

package cli

import "os"

func isTerminalFile(_ *os.File) bool { return false }

func readPasswordFile(_ *os.File) ([]byte, error) { return nil, os.ErrInvalid }
