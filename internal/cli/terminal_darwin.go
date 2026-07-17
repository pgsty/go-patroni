//go:build darwin

package cli

import (
	"bufio"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func isTerminalFile(file *os.File) bool {
	_, err := unix.IoctlGetTermios(int(file.Fd()), unix.TIOCGETA)
	return err == nil
}

func readPasswordFile(file *os.File) ([]byte, error) {
	fd := int(file.Fd())
	original, err := unix.IoctlGetTermios(fd, unix.TIOCGETA)
	if err != nil {
		return nil, err
	}
	hidden := *original
	hidden.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TIOCSETA, &hidden); err != nil {
		return nil, err
	}
	defer func() { _ = unix.IoctlSetTermios(fd, unix.TIOCSETA, original) }()

	line, err := bufio.NewReader(file).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return trimLineEnding(line), nil
}
