//go:build windows

package coldread

import (
	"os"
	"syscall"
)

// isTerminal: a console handle.
func isTerminal(f *os.File) bool {
	var mode uint32
	return syscall.GetConsoleMode(syscall.Handle(f.Fd()), &mode) == nil
}
