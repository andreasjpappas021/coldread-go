//go:build !windows && !plan9 && !js && !wasip1

package coldread

import (
	"errors"
	"syscall"
)

// processGone: no process has this pid (a claim it left can be taken at
// once). A process we may not signal is still alive.
func processGone(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return errors.Is(err, syscall.ESRCH)
}
