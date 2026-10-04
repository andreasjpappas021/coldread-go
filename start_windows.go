//go:build windows

package coldread

import (
	"syscall"
	"time"
)

// osProcessStart: when this process began (GetProcessTimes' creation time).
func osProcessStart() (time.Time, bool) {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return time.Time{}, false
	}
	var created, exited, kernel, user syscall.Filetime
	if syscall.GetProcessTimes(h, &created, &exited, &kernel, &user) != nil {
		return time.Time{}, false
	}
	return time.Unix(0, created.Nanoseconds()), true
}
