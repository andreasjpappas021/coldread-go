//go:build linux

package coldread

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// osProcessStart: when this process began: /proc/self/stat's start time
// (clock ticks since boot, field 22), against CLOCK_BOOTTIME now. Ticks
// are USER_HZ, 100 on every platform Go runs Linux on.
func osProcessStart() (time.Time, bool) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return time.Time{}, false
	}
	// The name (field 2) can hold spaces and parentheses: count from the last ")".
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return time.Time{}, false
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 20 {
		return time.Time{}, false
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64) // field 22
	if err != nil || ticks < 0 {
		return time.Time{}, false
	}
	var ts syscall.Timespec
	const clockBoottime = 7
	if _, _, e := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockBoottime, uintptr(unsafe.Pointer(&ts)), 0); e != 0 {
		return time.Time{}, false
	}
	now := time.Now()
	since := time.Duration(ts.Nano()) - time.Duration(ticks)*(time.Second/100)
	if since < 0 {
		return time.Time{}, false
	}
	return now.Add(-since).Round(0), true
}
