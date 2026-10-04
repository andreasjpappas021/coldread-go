//go:build darwin

package coldread

import (
	"encoding/binary"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// osProcessStart: when this process began, from sysctl kern.proc.pid:
// kinfo_proc begins with extern_proc, whose first field is p_starttime, a
// timeval (64-bit seconds, 32-bit microseconds).
func osProcessStart() (time.Time, bool) {
	mib := [4]int32{1, 14, 1, int32(os.Getpid())} // CTL_KERN, KERN_PROC, KERN_PROC_PID
	var buf [1024]byte
	size := uintptr(len(buf))
	_, _, e := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), 4, uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&size)), 0, 0)
	if e != 0 || size < 16 {
		return time.Time{}, false
	}
	sec := int64(binary.LittleEndian.Uint64(buf[0:8]))
	usec := int64(int32(binary.LittleEndian.Uint32(buf[8:12])))
	if sec <= 0 || usec < 0 || usec >= 1_000_000 {
		return time.Time{}, false
	}
	return time.Unix(sec, usec*1000), true
}
