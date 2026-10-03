//go:build !windows

package coldread

import "os"

// isTerminal: a character device other than /dev/null. The standard library
// has no isatty, and a raw ioctl isn't safe everywhere (OpenBSD forbids
// direct syscalls); other character devices on stdio are vanishingly rare.
func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	null, err := os.Stat(os.DevNull)
	return err != nil || !os.SameFile(st, null)
}
