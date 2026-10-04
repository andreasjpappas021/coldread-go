//go:build !windows

package coldread

import (
	"errors"
	"os"
	"syscall"
)

var errNotPrivate = errors.New("not this user's private folder")

// privateDir: dir is a real folder (not a link), owned by this user, that
// nobody else can read or write. create makes it (0700) when missing. The
// temp folder can be shared, so its spool lives only under a folder like
// this.
func privateDir(dir string, create bool) error {
	if create {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	st, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() || st.Mode().Perm()&0o077 != 0 || int(sys.Uid) != os.Getuid() {
		return errNotPrivate
	}
	return nil
}
