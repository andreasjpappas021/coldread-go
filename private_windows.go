//go:build windows

package coldread

import (
	"errors"
	"os"
)

var errNotPrivate = errors.New("not a folder")

// privateDir: dir is a folder (made when create is set). The temp folder
// is per user on Windows (%TEMP% under the profile).
func privateDir(dir string, create bool) error {
	if create {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	st, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errNotPrivate
	}
	return nil
}
