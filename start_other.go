//go:build !darwin && !linux && !windows

package coldread

import "time"

// osProcessStart: not read here; runs count from package init.
func osProcessStart() (time.Time, bool) { return time.Time{}, false }
