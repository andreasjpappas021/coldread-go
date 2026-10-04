//go:build windows || plan9 || js || wasip1

package coldread

// processGone can't tell here: a claim waits out staleClaim.
func processGone(int) bool { return false }
