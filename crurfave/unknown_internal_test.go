package crurfave

import "testing"

// vfox says "No help topic for 'nosuch'. search": the word, not the rest.
func TestUnknownWord(t *testing.T) {
	for msg, want := range map[string]string{
		"No help topic for 'nosuch'":                    "nosuch",
		"No help topic for 'nosuch'. search":            "nosuch",
		"No help topic for 'deplyo'. Did you mean 'x'?": "deplyo",
	} {
		if got := unknownWord(msg); got != want {
			t.Errorf("%q: %q", msg, got)
		}
	}
}
