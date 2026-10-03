package coldread

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// The ingest endpoint's rules (apps/web/lib/ingest.ts), as @coldread/cli
// applies them, so nothing is sent that it would refuse.
var (
	publicKeyRe   = regexp.MustCompile(`^cr_pub_[0-9a-f]{32}$`)
	toolNameRe    = regexp.MustCompile(`^[A-Za-z0-9@][\w.@/-]{0,63}$`)
	versionWireRe = regexp.MustCompile(`^v?[0-9A-Za-z][\w.+-]{0,31}$`)
	commandPartRe = regexp.MustCompile(`^[\w.:/@+-]+$`)
	flagRe        = regexp.MustCompile(`^--?[A-Za-z0-9][\w-]{0,47}$`)
	toolBadRe     = regexp.MustCompile(`[^\w.@/-]`)
	toolLeadRe    = regexp.MustCompile(`^[^A-Za-z0-9@]+`)
	cacheBadRe    = regexp.MustCompile(`[^a-z0-9._-]`)
	runtimeBadRe  = regexp.MustCompile(`[^\w.+-]`)
)

// cleanCommand: the command path as ingest accepts it, parts that look like
// words, at most 128 characters. "" when nothing is usable.
func cleanCommand(command string) string {
	out := ""
	for _, p := range strings.FieldsFunc(jsTrim(command), jsSpace) {
		if !commandPartRe.MatchString(p) {
			continue
		}
		next := p
		if out != "" {
			next = out + " " + p
		}
		if len(next) > 128 {
			break
		}
		out = next
	}
	return out
}

// cleanFlags: names only. Anything after = is dropped, anything that isn't
// a flag (a value passed by mistake) is dropped, 32 at most.
func cleanFlags(flags []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, f := range flags {
		name := jsTrim(strings.SplitN(f, "=", 2)[0])
		if flagRe.MatchString(name) && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
		if len(out) >= 32 {
			break
		}
	}
	return out
}

func cleanTool(name string) string {
	if toolNameRe.MatchString(name) {
		return name
	}
	s := toolLeadRe.ReplaceAllString(toolBadRe.ReplaceAllString(name, "-"), "")
	if len(s) > 64 {
		s = s[:64]
	}
	if s == "" {
		return "tool"
	}
	return s
}

func cleanVersion(v string) string {
	if versionWireRe.MatchString(v) {
		return v
	}
	return ""
}

// hashSession: a salted, truncated SHA-256 that groups one agent session's
// events without the id leaving the machine. Salted with the site's key, as
// @coldread/cli does, so one customer's Node and Go tools agree.
func hashSession(id, key string) string {
	sum := sha256.Sum256([]byte("coldread:" + key + ":" + id))
	return hex.EncodeToString(sum[:])[:16]
}

// cacheDirFor: $XDG_CACHE_HOME, %LOCALAPPDATA% on Windows, else ~/.cache;
// then coldread/<tool>. The same folder @coldread/cli uses.
func cacheDirFor(tool string, env map[string]string, home string) string {
	root := env["XDG_CACHE_HOME"]
	if root == "" && runtime.GOOS == "windows" {
		root = env["LOCALAPPDATA"]
	}
	if root == "" {
		root = filepath.Join(home, ".cache")
	}
	dir := cacheBadRe.ReplaceAllString(strings.ToLower(tool), "-")
	if len(dir) > 64 {
		dir = dir[:64]
	}
	if dir == "" {
		dir = "tool"
	}
	return filepath.Join(root, "coldread", dir)
}

// Node's names for the platform and CPU, so events from Go and Node tools
// read the same: windows is win32, amd64 is x64, 386 is ia32.
func osName() string {
	if runtime.GOOS == "windows" {
		return "win32"
	}
	return runtime.GOOS
}

func archName() string {
	switch runtime.GOARCH {
	case "amd64":
		return "x64"
	case "386":
		return "ia32"
	}
	return runtime.GOARCH
}

// runtimeTag: go/1.26.2.
func runtimeTag() string {
	v := runtime.Version() // go1.26.2, or "devel go1.27-abcdef ..." on a dev toolchain
	if strings.HasPrefix(v, "devel") {
		v = "devel"
	}
	v = strings.TrimPrefix(v, "go")
	v = runtimeBadRe.ReplaceAllString(v, "-")
	if len(v) > 32 {
		v = v[:32]
	}
	if v == "" {
		v = "0"
	}
	return "go/" + v
}
