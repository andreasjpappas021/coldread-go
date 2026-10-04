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
	// A CLI's command path and flag names: @coldread/agents/clean's rules,
	// which ingest applies too (shared golden cases in testdata/parity.json:
	// cleanCommand, cleanFlags, isHelpRun). MCP tool names keep the spec's
	// characters (mcpToolRe).
	commandPartRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:._-]{0,39}$`)
	fileOrHostRe  = regexp.MustCompile(`\.[A-Za-z]`)
	longFlagRe    = regexp.MustCompile(`^--[a-z0-9][a-z0-9_-]{0,31}$`)
	shortFlagRe   = regexp.MustCompile(`^-(?:[A-Za-z0-9]|[a-z]{2,3})$`)
	secretRe      = regexp.MustCompile(`^(?:(?:sk|pk|rk)[-_]|gh[pousr]_|github_pat_|xox[abpors]-|(?:AKIA|ASIA)[A-Z0-9]{4}|eyJ|glpat-|npm_|pypi-|AIza|ya29\.|hf_|whsec_|shp(?:at|ss|ca|pa)_|dop_v1_|cr_(?:sec|pub)_|SG\.)`)
	digitRunRe    = regexp.MustCompile(`[0-9]{6,}`)
	hexRe         = regexp.MustCompile(`^[0-9a-fA-F-]+$`)
	mcpToolRe     = regexp.MustCompile(`^[\w.:/@+-]{1,128}$`)
	toolBadRe     = regexp.MustCompile(`[^\w.@/-]`)
	toolLeadRe    = regexp.MustCompile(`^[^A-Za-z0-9@]+`)
	cacheBadRe    = regexp.MustCompile(`[^a-z0-9._-]`)
	runtimeBadRe  = regexp.MustCompile(`[^\w.+-]`)
)

const (
	maxCommandParts = 4
	maxFlags        = 32
)

// looksSecret: a secret, a key or an id rather than a word: a known
// credential prefix, 6+ digits in a row, a long hex string (UUIDs, hashes),
// or 20+ characters mixing upper and lower case with digits.
func looksSecret(part string) bool {
	if secretRe.MatchString(part) || digitRunRe.MatchString(part) {
		return true
	}
	if len(part) >= 12 && hexRe.MatchString(part) && strings.ContainsAny(part, "0123456789") && strings.ContainsAny(part, "abcdefABCDEF") {
		return true
	}
	return len(part) >= 20 && strings.ContainsAny(part, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(part, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(part, "0123456789")
}

var (
	addrV4Re = regexp.MustCompile(`^[0-9]{1,3}(?:\.[0-9]{1,3}){3}(?::[0-9]+)?$`)
	addrV6Re = regexp.MustCompile(`^[0-9A-Fa-f:.]+$`)
)

// looksAddress: an address or a credential pair rather than a word: IPv4
// (with or without a port), IPv6, host:port, or x:y whose part after a
// colon has a digit or a capital letter.
func looksAddress(part string) bool {
	if addrV4Re.MatchString(part) {
		return true
	}
	if strings.Count(part, ":") >= 2 && addrV6Re.MatchString(part) && (strings.ContainsAny(part, "0123456789") || strings.Contains(part, "::")) {
		return true
	}
	for _, s := range strings.Split(part, ":")[1:] {
		if strings.ContainsAny(s, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			return true
		}
	}
	return false
}

// cleanCommandParts: the command path as kept, up to 4 plain words, and
// what was dropped (for COLDREAD_DEBUG). The path ends at the first part
// that isn't a word (a path, a URL, a value, a file or host name, something
// like a key): whatever follows it is an argument.
func cleanCommandParts(command string) (string, []string) {
	parts := strings.FieldsFunc(command, jsSpace)
	kept := []string{}
	i := 0
	for ; i < len(parts) && len(kept) < maxCommandParts; i++ {
		p := parts[i]
		if !commandPartRe.MatchString(p) || fileOrHostRe.MatchString(p) || looksSecret(p) || looksAddress(p) {
			break
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, " "), parts[i:]
}

// cleanCommand: the command path ("deploy preview"), "" when nothing is a
// word. The caller passes the path; argv is never read.
func cleanCommand(command string) string {
	c, _ := cleanCommandParts(command)
	return c
}

// cleanFlag: a flag's name, or "": what's after = goes, the rest must be
// `--long-name` or a short `-x`.
func cleanFlag(f string) string {
	name := jsTrim(strings.SplitN(f, "=", 2)[0])
	if !longFlagRe.MatchString(name) && !shortFlagRe.MatchString(name) {
		return ""
	}
	if looksSecret(strings.TrimLeft(name, "-")) {
		return ""
	}
	return name
}

// cleanFlagsParts: names only, each once, at most 32; and what was dropped.
func cleanFlagsParts(flags []string) ([]string, []string) {
	out := []string{}
	var dropped []string
	seen := map[string]bool{}
	for _, f := range flags {
		name := cleanFlag(f)
		if name == "" {
			if f != "" {
				dropped = append(dropped, f)
			}
			continue
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
		if len(out) >= maxFlags {
			break
		}
	}
	return out, dropped
}

// cleanFlags: flag names only. Anything after = is dropped, anything that
// isn't a flag name (a value passed by mistake) is dropped, 32 at most.
func cleanFlags(flags []string) []string {
	out, _ := cleanFlagsParts(flags)
	return out
}

// isHelpRun: a run that only asked for help or the version (`--help`,
// `help ...`, the root's `--version`). Not a use of the tool: not sent.
func isHelpRun(command string, flags []string, tool string) bool {
	if contains(flags, "--help") {
		return true
	}
	if command == tool && contains(flags, "--version") {
		return true
	}
	return strings.SplitN(command, " ", 2)[0] == "help"
}

// cleanToolName: an MCP tool name as ingest accepts it, one word of the
// spec's characters, at most 128; "" when it isn't one.
func cleanToolName(name string) string {
	n := jsTrim(name)
	if mcpToolRe.MatchString(n) {
		return n
	}
	return ""
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
	return filepath.Join(root, "coldread", cacheName(tool))
}

// cacheName: the tool's folder under coldread/.
func cacheName(tool string) string {
	dir := cacheBadRe.ReplaceAllString(strings.ToLower(tool), "-")
	if len(dir) > 64 {
		dir = dir[:64]
	}
	if dir == "" {
		dir = "tool"
	}
	return dir
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
