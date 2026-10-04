package coldread

import (
	"regexp"
	"strings"
)

// The detector: a port of @coldread/agents' detect.ts, reading the same
// registry (registry.json) and held to the same results by the golden cases
// it shares with the TypeScript and Python detectors (testdata/parity.json). Everything it reads is self-reported: any
// process can set any variable, so these results are "reported", never
// verified.

// Agent is the AI coding agent running this process, as detected.
type Agent struct {
	// Name is ours, normalized: "claude-code", "codex", "cursor". "unknown"
	// when something says an agent runs this without naming one (AGENT=1).
	Name string
	// Raw is the identity string as given (AI_AGENT, AGENT), or "" when a
	// marker variable named the agent.
	Raw     string
	Version string
	// Host is where the agent runs: terminal, desktop, vscode, sdk, remote.
	Host string
	// Evidence is "declared" (the agent's own marker) or "inferred" (guessed
	// from a parent process; Options.InferParent).
	Evidence string
	// Confidence is "high", "medium" or "low".
	Confidence string
	// Signals are the names of the variables that matched, never their values.
	Signals []string
}

type detection struct {
	agent           *Agent
	ci              bool
	sessionID       string
	networkDisabled bool
}

const unknownAgent = "unknown"

// jsSpace is JavaScript's \s (and what String.prototype.trim removes), so
// names normalize here exactly as they do in TypeScript.
func jsSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

func jsTrim(s string) string { return strings.TrimFunc(s, jsSpace) }

// jsLen is a string's length as JavaScript counts it (UTF-16 code units).
func jsLen(s string) int {
	n := 0
	for _, r := range s {
		n += units16(r)
	}
	return n
}

func units16(r rune) int {
	if r >= 0x10000 {
		return 2
	}
	return 1
}

// jsSlice is s.slice(0, n), counting UTF-16 code units as JavaScript does.
func jsSlice(s string, n int) string {
	units := 0
	for i, r := range s {
		u := units16(r)
		if units+u > n {
			return s[:i]
		}
		units += u
	}
	return s
}

func lowerTrim(v string) string { return strings.ToLower(jsTrim(v)) }

// How values read, as in detect.ts: a falsy word is "not set", a truthy
// word is "true".
var (
	falsyWords  = map[string]bool{"": true, "0": true, "false": true, "no": true, "off": true, "n": true}
	truthyWords = map[string]bool{"1": true, "true": true, "yes": true, "on": true, "y": true}
)

// networkDisabledVar set to a truthy word: Codex's sandbox has no network.
const networkDisabledVar = "CODEX_SANDBOX_NETWORK_DISABLED"

// isSet: set to anything but a falsy word.
func isSet(env map[string]string, name string) bool {
	v, ok := env[name]
	return ok && !falsyWords[lowerTrim(v)]
}

func isSetValue(v string) bool { return !falsyWords[lowerTrim(v)] }

func isTrue(v string) bool { return truthyWords[lowerTrim(v)] }

// normalizeAgentName: lowercase, _ and spaces to -, nothing outside
// [a-z0-9.-], at most 48 characters, then aliases. "" for junk.
func normalizeAgentName(raw string) string {
	s := strings.ToLower(jsTrim(raw))
	var b strings.Builder
	inRun := false
	for _, r := range s {
		if r == '_' || jsSpace(r) {
			if !inRun {
				b.WriteByte('-')
			}
			inRun = true
			continue
		}
		inRun = false
		b.WriteRune(r)
	}
	var kept strings.Builder
	for _, r := range b.String() {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' {
			kept.WriteRune(r)
		}
	}
	n := kept.String()
	for strings.Contains(n, "--") {
		n = strings.ReplaceAll(n, "--", "-")
	}
	n = strings.Trim(n, "-.")
	if len(n) > 48 {
		n = n[:48]
	}
	n = strings.TrimRight(n, "-.")
	if n == "" {
		return ""
	}
	if a, ok := registry.Aliases[n]; ok {
		return a
	}
	return n
}

var (
	underscoredID = regexp.MustCompile(`^([A-Za-z][\w.-]*?)_(\d+(?:-\d+)+)_[a-z][a-z0-9-]{0,31}$`)
	slashedID     = regexp.MustCompile(`^([^/\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]+)/v?(\d[\w.+-]{0,31})$`)
	versionRe     = regexp.MustCompile(`^v?(\d[\w.+-]{0,31})$`)
)

type agentID struct{ name, version string }

// parseAgentID reads `claude-code_2-1-281_agent` (Claude Code today),
// `claude-code/2.1.227` (older), or a bare name.
func parseAgentID(raw string) *agentID {
	v := jsTrim(raw)
	if v == "" || jsLen(v) > 128 {
		return nil
	}
	if truthyWords[strings.ToLower(v)] {
		return &agentID{name: unknownAgent}
	}
	if m := underscoredID.FindStringSubmatch(v); m != nil {
		if name := normalizeAgentName(m[1]); name != "" {
			return &agentID{name: name, version: strings.ReplaceAll(m[2], "-", ".")}
		}
		return nil
	}
	if m := slashedID.FindStringSubmatch(v); m != nil {
		if name := normalizeAgentName(m[1]); name != "" {
			return &agentID{name: name, version: m[2]}
		}
		return nil
	}
	if name := normalizeAgentName(v); name != "" {
		return &agentID{name: name}
	}
	return nil
}

func cleanAgentVersion(v string) string {
	if v == "" {
		return ""
	}
	if m := versionRe.FindStringSubmatch(jsTrim(v)); m != nil {
		return m[1]
	}
	return ""
}

func normalizeHost(raw string) string {
	s := strings.ToLower(jsTrim(raw))
	var b strings.Builder
	inRun := false
	for _, r := range s {
		if r == '_' || jsSpace(r) {
			if !inRun {
				b.WriteByte('-')
			}
			inRun = true
			continue
		}
		inRun = false
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' {
			b.WriteRune(r)
		}
	}
	h := b.String()
	if len(h) > 32 {
		h = h[:32]
	}
	return h
}

func hostFrom(env map[string]string, rule agentRule) string {
	if rule.Host == nil {
		return ""
	}
	raw := env[rule.Host.Env]
	if raw == "" {
		return ""
	}
	e := lowerTrim(raw)
	if to, ok := rule.Host.Exact[e]; ok {
		return to
	}
	for _, p := range rule.Host.Prefix {
		if strings.HasPrefix(e, p[0]) {
			return p[1]
		}
	}
	return normalizeHost(e)
}

func present(env map[string]string, names []string) []string {
	out := []string{}
	for _, n := range names {
		if isSet(env, n) {
			out = append(out, n)
		}
	}
	return out
}

func counts(m marker, v string, isTTY bool) bool {
	switch m.When {
	case "set":
		return isSetValue(v)
	case "true":
		return isTrue(v)
	case "no-tty":
		return isTrue(v) && !isTTY
	case "equals":
		return lowerTrim(v) == strings.ToLower(m.Value)
	}
	return false
}

func isCI(env map[string]string) bool {
	for _, n := range registry.CIVars {
		if isSet(env, n) {
			return true
		}
	}
	return false
}

// detectAgent: the first rule in the registry that matches, then the
// generic conventions (AI_AGENT, then AGENT). Markers that leak into
// people's terminals only count without a TTY.
func detectAgent(env map[string]string, isTTY bool, ciOverride *bool) detection {
	d := detection{ci: isCI(env), networkDisabled: isTrue(env[networkDisabledVar])}
	if ciOverride != nil {
		d.ci = *ciOverride
	}
	inherited := map[string]bool{}

	for _, rule := range registry.Rules {
		for _, m := range rule.Markers {
			v, ok := env[m.Env]
			if !ok {
				continue
			}
			if m.When == "no-tty" && isTrue(v) && isTTY {
				inherited[rule.Name] = true
			}
			if !counts(m, v, isTTY) {
				continue
			}
			// The agent-id variable's version counts, and it's a signal, only
			// when it names this agent; else the version variable's.
			version, signals := "", present(env, rule.Signals)
			var own *agentID
			if raw := env[rule.VersionFromAgentID]; rule.VersionFromAgentID != "" && raw != "" {
				if id := parseAgentID(raw); id != nil && id.name == rule.Name {
					own = id
				}
			}
			switch {
			case own != nil:
				version = own.version
				signals = append(signals, present(env, []string{rule.VersionFromAgentID})...)
			case rule.VersionEnv != "":
				version = cleanAgentVersion(env[rule.VersionEnv])
			}
			for _, n := range rule.Session {
				if s := env[n]; s != "" {
					d.sessionID = s
					break
				}
			}
			all := []string{m.Env}
			for _, s := range signals {
				if s != m.Env {
					all = append(all, s)
				}
			}
			d.agent = &Agent{Name: rule.Name, Version: version, Host: hostFrom(env, rule), Evidence: "declared", Confidence: m.Confidence, Signals: all}
			return d
		}
	}

	for _, name := range registry.GenericEnv {
		if !isSet(env, name) {
			continue
		}
		v := env[name]
		id := parseAgentID(v)
		if id == nil || inherited[id.name] {
			continue
		}
		confidence := "medium"
		if id.name == unknownAgent {
			confidence = "low"
		}
		d.agent = &Agent{Name: id.name, Raw: jsSlice(jsTrim(v), 128), Version: id.version, Evidence: "declared", Confidence: confidence, Signals: []string{name}}
		return d
	}
	return d
}

// agentFromProcess: an agent with no marker, by the program that started
// this one (argv[0], or argv[1] for a Node CLI's script).
func agentFromProcess(argv []string) string {
	if len(argv) > 2 {
		argv = argv[:2]
	}
	for _, arg := range argv {
		base := arg
		if i := strings.LastIndexAny(base, `/\`); i >= 0 {
			base = base[i+1:]
		}
		lower := strings.ToLower(base)
		for _, ext := range []string{".exe", ".js", ".cjs", ".mjs"} {
			if strings.HasSuffix(lower, ext) {
				base = base[:len(base)-len(ext)]
				break
			}
		}
		for _, p := range registry.ProcessNames {
			if p.re.MatchString(base) {
				return p.Agent
			}
		}
	}
	return ""
}

func inferFromAncestors(ancestors [][]string) *Agent {
	for _, argv := range ancestors {
		if name := agentFromProcess(argv); name != "" {
			return &Agent{Name: name, Evidence: "inferred", Confidence: "low", Signals: []string{}}
		}
	}
	return nil
}

// formatAgentHeader is the AI-Agent request header (an RFC 9651 dictionary):
//
//	name="claude-code", version="2.1.281", host="terminal", evidence="declared"
func formatAgentHeader(a *Agent) string {
	q := func(s string) string {
		var b strings.Builder
		b.WriteByte('"')
		for _, r := range s {
			switch {
			case r == '\\' || r == '"':
				b.WriteByte('\\')
				b.WriteRune(r)
			case r >= 0x20 && r <= 0x7e:
				b.WriteRune(r)
			}
		}
		b.WriteByte('"')
		return b.String()
	}
	parts := []string{"name=" + q(a.Name)}
	if a.Version != "" {
		parts = append(parts, "version="+q(a.Version))
	}
	if a.Host != "" {
		parts = append(parts, "host="+q(a.Host))
	}
	parts = append(parts, "evidence="+q(a.Evidence))
	return strings.Join(parts, ", ")
}
