package coldread

import (
	_ "embed"
	"encoding/json"
	"regexp"
	"strings"
)

// Which requests a server counts instead of sending: a port of
// @coldread/agents' people.ts, as coldread-sdk's people.py ports it. The
// same verdicts on the same shared cases (testdata/parity.json), and
// classify()'s own user-agent patterns (people.json, a copy of the Python
// SDK's, which packages/agents/test/parity.test.ts writes from the
// JavaScript and holds equal).
//
// People's page views go up as counts: a path, a method, a status, a minute
// and how many. No IP, no headers. Only what is confidently a person is
// counted; anything that might be an agent is sent whole, so Coldread
// classifies it again and verifies it with its IP.
//
//go:generate cp ../python/src/coldread/people.json people.json

//go:embed people.json
var peopleJSON []byte

// The verdicts: a person's page view, sent as a count; a person's request
// Coldread drops anyway (a prefetch, an asset, an API call), not sent at
// all; everything else, sent whole.
const (
	verdictCount = "count"
	verdictSkip  = "skip"
	verdictSend  = "send"
)

type personPatterns struct {
	browser   *regexp.Regexp
	notPeople []*regexp.Regexp
	// How many patterns people.json has, to check every one compiled.
	total int
}

var personUA = loadPersonPatterns(peopleJSON)

func loadPersonPatterns(data []byte) personPatterns {
	var raw struct {
		Browser   jsPattern   `json:"browser"`
		NotPeople []jsPattern `json:"notPeople"`
	}
	p := personPatterns{}
	if json.Unmarshal(data, &raw) != nil {
		return p // only a broken build gets here: nobody is a person, everything is sent
	}
	p.total = 1 + len(raw.NotPeople)
	if re, err := jsRegexp(raw.Browser.Source, raw.Browser.Flags); err == nil {
		p.browser = re
	}
	for _, n := range raw.NotPeople {
		if re, err := jsRegexp(n.Source, n.Flags); err == nil {
			p.notPeople = append(p.notPeople, re)
		}
	}
	return p
}

type jsPattern struct {
	Source string `json:"source"`
	Flags  string `json:"flags"`
}

// jsSpaceClass is JavaScript's \s as the inside of an RE2 character class.
const jsSpaceClass = `\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}`

// jsRegexp compiles a JavaScript regex (no u flag) as RE2: \s becomes
// JavaScript's whitespace, the i flag (?i). \d, \w and \b are ASCII in both.
func jsRegexp(source, flags string) (*regexp.Regexp, error) {
	var b strings.Builder
	if strings.Contains(flags, "i") {
		b.WriteString("(?i)")
	}
	inClass := false
	for i := 0; i < len(source); i++ {
		c := source[i]
		switch {
		case c == '\\' && i+1 < len(source):
			next := source[i+1]
			i++
			switch {
			case next == 's' && inClass:
				b.WriteString(jsSpaceClass)
			case next == 's':
				b.WriteString("[" + jsSpaceClass + "]")
			case next == 'S' && !inClass:
				b.WriteString("[^" + jsSpaceClass + "]")
			default:
				b.WriteByte('\\')
				b.WriteByte(next)
			}
		case c == '[' && !inClass:
			inClass = true
			b.WriteByte(c)
		case c == ']' && inClass:
			inClass = false
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return regexp.Compile(b.String())
}

// Headers only an agent sends: Web Bot Auth signatures, and the AI-Agent
// stamp of CLIs a coding agent drives. Any of them means "send it whole".
var agentHeaders = []string{"signature", "signature-input", "signature-agent", "ai-agent"}

var (
	apiPathRe = regexp.MustCompile(`^/api(/|$)`)
	// classify() skips an AIAgent/ token it can't parse; here any mention sends.
	aiAgentRe = regexp.MustCompile(`(?i)AIAgent/`)
)

// claimedPlatform: the OS a browser's user-agent claims, in
// Sec-CH-UA-Platform's words.
func claimedPlatform(ua string) string {
	switch {
	case strings.Contains(ua, "Android"):
		return "Android"
	case strings.Contains(ua, "iPhone"), strings.Contains(ua, "iPad"), strings.Contains(ua, "iPod"):
		return "iOS"
	case strings.Contains(ua, "Windows"):
		return "Windows"
	case strings.Contains(ua, "CrOS"):
		return "Chrome OS"
	case strings.Contains(ua, "Macintosh"), strings.Contains(ua, "Mac OS X"):
		return "macOS"
	case strings.Contains(ua, "Linux"), strings.Contains(ua, "X11"):
		return "Linux"
	}
	return ""
}

// disguised: user-agent and Sec-CH-UA-Platform disagree, a browser on a
// server dressed as someone's computer. Chrome on Android asking for the
// desktop site (claims Linux) is the one honest disagreement.
func disguised(ua, hint string) bool {
	hint = strings.Trim(jsTrim(hint), `"`)
	claim := claimedPlatform(ua)
	if hint == "" || claim == "" || hint == claim {
		return false
	}
	return !(claim == "Linux" && hint == "Android")
}

// isPerson: classify() says human. A browser's user-agent nothing else
// claims, not disguised, with Accept-Language (every real browser sends it).
func isPerson(get func(string) string) bool {
	ua := jsTrim(get("user-agent"))
	if ua == "" || personUA.browser == nil || !personUA.browser.MatchString(ua) {
		return false
	}
	for _, re := range personUA.notPeople {
		if re.MatchString(ua) {
			return false
		}
	}
	if disguised(ua, get("sec-ch-ua-platform")) {
		return false
	}
	return get("accept-language") != ""
}

// isPageLoad: a real page load asks for HTML. Prefetches and assets don't.
func isPageLoad(get func(string) string) bool {
	return strings.Contains(get("accept"), "text/html") || get("sec-fetch-dest") == "document"
}

// peopleRule decides what happens to a request: verdictCount,
// verdictSkip or verdictSend. get reads a header by lowercase name.
func peopleRule(method, path string, get func(string) string) string {
	for _, name := range agentHeaders {
		if get(name) != "" {
			return verdictSend
		}
	}
	if aiAgentRe.MatchString(get("user-agent")) {
		return verdictSend
	}
	if !isPerson(get) {
		return verdictSend
	}
	// The server's own rule for people (lib/hits.ts toRow): page loads only.
	if !isPageLoad(get) || apiPathRe.MatchString(path) {
		return verdictSkip
	}
	if m := strings.ToUpper(method); m == "GET" || m == "HEAD" {
		return verdictCount
	}
	return verdictSend
}
