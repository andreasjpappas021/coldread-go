// Package coldread shows which AI agents use your Go website or API, CLI
// and MCP server. Three parts:
//
//   - NewTracker: net/http middleware for websites and APIs, the Go
//     counterpart of @coldread/track (secret key, COLDREAD_KEY).
//   - NewMCP: an MCP server's tool calls, the counterpart of
//     @coldread/cli/mcp, fed by the crmcp (official SDK) and crmcpgo
//     (mcp-go) helpers (public key).
//   - New: a CLI's runs, the counterpart of @coldread/cli (public key),
//     below.
//
// The CLI half sends the same events as @coldread/cli, with the same
// opt-outs, the same spool and the same COLDREAD_VERIFY lines.
//
//	cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
//	cr.Track("deploy preview", "--prod") // once the command is parsed
//	// ... run it ...
//	cr.Exit(code) // instead of os.Exit: sends, then exits
//
// Helpers for cobra (coldread.apappas.dev/go/crcobra) and urfave/cli
// (coldread.apappas.dev/go/crurfave) do the Track and the Exit for you; the
// standard flag package is TrackFlags.
//
// It never breaks your CLI: nothing panics, and nothing here returns an
// error. It barely slows it: the connection opens in the background at
// startup, and at exit the send waits ExitWait (300ms) at most; what isn't
// sent by then goes with the next run.
//
// Sent per run: your tool's name and version, the command path you pass,
// flag names (never values), exit code, duration, which agent ran it (by the
// names of the variables that said so, never their values), whether it ran
// in CI or at a person's terminal, a salted hash of the agent's session id,
// and os, arch and Go version. Never arguments, paths, hostnames, usernames,
// env values, a device id or anything typed. Coldread never stores the IP.
//
// Off with DO_NOT_TRACK=1, COLDREAD_DISABLED=1 or your own OptOut, and in
// test binaries unless COLDREAD_ENDPOINT points elsewhere. COLDREAD_DEBUG=1
// prints each event to stderr and sends nothing. COLDREAD_VERIFY=1 sends
// for real, waits, and prints what came back on stderr:
// "[coldread] verify: accepted", or why not.
package coldread

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Version is this SDK's version.
const Version = "0.2.0"

// DefaultEndpoint is where events go unless Options.Endpoint or
// COLDREAD_ENDPOINT says otherwise.
const DefaultEndpoint = "https://coldread.apappas.dev/api/ingest"

// Options configure a Client. Key and Tool are required.
type Options struct {
	// Key is your site's public key (cr_pub_...). It ships in your binary;
	// that's fine. A secret key (cr_sec_...) is never sent.
	Key string
	// Tool is your tool's name, as you want to see it ("acme").
	Tool string
	// Version is your tool's version.
	Version string
	// OptOut names an environment variable your users set to opt out
	// (set = opted out). DO_NOT_TRACK and COLDREAD_DISABLED always work.
	OptOut string
	// OptOutFunc returns true to opt out. A panic counts as opting out.
	OptOutFunc func() bool
	// Endpoint defaults to COLDREAD_ENDPOINT, then DefaultEndpoint.
	Endpoint string
	// InferParent also looks at parent processes for agents that set no
	// marker (Copilot CLI, Aider, Windsurf). Off by default: it runs ps on
	// macOS. Labeled "inferred".
	InferParent bool
	// Notice replaces the one-time notice shown on stderr at a person's
	// terminal. NoNotice turns it off.
	Notice   string
	NoNotice bool
}

// Client reports one run of your CLI. The zero value and nil do nothing.
type Client struct {
	tool, version, key string
	off                string
	debug, verifyMode  bool
	det                detection
	isTTY              bool
	started            time.Time
	now                func() time.Time
	write              func(string)
	exit               func(int)
	s                  *sender
	drained            chan struct{}

	mu      sync.Mutex
	command string
	flags   []string
	done    bool
}

// Seams for tests; none are needed in real use.
type internals struct {
	env       map[string]string
	isTTY     *bool
	stderrTTY *bool
	cacheDir  string
	now       func() time.Time
	write     func(string)
	exit      func(int)
	testRun   *bool
}

// New starts reporting this run. Call it once, at startup.
func New(opts Options) *Client { return newClient(opts, internals{}) }

func newClient(opts Options, in internals) (c *Client) {
	defer func() {
		if recover() != nil {
			c = &Client{off: "error"}
		}
	}()
	env := in.env
	if env == nil {
		env = environ()
	}
	now := in.now
	if now == nil {
		now = time.Now
	}
	write := in.write
	if write == nil {
		write = func(s string) { _, _ = os.Stderr.WriteString(s) }
	}
	exit := in.exit
	if exit == nil {
		exit = os.Exit
	}
	isTTY := anyTTY()
	if in.isTTY != nil {
		isTTY = *in.isTTY
	}
	testRun := isTestBinary()
	if in.testRun != nil {
		testRun = *in.testRun
	}

	c = &Client{
		tool: cleanTool(opts.Tool), version: cleanVersion(opts.Version), key: opts.Key,
		debug: env["COLDREAD_DEBUG"] == "1", verifyMode: env["COLDREAD_VERIFY"] == "1",
		isTTY: isTTY, started: now(), now: now, write: write, exit: exit,
	}
	endpoint := resolveEndpoint(opts.Endpoint, env)
	c.off = disabledBy(env, opts, endpoint, testRun)
	c.det = detectAgent(env, isTTY, nil)
	if c.det.agent == nil && opts.InferParent {
		func() {
			defer func() { _ = recover() }()
			c.det.agent = inferFromAncestors(ancestors())
		}()
	}
	cacheDir := in.cacheDir
	if cacheDir == "" {
		home, _ := os.UserHomeDir()
		cacheDir = cacheDirFor(c.tool, env, home)
	}
	c.s = newSender(endpoint, c.key, "coldread-go/"+Version, filepath.Join(cacheDir, "spool.jsonl"), now)

	switch {
	case c.off != "" && c.verifyMode:
		write(VerifyPrefix + "not sending (" + c.off + ").\n")
	case c.off != "" && c.debug:
		write("[coldread] not sending (" + c.off + ").\n")
	}
	if c.off == "" && !opts.NoNotice {
		stderrTTY := isTerminal(os.Stderr)
		if in.stderrTTY != nil {
			stderrTTY = *in.stderrTTY
		}
		notice(cacheDir, opts.Notice, c.tool, stderrTTY, now, write)
	}
	// The startup half of the send: open the connection, send the spool.
	if c.off == "" && !c.debug && !c.verifyMode && !c.det.networkDisabled {
		c.s.prewarm()
		c.drained = make(chan struct{})
		go func() {
			defer close(c.drained)
			defer func() { _ = recover() }()
			c.s.drain()
		}()
	}
	return c
}

// Agent is the AI coding agent running this process, or nil. Yours to use
// too, e.g. to skip a prompt no agent can answer. Detected even when
// reporting is off.
func (c *Client) Agent() *Agent {
	if c == nil || c.det.agent == nil {
		return nil
	}
	a := *c.det.agent
	a.Signals = append([]string(nil), a.Signals...)
	return &a
}

// Enabled is false when opted out (DO_NOT_TRACK, COLDREAD_DISABLED, OptOut),
// in a test binary, or without a valid public key.
func (c *Client) Enabled() bool { return c != nil && c.off == "" && c.s != nil }

// Track names the command once it's parsed: the path ("deploy preview"),
// never argv, and flag names ("--prod"); values after = are stripped. The
// last call before Finish wins.
func (c *Client) Track(command string, flags ...string) {
	if c == nil || c.s == nil {
		return
	}
	defer func() { _ = recover() }()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return
	}
	cleaned := cleanCommand(command)
	if cleaned == "" {
		if c.debug {
			c.write("[coldread] track: no usable command path.\n")
		}
		return
	}
	c.command, c.flags = cleaned, cleanFlags(flags)
}

// TrackFlags is Track for the standard flag package: the command, and the
// flags set on fs (flag.CommandLine when nil), as -x or --name.
func (c *Client) TrackFlags(command string, fs *flag.FlagSet) {
	if c == nil {
		return
	}
	if fs == nil {
		fs = flag.CommandLine
	}
	var names []string
	fs.Visit(func(f *flag.Flag) {
		if len(f.Name) == 1 {
			names = append(names, "-"+f.Name)
		} else {
			names = append(names, "--"+f.Name)
		}
	})
	c.Track(command, names...)
}

// Finish sends the run with its exit code, waiting ExitWait at most. Only
// the first call counts, and only after Track. Use it when main returns on
// its own; Exit when it calls os.Exit.
func (c *Client) Finish(exit int) {
	if c == nil || c.s == nil {
		return
	}
	defer func() { _ = recover() }()
	c.mu.Lock()
	if c.done || c.command == "" {
		untracked := !c.done && c.command == ""
		c.mu.Unlock()
		if untracked && c.off == "" && c.verifyMode {
			c.write(VerifyPrefix + "nothing sent (track() was never called).\n")
		}
		return
	}
	c.done = true
	command, flags := c.command, c.flags
	c.mu.Unlock()
	if c.off != "" {
		return
	}

	record := c.record(command, flags, exit)
	if c.debug {
		c.write("[coldread] " + string(record) + "\n")
	}
	if c.verifyMode {
		c.s.verify(record, c.det.networkDisabled, func(m string) { c.write(VerifyPrefix + m + "\n") })
		return
	}
	if c.debug {
		return
	}
	deadline := c.now().Add(ExitWait)
	// No network, or a recent send timed out: to the spool, no waiting.
	wait := !c.det.networkDisabled && !c.s.backedOff()
	if wait {
		c.s.sendOne(record, ExitWait)
	} else {
		toSpool(c.s.spool, [][]byte{record}, c.now())
	}
	// The spool sent at startup gets what's left of the wait; unfinished,
	// its events go back to the spool.
	if c.drained != nil {
		if left := deadline.Sub(c.now()); wait && left > 0 {
			t := time.NewTimer(left)
			select {
			case <-c.drained:
			case <-t.C:
			}
			t.Stop()
		}
		select {
		case <-c.drained:
		default:
			c.s.abandonDrain()
		}
	}
}

// Exit is Finish, then os.Exit(code). Use it wherever your CLI calls
// os.Exit, since os.Exit skips everything else.
func (c *Client) Exit(code int) {
	if c != nil {
		c.Finish(code)
		if c.exit != nil {
			c.exit(code)
			return
		}
	}
	os.Exit(code)
}

// Headers are for your CLI's own API calls, so your server (and Coldread's
// track SDK there) sees the agent: "User-Agent: <ua> AIAgent/<name>" and a
// structured AI-Agent header. Just the user-agent when no agent is running
// or the user opted out. userAgent "" means "<tool>/<version>".
func (c *Client) Headers(userAgent string) http.Header {
	h := http.Header{}
	if c == nil {
		if userAgent == "" {
			userAgent = "cli"
		}
		h.Set("User-Agent", userAgent)
		return h
	}
	if userAgent == "" {
		v := c.version
		if v == "" {
			v = "0"
		}
		userAgent = c.tool + "/" + v
	}
	if c.off != "" || c.det.agent == nil {
		h.Set("User-Agent", userAgent)
		return h
	}
	h.Set("User-Agent", userAgent+" AIAgent/"+c.det.agent.Name)
	h.Set("AI-Agent", formatAgentHeader(c.det.agent))
	return h
}

// --- the event ---

type wireAgent struct {
	Name       string   `json:"name"`
	Raw        *string  `json:"raw"`
	Version    *string  `json:"version"`
	Host       *string  `json:"host"`
	Evidence   string   `json:"evidence"`
	Confidence string   `json:"confidence"`
	Signals    []string `json:"signals"`
}

type wireTool struct {
	Name    string  `json:"name"`
	Version *string `json:"version"`
}

// wireRecord is one record in a POST /api/ingest batch, in @coldread/cli's
// field order.
type wireRecord struct {
	Source      string     `json:"source"`
	TS          int64      `json:"ts"`
	Tool        wireTool   `json:"tool"`
	Command     string     `json:"command"`
	Flags       []string   `json:"flags"`
	Exit        int        `json:"exit"`
	DurationMs  int64      `json:"durationMs"`
	Agent       *wireAgent `json:"agent"`
	CI          bool       `json:"ci"`
	Interactive bool       `json:"interactive"`
	Session     string     `json:"session,omitempty"`
	OS          string     `json:"os"`
	Arch        string     `json:"arch"`
	Runtime     string     `json:"runtime"`
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toWireAgent(a *Agent) *wireAgent {
	if a == nil {
		return nil
	}
	signals := a.Signals
	if signals == nil {
		signals = []string{}
	}
	return &wireAgent{Name: a.Name, Raw: strOrNil(a.Raw), Version: strOrNil(a.Version), Host: strOrNil(a.Host), Evidence: a.Evidence, Confidence: a.Confidence, Signals: signals}
}

func (c *Client) record(command string, flags []string, exit int) []byte {
	if exit < -1024 {
		exit = -1024
	} else if exit > 1024 {
		exit = 1024
	}
	d := c.now().Sub(c.started).Milliseconds()
	if d < 0 {
		d = 0
	} else if d > 86_400_000 {
		d = 86_400_000
	}
	if flags == nil {
		flags = []string{}
	}
	r := wireRecord{
		Source: "cli", TS: c.started.UnixMilli(),
		Tool:    wireTool{Name: c.tool, Version: strOrNil(c.version)},
		Command: command, Flags: flags, Exit: exit, DurationMs: d,
		CI: c.det.ci, Interactive: c.isTTY,
		OS: osName(), Arch: archName(), Runtime: runtimeTag(),
	}
	r.Agent = toWireAgent(c.det.agent)
	if c.det.sessionID != "" {
		r.Session = hashSession(c.det.sessionID, c.key)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(r)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// --- when it's off ---

var falsyOptOut = map[string]bool{"": true, "0": true, "false": true, "no": true, "off": true}

func truthy(env map[string]string, name string) bool {
	v, ok := env[name]
	return ok && !falsyOptOut[strings.ToLower(jsTrim(v))]
}

func resolveEndpoint(option string, env map[string]string) string {
	if option != "" {
		return option
	}
	if e := env["COLDREAD_ENDPOINT"]; e != "" {
		return e
	}
	return DefaultEndpoint
}

// isTestRun: a JavaScript test runner's markers, as @coldread/cli checks.
// A Go test binary is checked separately (isTestBinary).
func isTestRun(env map[string]string) bool {
	_, jest := env["JEST_WORKER_ID"]
	_, nodeTest := env["NODE_TEST_CONTEXT"]
	return truthy(env, "VITEST") || jest || nodeTest || env["NODE_ENV"] == "test"
}

// isTestBinary: this process is a `go test` binary.
func isTestBinary() bool {
	base := filepath.Base(os.Args[0])
	return strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe") || flag.Lookup("test.v") != nil
}

// disabledBy: why sending is off, or "". Checked once, at startup. A test
// run never sends to Coldread's production endpoint.
func disabledBy(env map[string]string, opts Options, endpoint string, testBinary bool) (reason string) {
	switch {
	case truthy(env, "DO_NOT_TRACK"):
		return "DO_NOT_TRACK"
	case truthy(env, "COLDREAD_DISABLED"):
		return "COLDREAD_DISABLED"
	case endpoint == DefaultEndpoint && (testBinary || isTestRun(env)):
		return "test run; set COLDREAD_ENDPOINT to send"
	case opts.OptOut != "" && truthy(env, opts.OptOut):
		return opts.OptOut
	}
	if opts.OptOutFunc != nil {
		out := func() (out bool) {
			defer func() {
				if recover() != nil {
					out = true
				}
			}()
			return opts.OptOutFunc()
		}()
		if out {
			return "optOut"
		}
	}
	if !publicKeyRe.MatchString(opts.Key) {
		return "key"
	}
	return ""
}

func defaultNotice(tool string) string {
	return tool + " sends anonymous usage data: which commands run and whether an AI agent ran them. Never arguments or personal data. Opt out: DO_NOT_TRACK=1"
}

// notice: once, on stderr, at a person's terminal only (an agent would
// read it as output). A marker file in the cache says it was shown.
func notice(cacheDir, text, tool string, stderrTTY bool, now func() time.Time, write func(string)) {
	if !stderrTTY {
		return
	}
	marker := filepath.Join(cacheDir, "notice")
	if _, err := os.Stat(marker); err == nil {
		return
	}
	if text == "" {
		text = defaultNotice(tool)
	}
	write(text + "\n")
	if os.MkdirAll(cacheDir, 0o700) == nil {
		_ = os.WriteFile(marker, []byte(now().UTC().Format("2006-01-02T15:04:05.000Z")+"\n"), 0o600)
	}
}

func environ() map[string]string {
	env := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	return env
}

// anyTTY: a person's terminal is attached (any of stdin, stdout, stderr).
func anyTTY() bool {
	return isTerminal(os.Stdin) || isTerminal(os.Stdout) || isTerminal(os.Stderr)
}
