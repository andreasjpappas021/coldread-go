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
// Killed (SIGTERM, SIGHUP, SIGQUIT: what agents' timeouts send), a run is
// recorded only when the CLI asks: catching a signal in Go turns its
// default exit off, so Coldread never does it unasked, and by default a
// killed run is simply not recorded. CaptureSignals (no handler of its
// own): saved to the spool as 128+n and raised again at once. OwnSignals
// (the CLI handles the signal itself): its handler runs as ever and the run
// reads 128+n when it ends. Ctrl-C (SIGINT) sends nothing; SIGKILL can't be
// seen. A refused command line is ParseError (exit
// 2); a panic is Recover (exit 2). Help and version runs aren't sent.
//
// Off with DO_NOT_TRACK=1, COLDREAD_DISABLED=1 or your own OptOut, and in
// test binaries (unless COLDREAD_VERIFY=1 to an endpoint that isn't
// production). COLDREAD_DEBUG=1
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
	"sync/atomic"
	"time"
)

// Version is this SDK's version.
const Version = "0.3.1"

// DefaultEndpoint is where events go unless Options.Endpoint or
// COLDREAD_ENDPOINT says otherwise.
const DefaultEndpoint = "https://coldread.apappas.dev/api/ingest"

// processStart is when this package was initialized, before main runs. A
// run's duration counts from the process's start (processStarted), never
// from New: the startup a CLI does before New counts, as the person or
// agent waiting on it felt it.
var processStart = time.Now()

// processStarted: when this process began, as the OS says (macOS, Linux,
// Windows), else when this package was initialized. Loading a large binary
// and initializing the packages before this one can take tens of
// milliseconds, all of it before package init.
func processStarted() time.Time {
	if t, ok := osProcessStart(); ok {
		if before := processStart.Sub(t); before >= 0 && before < time.Hour {
			return processStart.Add(-before) // keeps the monotonic reading
		}
	}
	return processStart
}

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
	// CaptureSignals records runs killed by SIGTERM, SIGHUP or SIGQUIT, for
	// a CLI with no handler of its own: the run is saved to the spool as
	// 128+n and the signal raised again at once, so the CLI dies of it as
	// before. Off by default (Go can't tell whether anyone else listens for
	// a signal, and catching one turns its default exit off): killed runs
	// are then not recorded. A CLI that also handles the signal would get
	// the raised one as a second signal: use OwnSignals there.
	CaptureSignals bool
	// OwnSignals records killed runs for a CLI that handles SIGTERM, SIGHUP
	// and SIGQUIT itself (signal.Notify, signal.NotifyContext: a server, a
	// watcher), and only then. Its handler runs as ever and ends the run
	// (Finish, Exit); the run reads 128+n. Coldread never raises the signal
	// again: set on a CLI without a handler, SIGTERM would no longer end it
	// (only SIGKILL would). Finish waits 2 ms for a signal not yet read.
	OwnSignals bool
	// Expected: exit codes that are a normal result, not a failure, by
	// command path: {"detect": {1}} for a scanner that exits 1 when it
	// finds something. The root command is "" (or the tool's name). Codes
	// 1-255. Sent once (again only when it changes) and kept as the site's
	// expected outcomes, so those runs never count as dead ends. Only what
	// your tool documents.
	Expected map[string][]int
}

// ownSignalWait: how long an OwnSignals run's Finish waits for a kill its
// handler saw first.
const ownSignalWait = 2 * time.Millisecond

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
	cacheDir           string
	expected           map[string][]int

	mu      sync.Mutex
	command string
	flags   []string
	done    bool
	// unusable: Track was given a command with nothing ingest accepts.
	unusable bool
	// parseFailed: ParseError named the run (exit 2, help or not).
	parseFailed bool
	// killedBy: the kill signal this run got (SIGTERM 15, SIGHUP 1,
	// SIGQUIT 3), 0 for none. Its exit reads 128+n.
	killedBy       int
	captureSignals bool
	ownSignals     bool
	sigCh          chan os.Signal
	// sigSeen: a kill signal the watcher has read (its number), before it
	// takes the lock to note it.
	sigSeen atomic.Int32
}

// Seams for tests; none are needed in real use.
type internals struct {
	env       map[string]string
	isTTY     *bool
	stderrTTY *bool
	cacheDir  string
	tmpDir    string
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
	var started time.Time
	if now == nil {
		now, started = time.Now, processStarted()
	} else {
		started = now()
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
		isTTY: isTTY, started: started, now: now, write: write, exit: exit,
		ownSignals:     opts.OwnSignals,
		captureSignals: opts.CaptureSignals,
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
	c.cacheDir = cacheDir
	c.expected = cleanExpected(opts.Expected, c.tool)
	c.s = newSender(endpoint, c.key, "coldread-go/"+Version, filepath.Join(cacheDir, "spool.jsonl"), now)
	tmp := in.tmpDir
	if tmp == "" {
		tmp = os.TempDir()
	}
	c.s.alt = tmpSpoolFor(c.tool, tmp)

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
// root command itself is "" (or the tool's name): it's sent as the tool's
// name, so a bare `acme` reads "acme". The last call before Finish wins.
//
// A command is up to 4 plain words; the path ends at the first part that
// isn't one (a path, a URL, a value, something like a key). Flags are
// `--long-name` or `-x`; anything else is dropped. COLDREAD_DEBUG=1 says
// what was dropped.
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
	cleaned, kept := c.name(command, flags)
	if cleaned == "" {
		c.unusable = true
		if c.debug {
			c.write("[coldread] track: no usable command path.\n")
		}
		return
	}
	c.command, c.flags, c.unusable = cleaned, kept, false
	c.watchSignals()
}

// name: the command and flags as kept; COLDREAD_DEBUG says what went.
func (c *Client) name(command string, flags []string) (string, []string) {
	cleaned, dropped := cleanCommandParts(command)
	if jsTrim(command) == "" {
		cleaned, dropped = c.tool, nil // the root command
	}
	kept, droppedFlags := cleanFlagsParts(flags)
	if c.debug && len(dropped)+len(droppedFlags) > 0 {
		var quoted []string
		for _, d := range append(dropped, droppedFlags...) {
			b, _ := json.Marshal(d)
			quoted = append(quoted, string(b))
		}
		c.write("[coldread] track: dropped " + strings.Join(quoted, ", ") + " (only command words and flag names are sent).\n")
	}
	return cleaned, kept
}

// ParseError records a command line the parser refused (an unknown
// command or flag, a missing or bad argument, a value that didn't
// validate): the command that was tried (sanitised; the root when nothing
// usable), sent when the run ends (Exit, Finish) with the code the CLI
// exits with, or 2 if it exits 0. No command given: the one already
// tracked, else the root.
func (c *Client) ParseError(command string, flags ...string) {
	if c == nil || c.s == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.done {
			return
		}
		if jsTrim(command) != "" || c.command == "" {
			cleaned, kept := c.name(command, flags)
			if cleaned == "" {
				cleaned = c.tool
			}
			c.command, c.flags = cleaned, kept
		} else if len(flags) > 0 {
			_, c.flags = c.name(c.command, flags)
		}
		c.unusable, c.parseFailed = false, true
	}()
}

// Recover, deferred at the top of main (`defer cr.Recover()`), records a
// run that panics with Go's own exit code for it, 2, then panics again, so
// the CLI ends exactly as it would have. crcobra and crurfave do it for you.
func (c *Client) Recover() {
	if r := recover(); r != nil {
		c.Finish(2)
		panic(r)
	}
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
		// Names from the flag set's own definitions: lowercased, so a
		// -baseURL reads --baseurl, never dropped.
		if len(f.Name) == 1 {
			names = append(names, "-"+f.Name)
		} else {
			names = append(names, "--"+strings.ToLower(f.Name))
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
		untracked, unusable := !c.done && c.command == "", c.unusable
		c.mu.Unlock()
		if untracked && c.off == "" && c.verifyMode {
			if unusable {
				c.write(VerifyPrefix + "nothing sent (no command).\n")
			} else {
				c.write(VerifyPrefix + "nothing sent (track() was never called).\n")
			}
		}
		return
	}
	c.done = true
	command, flags, parseFailed := c.command, c.flags, c.parseFailed
	// A kill signal waiting to be read: the CLI's own handler is ending the
	// run on it before our watcher got to it.
	if c.killedBy == 0 && c.sigCh != nil {
		select {
		case sig := <-c.sigCh:
			c.killedBy = signalNumber(sig)
		default:
			c.killedBy = int(c.sigSeen.Load()) // read by the watcher, not yet noted
		}
		// OwnSignals: the CLI's handler may have got the signal a moment
		// before ours (Go hands it to each channel in turn) and be exiting
		// already. A moment's wait makes the run read 128+n every time.
		if c.killedBy == 0 && c.ownSignals {
			t := time.NewTimer(ownSignalWait)
			select {
			case sig := <-c.sigCh:
				c.killedBy = signalNumber(sig)
			case <-t.C:
				c.killedBy = int(c.sigSeen.Load())
			}
			t.Stop()
		}
	}
	if c.killedBy != 0 {
		exit = 128 + c.killedBy // killed, whatever it then exited with
	} else if parseFailed && exit == 0 {
		exit = 2 // refused, though the CLI exits 0 (cobra's help for a parent)
	}
	c.mu.Unlock()
	c.unwatchSignals()
	if c.off != "" {
		return
	}
	if !parseFailed && isHelpRun(command, flags, c.tool) {
		if c.verifyMode {
			c.write(VerifyPrefix + "nothing sent (help or version).\n")
		} else if c.debug {
			c.write("[coldread] not sending (help or version).\n")
		}
		return
	}

	record := c.record(command, flags, exit)
	if c.debug {
		c.write("[coldread] " + string(record) + "\n")
	}
	records := [][]byte{record}
	if rules := c.rulesToSend(); rules != nil {
		if c.debug {
			c.write("[coldread] " + string(rules) + "\n")
		}
		records = append(records, rules)
	}
	if c.verifyMode {
		c.s.verifyAll(records, c.det.networkDisabled, func(m string) { c.write(VerifyPrefix + m + "\n") })
		return
	}
	if c.debug {
		return
	}
	deadline := c.now().Add(ExitWait)
	// No network, or a recent send timed out: to the spool, no waiting.
	// Backed off, the startup probe gets a moment first (probeAtExit): a
	// network that works again clears the mark, and this run sends as usual.
	if !c.det.networkDisabled && c.s.backedOff() {
		c.s.probeAtExit(probeWait)
	}
	wait := !c.det.networkDisabled && !c.s.backedOff()
	if wait {
		c.s.sendAll(records, ExitWait)
	} else {
		_, _ = c.s.save(records)
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
	// ID is made once, when the event happens: ingest stores an id once,
	// so a resend (a POST given up on that went through) counts once.
	ID string `json:"id,omitempty"`
	// Verify: sent under COLDREAD_VERIFY=1, an install check, kept out of
	// every number on the dashboard.
	Verify bool `json:"verify,omitempty"`
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
		OS: osName(), Arch: archName(), Runtime: runtimeTag(), ID: newEventID(),
	}
	r.Agent = toWireAgent(c.det.agent)
	if c.det.sessionID != "" {
		r.Session = hashSession(c.det.sessionID, c.key)
	}
	r.Verify = c.verifyMode
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
// run never sends (a suite runs the CLI hundreds of times), wherever the
// endpoint points; only COLDREAD_VERIFY=1 with an endpoint other than
// Coldread's production one does.
func disabledBy(env map[string]string, opts Options, endpoint string, testBinary bool) (reason string) {
	switch {
	case truthy(env, "DO_NOT_TRACK"):
		return "DO_NOT_TRACK"
	case truthy(env, "COLDREAD_DISABLED"):
		return "COLDREAD_DISABLED"
	case (testBinary || isTestRun(env)) && !(env["COLDREAD_VERIFY"] == "1" && endpoint != DefaultEndpoint):
		return "test run"
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
