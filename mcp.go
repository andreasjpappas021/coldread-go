package coldread

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// The MCP half: the Go counterpart of @coldread/cli/mcp and coldread.mcp.
//
//	cr := coldread.NewMCP(coldread.MCPOptions{Key: "cr_pub_...", Tool: "acme-mcp", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
//	defer cr.Close()
//	server.AddReceivingMiddleware(crmcp.Middleware(cr)) // the official SDK
//
// Every tools/call becomes one event: the tool name, whether it failed, how
// long it took, and who called it. Never the arguments or the result.
// SDK-specific helpers feed it: coldread.apappas.dev/go/crmcp (the official
// github.com/modelcontextprotocol/go-sdk) and coldread.apappas.dev/go/crmcpgo
// (github.com/mark3labs/mcp-go); Record is for anything else.
//
// Who called: the client names itself, in initialize (clientInfo) or on
// every request (_meta["io.modelcontextprotocol/clientInfo"], the stateless
// 2026-07-28 spec). Self-reported either way, so Coldread stores it as
// reported, and a client name only counts as an agent when the registry
// knows it. For stdio servers, which run inside the agent's environment,
// the markers the CLI reads back it up; set Remote for an HTTP server.
//
// Events are batched (2s or 20 events) and sent from a goroutine, so a tool
// call never waits on Coldread; what doesn't get through waits in the same
// spool @coldread/cli uses and goes with the next batch. Nothing is written
// to stdout, which stdio servers use for the protocol. Opted out
// (DO_NOT_TRACK, COLDREAD_DISABLED, OptOut), nothing is recorded.
// COLDREAD_VERIFY=1 sends each call at once and prints what came back on
// stderr: "[coldread] verify: accepted", or why not. COLDREAD_DEBUG=1
// prints each event there and sends nothing.

// ClientInfoMeta is the _meta key a client names itself under on each
// request (the stateless MCP spec).
const ClientInfoMeta = "io.modelcontextprotocol/clientInfo"

const mcpBatch = 20

// MCPOptions configure an MCP. Key and Tool are required.
type MCPOptions struct {
	// Key is your site's public key (cr_pub_...). It ships in your code;
	// that's fine. A secret key (cr_sec_...) is never sent.
	Key string
	// Tool is your server's name, as you want to see it ("acme-mcp").
	Tool string
	// Version is your server's version.
	Version string
	// OptOut names an environment variable your users set to opt out
	// (set = opted out). DO_NOT_TRACK and COLDREAD_DISABLED always work.
	OptOut string
	// OptOutFunc returns true to opt out. A panic counts as opting out.
	OptOutFunc func() bool
	// Endpoint defaults to COLDREAD_ENDPOINT, then DefaultEndpoint.
	Endpoint string
	// Remote is for a server reached over HTTP: the agent's markers aren't
	// in this process's environment, so they aren't read. Leave it false
	// for stdio servers, which the agent starts.
	Remote bool
	// InferParent also looks at parent processes for agents that set no
	// marker. Off by default: it runs ps on macOS. Labeled "inferred".
	InferParent bool
	// FlushInterval is the longest an event waits before its batch is
	// sent. Default 2s.
	FlushInterval time.Duration
}

// ClientInfo is the MCP client that called, as it named itself.
type ClientInfo struct{ Name, Version string }

// ToolCall is one tools/call, for Record.
type ToolCall struct {
	// Name is the tool's name.
	Name string
	// Failed: the call returned an error or a result with isError.
	Failed bool
	// Start defaults to now minus Duration.
	Start    time.Time
	Duration time.Duration
	Client   ClientInfo
	// SessionID is the transport's session (Streamable HTTP), if any. It
	// is hashed before it leaves the process.
	SessionID string
}

// MCP reports an MCP server's tool calls. The zero value and nil do nothing.
type MCP struct {
	off           string
	key           string
	tool, version string
	debug, verify bool
	det           *detection
	offline       bool
	flushInterval time.Duration
	now           func() time.Time
	write         func(string)
	s             *sender

	mu     sync.Mutex
	queue  [][]byte
	timer  *time.Timer
	sendMu sync.Mutex // one send at a time: they share the spool
	// Sends in progress, and a channel closed when they next reach none.
	pending int
	idle    chan struct{}
}

// Seams for tests; none are needed in real use.
type mcpInternals struct {
	env      map[string]string
	cacheDir string
	now      func() time.Time
	write    func(string)
	testRun  *bool
}

// NewMCP starts reporting an MCP server's tool calls. One per process,
// shared by every server it creates (a server per session is fine).
func NewMCP(opts MCPOptions) *MCP { return newMCP(opts, mcpInternals{}) }

func newMCP(opts MCPOptions, in mcpInternals) (m *MCP) {
	defer func() {
		if recover() != nil {
			m = &MCP{off: "error"}
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
	testRun := isTestBinary()
	if in.testRun != nil {
		testRun = *in.testRun
	}
	m = &MCP{
		key: opts.Key, tool: cleanTool(opts.Tool), version: cleanVersion(opts.Version),
		debug: env["COLDREAD_DEBUG"] == "1", verify: env["COLDREAD_VERIFY"] == "1",
		flushInterval: opts.FlushInterval, now: now, write: write,
	}
	endpoint := resolveEndpoint(opts.Endpoint, env)
	m.off = disabledBy(env, Options{Key: opts.Key, OptOut: opts.OptOut, OptOutFunc: opts.OptOutFunc}, endpoint, testRun)
	if m.off != "" {
		if m.verify {
			write(VerifyPrefix + "not sending (" + m.off + ").\n")
		} else if m.debug {
			write("[coldread] not sending (" + m.off + ").\n")
		}
		return m
	}
	if m.flushInterval <= 0 {
		m.flushInterval = 2 * time.Second
	}
	// The environment doesn't change under a running server. stdio has no TTY.
	if !opts.Remote {
		d := detectAgent(env, false, nil)
		if d.agent == nil && opts.InferParent {
			func() {
				defer func() { _ = recover() }()
				d.agent = inferFromAncestors(ancestors())
			}()
		}
		m.det = &d
		m.offline = d.networkDisabled
	}
	cacheDir := in.cacheDir
	if cacheDir == "" {
		home, _ := os.UserHomeDir()
		cacheDir = cacheDirFor(m.tool, env, home)
	}
	m.s = newSender(endpoint, m.key, "coldread-go/"+Version, filepath.Join(cacheDir, "spool.jsonl"), now)
	m.s.backoff = ""
	return m
}

// Enabled is false when opted out (DO_NOT_TRACK, COLDREAD_DISABLED,
// OptOut), in a test binary, or without a valid public key.
func (m *MCP) Enabled() bool { return m != nil && m.off == "" && m.s != nil }

// Agent is the AI agent this server's environment names, or nil (always
// nil with Remote).
func (m *MCP) Agent() *Agent {
	if m == nil || m.det == nil || m.det.agent == nil {
		return nil
	}
	a := *m.det.agent
	a.Signals = append([]string(nil), a.Signals...)
	return &a
}

type wireClient struct {
	Name    string  `json:"name"`
	Version *string `json:"version"`
}

// mcpRecord is one record in a POST /api/ingest batch, in
// @coldread/cli/mcp's field order.
type mcpRecord struct {
	Source     string      `json:"source"`
	TS         int64       `json:"ts"`
	Tool       wireTool    `json:"tool"`
	Command    string      `json:"command"`
	Exit       int         `json:"exit"`
	DurationMs int64       `json:"durationMs"`
	Agent      *wireAgent  `json:"agent"`
	Client     *wireClient `json:"client"`
	OS         string      `json:"os"`
	Arch       string      `json:"arch"`
	Runtime    string      `json:"runtime"`
	Session    string      `json:"session,omitempty"`
}

var notPrintableRe = regexp.MustCompile(`[^\x20-\x7e]`)

// readClient: clientInfo as ingest takes it, a printable name and a
// version if it is one. nil without a name.
func readClient(c ClientInfo) *wireClient {
	name := strings.TrimSpace(notPrintableRe.ReplaceAllString(c.Name, ""))
	if len(name) > 64 {
		name = name[:64]
	}
	if name == "" {
		return nil
	}
	return &wireClient{Name: name, Version: strOrNil(cleanVersion(c.Version))}
}

var mcpClientSuffixRe = regexp.MustCompile(`-(mcp-client|mcp|client)$`)

// mcpClientAgent is our agent name for an MCP client, from its clientInfo
// name. Unknown clients keep their own name, normalized, minus a trailing
// "-mcp-client".
func mcpClientAgent(clientName string) string {
	key := strings.ToLower(jsTrim(clientName))
	if key == "" || jsLen(key) > 128 {
		return ""
	}
	if known, ok := registry.MCPClients[key]; ok {
		return known
	}
	n := normalizeAgentName(key)
	if n == "" {
		return ""
	}
	stripped := mcpClientSuffixRe.ReplaceAllString(n, "")
	if a, ok := registry.Aliases[stripped]; ok {
		return a
	}
	if stripped != "" {
		return stripped
	}
	return n
}

// Record reports one tool call. It returns at once and never panics. The
// helpers call it for you.
func (m *MCP) Record(c ToolCall) {
	if !m.Enabled() {
		return
	}
	defer func() { _ = recover() }() // never break the host
	command := cleanCommand(c.Name)
	if command == "" {
		return
	}
	now := m.now()
	start := c.Start
	if start.IsZero() {
		start = now.Add(-c.Duration)
	}
	d := int64(math.Round(float64(c.Duration) / float64(time.Millisecond)))
	if d < 0 {
		d = 0
	} else if d > 86_400_000 {
		d = 86_400_000
	}
	client := readClient(c.Client)
	// The environment only speaks for the client when they agree: Codex
	// started from a Claude Code terminal inherits Claude Code's variables,
	// and its calls mustn't carry Claude Code's host or session.
	clientAgent := ""
	if client != nil {
		clientAgent = mcpClientAgent(client.Name)
	}
	var envAgent *Agent
	if m.det != nil && m.det.agent != nil && (clientAgent == "" || clientAgent == m.det.agent.Name) {
		envAgent = m.det.agent
	}
	exit := 0
	if c.Failed {
		exit = 1
	}
	r := mcpRecord{
		Source: "mcp", TS: start.UnixMilli(),
		Tool:    wireTool{Name: m.tool, Version: strOrNil(m.version)},
		Command: command, Exit: exit, DurationMs: d,
		Agent: toWireAgent(envAgent), Client: client,
		OS: osName(), Arch: archName(), Runtime: runtimeTag(),
	}
	// The agent's own session when the environment has it, else the
	// transport's: hashed either way.
	sid := ""
	if envAgent != nil {
		sid = m.det.sessionID
	}
	if sid == "" {
		sid = c.SessionID
	}
	if sid != "" {
		r.Session = hashSession(sid, m.key)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(r) != nil {
		return
	}
	record := bytes.TrimRight(b.Bytes(), "\n")
	if m.debug {
		m.write("[coldread] " + string(record) + "\n")
	}
	// COLDREAD_VERIFY: each call is sent at once, and what came back is
	// printed on stderr, so the call itself never waits.
	if m.verify {
		m.begin()
		go func() {
			defer m.end()
			defer func() { _ = recover() }()
			m.sendMu.Lock()
			defer m.sendMu.Unlock()
			m.s.verify(record, m.offline, func(s string) { m.write(VerifyPrefix + s + "\n") })
		}()
		return
	}
	if m.debug {
		return
	}
	m.mu.Lock()
	m.queue = append(m.queue, record)
	full := len(m.queue) >= mcpBatch
	if !full && m.timer == nil {
		m.timer = time.AfterFunc(m.flushInterval, m.flushQueue)
	}
	m.mu.Unlock()
	if full {
		m.flushQueue()
	}
}

// flushQueue hands what's queued to a send, in the background.
func (m *MCP) flushQueue() {
	m.mu.Lock()
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
	records := m.queue
	m.queue = nil
	if len(records) > 0 {
		m.pending++
	}
	m.mu.Unlock()
	if len(records) == 0 {
		return
	}
	go func() {
		defer m.end()
		defer func() { _ = recover() }()
		m.send(records)
	}()
}

func (m *MCP) begin() {
	m.mu.Lock()
	m.pending++
	m.mu.Unlock()
}

func (m *MCP) end() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending--; m.pending == 0 && m.idle != nil {
		close(m.idle)
		m.idle = nil
	}
}

// send is @coldread/cli's flusher: these events, then spooled ones oldest
// first, in POSTs of at most 8 KB. Sent, or refused for good (4xx but 429):
// done. Otherwise (network, timeout, 429, 5xx) what's left goes back to the
// spool. A server outlives a CLI run, so unlike the flusher it sends what
// doesn't fit in further POSTs (a few at most: the spool holds 100).
func (m *MCP) send(records [][]byte) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	now := m.now()
	if m.offline {
		toSpool(m.s.spool, records, now)
		return
	}
	claimed := claimSpool(m.s.spool, now)
	spooled := readClaimed(claimed, now)
	removeAll(claimed)
	rest := append(records, spooled...)
	for i := 0; i < 8 && len(rest) > 0; i++ {
		var batch [][]byte
		batch, rest = fit(rest)
		if len(batch) == 0 {
			break
		}
		if r, err := m.s.post(context.Background(), batch); err != nil || r.retry() {
			rest = append(batch, rest...)
			break
		}
	}
	toSpool(m.s.spool, rest, m.now())
}

// Flush sends what's queued now and waits for every send in progress, or
// for ctx to be done.
func (m *MCP) Flush(ctx context.Context) {
	if !m.Enabled() {
		return
	}
	m.flushQueue()
	m.mu.Lock()
	if m.pending == 0 {
		m.mu.Unlock()
		return
	}
	if m.idle == nil {
		m.idle = make(chan struct{})
	}
	idle := m.idle
	m.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
	}
}

// Close is Flush, waiting 2 seconds at most. Call it when the server stops
// (defer it in main): stdio servers stop when the client closes stdin.
func (m *MCP) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	m.Flush(ctx)
}
