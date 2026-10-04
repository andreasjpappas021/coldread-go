package coldread

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type mcpWire struct {
	Source string `json:"source"`
	TS     int64  `json:"ts"`
	Tool   struct {
		Name    string  `json:"name"`
		Version *string `json:"version"`
	} `json:"tool"`
	Command    string          `json:"command"`
	Exit       int             `json:"exit"`
	DurationMs int64           `json:"durationMs"`
	Agent      *wantAgent      `json:"agent"`
	Client     *wireClient     `json:"client"`
	Session    string          `json:"session"`
	OS         string          `json:"os"`
	Runtime    string          `json:"runtime"`
	Flags      json.RawMessage `json:"flags"`
}

func (in *ingest) mcpRecords(t *testing.T) []mcpWire {
	t.Helper()
	var out []mcpWire
	for _, p := range in.posts() {
		for _, raw := range p.body.Records {
			var w mcpWire
			if err := json.Unmarshal(raw, &w); err != nil {
				t.Fatal(err)
			}
			out = append(out, w)
		}
	}
	return out
}

func testMCP(t *testing.T, env map[string]string, opts MCPOptions) (*MCP, *lines, string) {
	t.Helper()
	out := &lines{}
	cache := t.TempDir()
	if env == nil {
		env = map[string]string{}
	}
	if opts.Key == "" {
		opts.Key = testKey
	}
	if opts.Tool == "" {
		opts.Tool = "acme-mcp"
	}
	if opts.Version == "" {
		opts.Version = "1.2.0"
	}
	m := newMCP(opts, mcpInternals{env: env, cacheDir: cache, tmpDir: t.TempDir(), write: out.write, testRun: bptr(false)})
	t.Cleanup(m.Close)
	return m, out, cache
}

func TestMCPToolCalls(t *testing.T) {
	in := newIngest(t)
	m, out, _ := testMCP(t, claudeEnv(), MCPOptions{Endpoint: in.endpoint()})
	if !m.Enabled() || m.Agent() == nil || m.Agent().Name != "claude-code" {
		t.Fatal("not enabled, or no agent")
	}
	start := time.UnixMilli(time.Now().Add(-time.Hour).UnixMilli()) // within the spool's week
	m.Record(ToolCall{Name: "search_docs", Start: start, Duration: 1234567 * time.Microsecond, Client: ClientInfo{Name: "claude-code", Version: "2.1.281"}, SessionID: "transport"})
	m.Record(ToolCall{Name: "deploy", Failed: true, Duration: time.Millisecond, Client: ClientInfo{Name: "codex-mcp-client", Version: "0.1.0"}, SessionID: "transport"})
	m.Record(ToolCall{Name: "lookup", Client: ClientInfo{Name: "\x00 Some\tClienté ", Version: "not a version!"}})
	m.Record(ToolCall{Name: "   "}) // no usable tool name: nothing
	if len(in.posts()) != 0 {
		t.Fatal("sent before the batch was due")
	}
	m.Flush(context.Background())
	recs := in.mcpRecords(t)
	if len(recs) != 3 || len(in.posts()) != 1 {
		t.Fatalf("%d records in %d posts", len(recs), len(in.posts()))
	}
	a, b, c := recs[0], recs[1], recs[2]
	if a.Source != "mcp" || a.Tool.Name != "acme-mcp" || *a.Tool.Version != "1.2.0" || a.Command != "search_docs" || a.Exit != 0 || a.DurationMs != 1235 || a.TS != start.UnixMilli() {
		t.Errorf("first %+v", a)
	}
	if a.Agent == nil || a.Agent.Name != "claude-code" || a.Agent.Evidence != "declared" || a.Client.Name != "claude-code" || *a.Client.Version != "2.1.281" {
		t.Errorf("first's agent %+v client %+v", a.Agent, a.Client)
	}
	// The environment's own session, not the transport's.
	if a.Session != hashSession(session, testKey) || a.OS == "" || !strings.HasPrefix(a.Runtime, "go/") || a.Flags != nil {
		t.Errorf("first %+v", a)
	}
	// Codex called, from a server started in a Claude Code terminal: the
	// environment doesn't speak for it.
	if b.Agent != nil || b.Exit != 1 || b.Session != hashSession("transport", testKey) || b.Client.Name != "codex-mcp-client" || b.DurationMs != 1 {
		t.Errorf("second %+v", b)
	}
	// Any other client disagrees with the environment too.
	if c.Client == nil || c.Client.Name != "SomeClient" || c.Client.Version != nil || c.Agent != nil || c.Session != "" {
		t.Errorf("third %+v", c.Client)
	}
	if len(out.all()) != 0 {
		t.Errorf("printed %q", out.all())
	}
}

func TestMCPBatchesOf20(t *testing.T) {
	in := newIngest(t)
	// With an agent in the environment, 20 events are over 8 KB: two POSTs.
	m, _, cache := testMCP(t, claudeEnv(), MCPOptions{Endpoint: in.endpoint(), FlushInterval: time.Hour})
	for i := 0; i < 20; i++ {
		m.Record(ToolCall{Name: "t", Client: ClientInfo{Name: "claude-code"}})
	}
	eventually(t, "a full batch goes at once", func() bool { return len(in.mcpRecords(t)) == 20 })
	if n := len(in.posts()); n != 2 || len(spooled(t, cache)) != 0 {
		t.Fatalf("%d posts, spool %v", n, spooled(t, cache))
	}
	m.Record(ToolCall{Name: "t"})
	time.Sleep(50 * time.Millisecond)
	if len(in.posts()) != 2 {
		t.Fatal("sent a part batch early")
	}
	m2, _, _ := testMCP(t, map[string]string{}, MCPOptions{Endpoint: in.endpoint(), FlushInterval: 30 * time.Millisecond})
	m2.Record(ToolCall{Name: "t"})
	eventually(t, "the interval sends", func() bool { return len(in.posts()) == 3 })
}

func TestMCPRemoteReadsNoEnvironment(t *testing.T) {
	in := newIngest(t)
	m, _, _ := testMCP(t, claudeEnv(), MCPOptions{Endpoint: in.endpoint(), Remote: true})
	m.Record(ToolCall{Name: "t", Client: ClientInfo{Name: "claude-code"}, SessionID: "mcp-session-1"})
	m.Flush(context.Background())
	r := in.mcpRecords(t)[0]
	if m.Agent() != nil || r.Agent != nil || r.Session != hashSession("mcp-session-1", testKey) || r.Client.Name != "claude-code" {
		t.Errorf("%+v", r)
	}
}

// A server on stdio and HTTP at once, started from the operator's Claude
// Code terminal: a call over HTTP never takes the environment's agent or
// session, whoever called; a stdio call still does.
func TestMCPCallsOverHTTPReadNoEnvironment(t *testing.T) {
	in := newIngest(t)
	m, _, _ := testMCP(t, claudeEnv(), MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "http", HTTP: true, SessionID: "mcp-session-1"})
	m.Record(ToolCall{Name: "stdio"})
	// Stateless HTTP from Codex: no clientInfo, its User-Agent names it.
	m.Record(ToolCall{Name: "codex", HTTP: true, UserAgent: "codex_cli_rs/0.142.5 (Mac OS 26.0.0; arm64)", AgentSession: "s1"})
	// A client with no version takes its User-Agent's, when it's the same agent.
	m.Record(ToolCall{Name: "codex2", HTTP: true, Client: ClientInfo{Name: CodexClientName}, UserAgent: "codex_cli_rs/0.142.5"})
	// A User-Agent the registry doesn't know names nothing.
	m.Record(ToolCall{Name: "script", HTTP: true, UserAgent: "python-httpx/0.27.0"})
	m.Flush(context.Background())
	recs := in.mcpRecords(t)
	by := map[string]mcpWire{}
	for _, r := range recs {
		by[r.Command] = r
	}
	if r := by["http"]; r.Agent != nil || r.Session != hashSession("mcp-session-1", testKey) {
		t.Errorf("http: %+v", r)
	}
	if r := by["stdio"]; r.Agent == nil || r.Agent.Name != "claude-code" {
		t.Errorf("stdio: %+v", r)
	}
	if r := by["codex"]; r.Agent != nil || r.Client == nil || r.Client.Name != "codex_cli_rs" || r.Client.Version == nil || *r.Client.Version != "0.142.5" || r.Session != hashSession("s1", testKey) {
		t.Errorf("codex: %+v", r)
	}
	if r := by["codex2"]; r.Client == nil || r.Client.Name != CodexClientName || r.Client.Version == nil || *r.Client.Version != "0.142.5" {
		t.Errorf("codex2: %+v", r)
	}
	if r := by["script"]; r.Client != nil || r.Agent != nil {
		t.Errorf("script: %+v", r)
	}
}

// What a killed run left on disk goes shortly after the next start, though
// that start makes no call.
func TestMCPSendsTheSpoolShortlyAfterStart(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	left := withKey(rec("left", time.Now(), ""), testKey)
	if err := toSpool(filepath.Join(cache, "spool.jsonl"), [][]byte{left}, time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = newMCP(MCPOptions{Key: testKey, Tool: "acme-mcp", Endpoint: in.endpoint()}, mcpInternals{env: map[string]string{}, cacheDir: cache, tmpDir: t.TempDir(), write: (&lines{}).write, testRun: bptr(false), startDrain: 10 * time.Millisecond})
	deadline := time.Now().Add(2 * time.Second)
	for len(in.commands()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := in.commands(); len(got) != 1 || got[0] != "left" {
		t.Fatalf("sent %v", got)
	}
}

func TestMCPSpoolsWhatDoesntGetThrough(t *testing.T) {
	in := newIngest(t)
	in.set(503, `{}`)
	m, _, cache := testMCP(t, nil, MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "first"})
	m.Flush(context.Background())
	if got := spooled(t, cache); !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("spool %v", got)
	}
	in.set(202, `{"accepted":2,"rejected":[]}`)
	m.Record(ToolCall{Name: "second"})
	m.Flush(context.Background())
	// The spool, oldest first: each event is in it as soon as it happens.
	if got := in.commands(); !reflect.DeepEqual(got, []string{"first", "first", "second"}) {
		t.Errorf("sent %v", got)
	}
	if got := spooled(t, cache); len(got) != 0 {
		t.Errorf("spool %v", got)
	}
	// Refused for good (a 4xx but 429): not kept.
	in.set(400, `{"error":"no"}`)
	m.Record(ToolCall{Name: "third"})
	m.Flush(context.Background())
	if got := spooled(t, cache); len(got) != 0 {
		t.Errorf("kept a refused event: %v", got)
	}
}

func TestMCPWithoutNetworkSpoolsOnly(t *testing.T) {
	in := newIngest(t)
	m, _, cache := testMCP(t, map[string]string{"CODEX_THREAD_ID": "x", "CODEX_SANDBOX_NETWORK_DISABLED": "1"}, MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "t"})
	m.Flush(context.Background())
	if len(in.posts()) != 0 || !reflect.DeepEqual(spooled(t, cache), []string{"t"}) {
		t.Errorf("posts %d spool %v", len(in.posts()), spooled(t, cache))
	}
}

func TestMCPDebugAndVerify(t *testing.T) {
	in := newIngest(t)
	m, out, _ := testMCP(t, map[string]string{"COLDREAD_DEBUG": "1"}, MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "t"})
	m.Flush(context.Background())
	if len(in.posts()) != 0 || len(out.all()) != 1 || !strings.HasPrefix(out.all()[0], `[coldread] {"source":"mcp"`) {
		t.Errorf("debug: %d posts, %q", len(in.posts()), out.all())
	}

	m, out, _ = testMCP(t, map[string]string{"COLDREAD_VERIFY": "1"}, MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "t"})
	m.Flush(context.Background())
	if !reflect.DeepEqual(out.all(), []string{VerifyAccepted}) || len(in.posts()) != 1 {
		t.Errorf("verify: %q", out.all())
	}
	// An install check says so, so it's kept out of every number.
	if raw := string(in.posts()[0].body.Records[0]); !strings.Contains(raw, `"verify":true`) {
		t.Errorf("verify record: %s", raw)
	}
	in.set(401, `{"error":"Unknown or revoked key."}`)
	m.Record(ToolCall{Name: "t"})
	m.Flush(context.Background())
	if got := out.all(); got[len(got)-1] != "[coldread] verify: rejected (401: Unknown or revoked key.)" {
		t.Errorf("verify: %q", got)
	}
}

func TestMCPOff(t *testing.T) {
	in := newIngest(t)
	for _, c := range []struct {
		env  map[string]string
		opts MCPOptions
		want string
	}{
		{map[string]string{"DO_NOT_TRACK": "1", "COLDREAD_VERIFY": "1"}, MCPOptions{}, "[coldread] verify: not sending (DO_NOT_TRACK)."},
		{map[string]string{"ACME_NO_TELEMETRY": "yes", "COLDREAD_DEBUG": "1"}, MCPOptions{OptOut: "ACME_NO_TELEMETRY"}, "[coldread] not sending (ACME_NO_TELEMETRY)."},
		{map[string]string{"COLDREAD_VERIFY": "1"}, MCPOptions{Key: secretKey}, "[coldread] verify: not sending (key)."},
		{map[string]string{"COLDREAD_VERIFY": "1"}, MCPOptions{OptOutFunc: func() bool { panic("x") }}, "[coldread] verify: not sending (optOut)."},
	} {
		c.opts.Endpoint = in.endpoint()
		m, out, _ := testMCP(t, c.env, c.opts)
		m.Record(ToolCall{Name: "t"})
		m.Close()
		if m.Enabled() || !reflect.DeepEqual(out.all(), []string{c.want}) {
			t.Errorf("got %q, want %q", out.all(), c.want)
		}
	}
	// A test binary never sends to Coldread's own endpoint.
	m := newMCP(MCPOptions{Key: testKey, Tool: "x"}, mcpInternals{env: map[string]string{}, cacheDir: t.TempDir(), tmpDir: t.TempDir(), write: (&lines{}).write, testRun: bptr(true)})
	if m.Enabled() {
		t.Error("enabled in a test binary")
	}
	if len(in.posts()) != 0 {
		t.Error("sent")
	}
	var none *MCP
	none.Record(ToolCall{Name: "t"})
	none.Close()
	if none.Enabled() || none.Agent() != nil {
		t.Error("nil MCP")
	}
}

func TestReadClient(t *testing.T) {
	v := "1.0"
	for in, want := range map[ClientInfo]*wireClient{
		{Name: "claude-code", Version: "1.0"}: {Name: "claude-code", Version: &v},
		{Name: "  "}:                          nil,
		{Name: "éé"}:                          nil,
		{Name: strings.Repeat("x", 70), Version: "1.0"}: {Name: strings.Repeat("x", 64), Version: &v},
		{Name: "a b", Version: "1 0"}:                   {Name: "a b"},
	} {
		if got := readClient(in); !reflect.DeepEqual(got, want) {
			t.Errorf("readClient(%+v) = %+v, want %+v", in, got, want)
		}
	}
}

// Each call is in the spool the moment it's recorded: a server killed right
// after loses nothing (the next send takes it).
func TestMCPEachCallIsSpooledAtOnce(t *testing.T) {
	in := newIngest(t)
	m, _, cache := testMCP(t, nil, MCPOptions{Endpoint: in.endpoint(), FlushInterval: time.Hour})
	m.Record(ToolCall{Name: "search_docs"})
	if got := spooled(t, cache); !reflect.DeepEqual(got, []string{"search_docs"}) {
		t.Fatalf("spool %v", got)
	}
	m.Flush(context.Background())
	if got := in.commands(); !reflect.DeepEqual(got, []string{"search_docs"}) || len(spooled(t, cache)) != 0 {
		t.Fatalf("sent %v, spool %v", got, spooled(t, cache))
	}
}

// Codex passes MCP servers no environment, but names its session on every
// call: that's the session, ahead of the transport's.
func TestMCPCodexSession(t *testing.T) {
	sid := "01a104fe-3eb7-7603-aa88-9667e3f20578"
	meta := map[string]any{CodexTurnMeta: map[string]any{"session_id": sid, "thread_id": sid, "turn_id": "t"}}
	if CodexSession(meta) != sid || CodexSession(map[string]any{CodexTurnMeta: map[string]any{"thread_id": "t1"}}) != "t1" || CodexSession(nil) != "" || CodexSession(map[string]any{CodexTurnMeta: "x"}) != "" {
		t.Fatal("CodexSession")
	}
	in := newIngest(t)
	m, _, _ := testMCP(t, nil, MCPOptions{Endpoint: in.endpoint()})
	m.Record(ToolCall{Name: "t", Client: ClientInfo{Name: "codex-mcp-client"}, AgentSession: CodexSession(meta), SessionID: "transport"})
	m.Flush(context.Background())
	if r := in.mcpRecords(t)[0]; r.Session != hashSession(sid, testKey) {
		t.Fatalf("%+v", r)
	}
}
