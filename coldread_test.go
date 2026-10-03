package coldread

import (
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "cr_pub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const session = "592a6d91-34ac-4e97-a7fd-09bdc4dee34e"

func claudeEnv() map[string]string {
	return map[string]string{
		"CLAUDECODE": "1", "CLAUDE_CODE_CHILD_SESSION": "1", "CLAUDE_CODE_ENTRYPOINT": "cli",
		"CLAUDE_CODE_SESSION_ID": session, "AI_AGENT": "claude-code_2-1-281_agent", "HOME": "/home/x",
	}
}

func with(env map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	for k, v := range env {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

// ingest answers like /api/ingest and keeps what it got.
type ingest struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	body   string
	delay  time.Duration
	got    []got
}

type got struct {
	auth, ua string
	body     struct {
		V       int               `json:"v"`
		Records []json.RawMessage `json:"records"`
	}
}

func newIngest(t *testing.T) *ingest {
	in := &ingest{status: 202, body: `{"accepted":1,"rejected":[]}`}
	in.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		g := got{auth: r.Header.Get("Authorization"), ua: r.Header.Get("User-Agent")}
		_ = json.Unmarshal(raw, &g.body)
		in.mu.Lock()
		in.got = append(in.got, g)
		status, body, delay := in.status, in.body, in.delay
		in.mu.Unlock()
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(in.Close)
	return in
}

func (in *ingest) set(status int, body string) {
	in.mu.Lock()
	in.status, in.body = status, body
	in.mu.Unlock()
}

func (in *ingest) posts() []got {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]got(nil), in.got...)
}

func (in *ingest) commands() []string {
	var out []string
	for _, g := range in.posts() {
		for _, r := range g.body.Records {
			var x struct{ Command string }
			_ = json.Unmarshal(r, &x)
			out = append(out, x.Command)
		}
	}
	return out
}

func (in *ingest) endpoint() string { return in.URL + "/api/ingest" }

type run struct {
	c     *Client
	out   []string
	mu    sync.Mutex
	clock *atomic.Int64 // ms
	cache string
	exits []int
}

func (r *run) tick(d time.Duration) { r.clock.Add(d.Milliseconds()) }
func (r *run) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.out...)
}

func bptr(b bool) *bool { return &b }

type setup struct {
	env       map[string]string
	opts      Options
	isTTY     bool
	stderrTTY bool
	cache     string
	testRun   bool
	clock     bool // a fake clock (for durations); the send uses real time regardless
}

func start(t *testing.T, s setup) *run {
	t.Helper()
	if s.cache == "" {
		s.cache = t.TempDir()
	}
	if s.env == nil {
		s.env = claudeEnv()
	}
	if s.opts.Key == "" {
		s.opts.Key = testKey
	}
	if s.opts.Tool == "" {
		s.opts.Tool = "acme"
	}
	if s.opts.Version == "" {
		s.opts.Version = "1.2.0"
	}
	r := &run{cache: s.cache}
	now := time.Now
	if s.clock {
		r.clock = &atomic.Int64{}
		r.clock.Store(1_000_000_000_000)
		now = func() time.Time { return time.UnixMilli(r.clock.Load()) }
	}
	r.c = newClient(s.opts, internals{
		env: s.env, isTTY: bptr(s.isTTY), stderrTTY: bptr(s.stderrTTY), cacheDir: s.cache, now: now,
		write:   func(l string) { r.mu.Lock(); r.out = append(r.out, l); r.mu.Unlock() },
		exit:    func(code int) { r.exits = append(r.exits, code) },
		testRun: bptr(s.testRun),
	})
	return r
}

func spooled(t *testing.T, cache string) []string {
	t.Helper()
	var out []string
	for _, r := range readSpool(filepath.Join(cache, "spool.jsonl"), time.Now()) {
		var x struct{ Command string }
		_ = json.Unmarshal(r, &x)
		out = append(out, x.Command)
	}
	return out
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("never: %s", what)
}

func TestClaudeCodeRun(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}, clock: true})
	if !r.c.Enabled() {
		t.Fatal("not enabled")
	}
	a := r.c.Agent()
	if a == nil || a.Name != "claude-code" || a.Version != "2.1.281" || a.Host != "terminal" || a.Confidence != "high" {
		t.Fatalf("agent %+v", a)
	}
	r.c.Track("deploy preview", "--prod", "--token=hunter2", "-y")
	if len(in.posts()) != 0 {
		t.Fatal("sent before the command finished")
	}
	r.tick(812 * time.Millisecond)
	r.c.Finish(0)
	posts := in.posts()
	if len(posts) != 1 || len(posts[0].body.Records) != 1 {
		t.Fatalf("posts %+v", posts)
	}
	if posts[0].auth != "Bearer "+testKey || posts[0].ua != "coldread-go/"+Version || posts[0].body.V != 1 {
		t.Fatalf("headers %+v", posts[0])
	}
	want := `{"source":"cli","ts":1000000000000,"tool":{"name":"acme","version":"1.2.0"},"command":"deploy preview","flags":["--prod","--token","-y"],"exit":0,"durationMs":812,` +
		`"agent":{"name":"claude-code","raw":null,"version":"2.1.281","host":"terminal","evidence":"declared","confidence":"high","signals":["CLAUDE_CODE_CHILD_SESSION","CLAUDECODE","CLAUDE_CODE_SESSION_ID","CLAUDE_CODE_ENTRYPOINT","AI_AGENT"]},` +
		`"ci":false,"interactive":false,"session":"` + hashSession(session, testKey) + `","os":"` + osName() + `","arch":"` + archName() + `","runtime":"` + runtimeTag() + `"}`
	if got := string(posts[0].body.Records[0]); got != want {
		t.Fatalf("record\n got %s\nwant %s", got, want)
	}
	if spooled(t, r.cache) != nil {
		t.Fatal("left a spool")
	}
}

func TestNeverSendsValues(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{env: with(claudeEnv(), "SECRET_TOKEN", "hunter2"), opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("login", "--password=hunter2")
	r.c.Finish(0)
	raw, _ := json.Marshal(in.posts()[0].body.Records)
	for _, leak := range []string{"hunter2", session, "/home/x", os.Args[0]} {
		if strings.Contains(string(raw), leak) {
			t.Errorf("sent %q", leak)
		}
	}
}

func TestFirstFinishCountsLastTrackWins(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy")
	r.c.Track("deploy preview", "--prod")
	r.c.Finish(1)
	r.c.Finish(0)
	r.c.Track("b")
	r.c.Finish(0)
	if got := in.commands(); len(got) != 1 || got[0] != "deploy preview" {
		t.Fatalf("sent %v", got)
	}
	var rec struct {
		Exit  int
		Flags []string
	}
	_ = json.Unmarshal(in.posts()[0].body.Records[0], &rec)
	if rec.Exit != 1 || len(rec.Flags) != 1 || rec.Flags[0] != "--prod" {
		t.Fatalf("record %+v", rec)
	}
}

func TestNothingTrackedNothingSent(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Finish(0)
	r2 := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r2.c.Track("   ")
	r2.c.Finish(0)
	if len(in.posts()) != 0 {
		t.Fatal("sent")
	}
}

func TestPersonAtATerminal(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{env: map[string]string{"HOME": "/home/x"}, isTTY: true, opts: Options{Endpoint: in.endpoint()}})
	if r.c.Agent() != nil {
		t.Fatal("agent")
	}
	r.c.Track("init")
	r.c.Finish(0)
	var rec map[string]any
	_ = json.Unmarshal(in.posts()[0].body.Records[0], &rec)
	if rec["agent"] != nil || rec["interactive"] != true || rec["ci"] != false {
		t.Fatalf("record %v", rec)
	}
	if _, ok := rec["session"]; ok {
		t.Fatal("session")
	}
}

func TestCodexOfflineSpoolsOnly(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{env: map[string]string{"CODEX_THREAD_ID": "thr_1", "CODEX_SANDBOX_NETWORK_DISABLED": "1"}, opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("test")
	r.c.Finish(0)
	time.Sleep(50 * time.Millisecond)
	if len(in.posts()) != 0 {
		t.Fatal("sent from a sandbox without network")
	}
	if got := spooled(t, r.cache); len(got) != 1 || got[0] != "test" {
		t.Fatalf("spool %v", got)
	}
	// The next run with network sends it, in the background.
	r2 := start(t, setup{cache: r.cache, opts: Options{Endpoint: in.endpoint()}})
	<-r2.c.drained
	if got := in.commands(); len(got) != 1 || got[0] != "test" {
		t.Fatalf("sent %v", got)
	}
}

func TestEndpointOptionAndEnv(t *testing.T) {
	if got := resolveEndpoint("", map[string]string{"COLDREAD_ENDPOINT": "http://localhost:3180/api/ingest"}); got != "http://localhost:3180/api/ingest" {
		t.Fatal(got)
	}
	if got := resolveEndpoint("https://example.com/i", map[string]string{"COLDREAD_ENDPOINT": "x"}); got != "https://example.com/i" {
		t.Fatal(got)
	}
	if got := resolveEndpoint("", nil); got != DefaultEndpoint {
		t.Fatal(got)
	}
}

func TestOptingOut(t *testing.T) {
	in := newIngest(t)
	for name, env := range map[string]map[string]string{
		"DO_NOT_TRACK=1":          with(claudeEnv(), "DO_NOT_TRACK", "1"),
		"DO_NOT_TRACK=true":       with(claudeEnv(), "DO_NOT_TRACK", "true"),
		"COLDREAD_DISABLED=1":     with(claudeEnv(), "COLDREAD_DISABLED", "1"),
		"the tool's own variable": with(claudeEnv(), "ACME_TELEMETRY_OFF", "1"),
	} {
		r := start(t, setup{env: env, opts: Options{Endpoint: in.endpoint(), OptOut: "ACME_TELEMETRY_OFF"}})
		if r.c.Enabled() {
			t.Errorf("%s: enabled", name)
		}
		if a := r.c.Agent(); a == nil || a.Name != "claude-code" {
			t.Errorf("%s: agent still yours to use", name)
		}
		r.c.Track("x")
		r.c.Finish(0)
		if h := r.c.Headers(""); len(h) != 1 || h.Get("User-Agent") != "acme/1.2.0" {
			t.Errorf("%s: headers %v", name, h)
		}
	}
	if len(in.posts()) != 0 {
		t.Fatal("sent while opted out")
	}
	if !start(t, setup{env: with(claudeEnv(), "DO_NOT_TRACK", "0"), opts: Options{Endpoint: in.endpoint()}}).c.Enabled() {
		t.Error("DO_NOT_TRACK=0 opted out")
	}
	if start(t, setup{opts: Options{Endpoint: in.endpoint(), OptOutFunc: func() bool { return true }}}).c.Enabled() {
		t.Error("OptOutFunc true")
	}
	if start(t, setup{opts: Options{Endpoint: in.endpoint(), OptOutFunc: func() bool { panic("x") }}}).c.Enabled() {
		t.Error("OptOutFunc panic")
	}
	if !start(t, setup{opts: Options{Endpoint: in.endpoint(), OptOutFunc: func() bool { return false }}}).c.Enabled() {
		t.Error("OptOutFunc false")
	}
}

func TestOnlyAPublicKey(t *testing.T) {
	in := newIngest(t)
	for _, key := range []string{"cr_sec_" + strings.Repeat("A", 43), "nope", "cr_pub_" + strings.Repeat("A", 32)} {
		r := start(t, setup{opts: Options{Endpoint: in.endpoint(), Key: key}})
		r.c.Track("x")
		r.c.Finish(0)
		if r.c.Enabled() {
			t.Errorf("%s: enabled", key)
		}
	}
	if len(in.posts()) != 0 {
		t.Fatal("sent with a key that isn't public")
	}
}

func TestTestRunsNeverSendToProduction(t *testing.T) {
	for _, env := range []map[string]string{with(claudeEnv(), "VITEST", "true"), with(claudeEnv(), "JEST_WORKER_ID", "1"), with(claudeEnv(), "NODE_TEST_CONTEXT", "child-v8"), with(claudeEnv(), "NODE_ENV", "test")} {
		if start(t, setup{env: env}).c.Enabled() {
			t.Errorf("%v: enabled", env)
		}
	}
	if start(t, setup{testRun: true}).c.Enabled() {
		t.Error("a go test binary is enabled")
	}
	if !start(t, setup{testRun: true, env: with(claudeEnv(), "COLDREAD_ENDPOINT", "http://127.0.0.1:9/i")}).c.Enabled() {
		t.Error("another endpoint is muted")
	}
	// This very test binary, on the real environment.
	os.Unsetenv("COLDREAD_ENDPOINT")
	c := New(Options{Key: testKey, Tool: "acme", Version: "1", NoNotice: true})
	if c.Enabled() || !isTestBinary() {
		t.Fatal("this test binary would send")
	}
}

func TestDebugPrintsAndSendsNothing(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{env: with(claudeEnv(), "COLDREAD_DEBUG", "1"), opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy")
	r.c.Finish(0)
	out := r.lines()
	if len(in.posts()) != 0 || len(out) != 1 || !strings.HasPrefix(out[0], `[coldread] {"source":"cli","ts":`) || !strings.Contains(out[0], `"command":"deploy"`) {
		t.Fatalf("out %q posts %d", out, len(in.posts()))
	}
	r2 := start(t, setup{env: with(claudeEnv(), "COLDREAD_DEBUG", "1", "DO_NOT_TRACK", "1")})
	if got := r2.lines(); len(got) != 1 || got[0] != "[coldread] not sending (DO_NOT_TRACK).\n" {
		t.Fatalf("%q", got)
	}
	r3 := start(t, setup{env: with(claudeEnv(), "COLDREAD_DEBUG", "1"), opts: Options{Endpoint: in.endpoint()}})
	r3.c.Track("!!!")
	if got := r3.lines(); len(got) != 1 || got[0] != "[coldread] track: no usable command path.\n" {
		t.Fatalf("%q", got)
	}
}

func verifyLine(t *testing.T, in *ingest, env map[string]string, opts Options) []string {
	t.Helper()
	if opts.Endpoint == "" {
		opts.Endpoint = in.endpoint()
	}
	r := start(t, setup{env: with(env, "COLDREAD_VERIFY", "1"), opts: opts})
	r.c.Track("deploy")
	r.c.Finish(0)
	return r.lines()
}

func TestVerifyLines(t *testing.T) {
	in := newIngest(t)
	check := func(want string, got []string) {
		t.Helper()
		if len(got) != 1 || got[0] != want+"\n" {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	check(VerifyAccepted, verifyLine(t, in, claudeEnv(), Options{}))
	in.set(202, `{"accepted":0,"rejected":[{"i":0,"error":"tool.name: invalid"}]}`)
	check("[coldread] verify: rejected (202: tool.name: invalid)", verifyLine(t, in, claudeEnv(), Options{}))
	in.set(401, `{"error":"Unknown or revoked key."}`)
	check("[coldread] verify: rejected (401: Unknown or revoked key.)", verifyLine(t, in, claudeEnv(), Options{}))
	in.set(400, `not json`)
	check("[coldread] verify: rejected (400)", verifyLine(t, in, claudeEnv(), Options{}))
	in.set(503, `{}`)
	check("[coldread] verify: not sent (503); saved for the next run.", verifyLine(t, in, claudeEnv(), Options{}))

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := "http://" + l.Addr().String() + "/api/ingest"
	l.Close()
	check("[coldread] verify: not sent (ECONNREFUSED); saved for the next run.", verifyLine(t, in, claudeEnv(), Options{Endpoint: closed}))

	before := len(in.posts())
	check("[coldread] verify: no network here; saved for the next run with network.", verifyLine(t, in, map[string]string{"CODEX_THREAD_ID": "x", "CODEX_SANDBOX_NETWORK_DISABLED": "1"}, Options{}))
	if len(in.posts()) != before {
		t.Error("sent without network")
	}
	check("[coldread] verify: not sending (DO_NOT_TRACK).", start(t, setup{env: with(claudeEnv(), "COLDREAD_VERIFY", "1", "DO_NOT_TRACK", "1")}).lines())
	check("[coldread] verify: not sending (key).", start(t, setup{env: map[string]string{"COLDREAD_VERIFY": "1"}, opts: Options{Key: "cr_sec_nope"}}).lines())

	r := start(t, setup{env: with(claudeEnv(), "COLDREAD_VERIFY", "1"), opts: Options{Endpoint: in.endpoint()}})
	r.c.Finish(0)
	check("[coldread] verify: nothing sent (track() was never called).", r.lines())

	// Off by default: nothing printed.
	in.set(401, `{"error":"Unknown or revoked key."}`)
	r = start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy")
	r.c.Finish(0)
	if len(r.lines()) != 0 {
		t.Errorf("printed %q", r.lines())
	}
}

func TestVerifySendsTheSpoolToo(t *testing.T) {
	in := newIngest(t)
	in.set(503, `{}`)
	cache := t.TempDir()
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("one")
	r.c.Finish(0)
	in.set(202, `{"accepted":2,"rejected":[]}`)
	r2 := start(t, setup{cache: cache, env: with(claudeEnv(), "COLDREAD_VERIFY", "1"), opts: Options{Endpoint: in.endpoint()}})
	r2.c.Track("two")
	r2.c.Finish(0)
	got := in.posts()
	last := got[len(got)-1]
	if len(last.body.Records) != 2 || r2.lines()[0] != VerifyAccepted+"\n" {
		t.Fatalf("%d records, %q", len(last.body.Records), r2.lines())
	}
	if spooled(t, cache) != nil {
		t.Fatal("spool left")
	}
}

func TestNotice(t *testing.T) {
	cache := t.TempDir()
	env := map[string]string{}
	a := start(t, setup{env: env, cache: cache, stderrTTY: true, isTTY: true})
	b := start(t, setup{env: env, cache: cache, stderrTTY: true, isTTY: true})
	if got := append(a.lines(), b.lines()...); len(got) != 1 || got[0] != defaultNotice("acme")+"\n" {
		t.Fatalf("%q", got)
	}
	if _, err := os.Stat(filepath.Join(cache, "notice")); err != nil {
		t.Fatal("no marker")
	}
	c := start(t, setup{stderrTTY: false})
	if len(c.lines()) != 0 {
		t.Error("shown to an agent")
	}
	if _, err := os.Stat(filepath.Join(c.cache, "notice")); err == nil {
		t.Error("marked shown")
	}
	if got := start(t, setup{env: env, stderrTTY: true, opts: Options{Notice: "Acme collects usage."}}).lines(); len(got) != 1 || got[0] != "Acme collects usage.\n" {
		t.Errorf("%q", got)
	}
	if got := start(t, setup{env: env, stderrTTY: true, opts: Options{NoNotice: true}}).lines(); len(got) != 0 {
		t.Errorf("%q", got)
	}
	if got := start(t, setup{env: map[string]string{"DO_NOT_TRACK": "1"}, stderrTTY: true}).lines(); len(got) != 0 {
		t.Errorf("%q", got)
	}
}

func TestHeaders(t *testing.T) {
	r := start(t, setup{testRun: false, opts: Options{Endpoint: "http://127.0.0.1:9/i"}})
	h := r.c.Headers("")
	if h.Get("User-Agent") != "acme/1.2.0 AIAgent/claude-code" || h.Get("AI-Agent") != `name="claude-code", version="2.1.281", host="terminal", evidence="declared"` {
		t.Fatalf("%v", h)
	}
	if got := r.c.Headers("acme-cli/1.2.0 (darwin)").Get("User-Agent"); got != "acme-cli/1.2.0 (darwin) AIAgent/claude-code" {
		t.Fatal(got)
	}
	p := start(t, setup{env: map[string]string{}, opts: Options{Endpoint: "http://127.0.0.1:9/i"}})
	if h := p.c.Headers(""); len(h) != 1 || h.Get("User-Agent") != "acme/1.2.0" {
		t.Fatalf("%v", h)
	}
}

func TestNeverBreaksTheHost(t *testing.T) {
	var c *Client
	c.Track("x")
	c.TrackFlags("x", nil)
	c.Finish(0)
	if c.Enabled() || c.Agent() != nil || c.Headers("").Get("User-Agent") != "cli" {
		t.Fatal("nil client")
	}
	var zero Client
	zero.Track("x")
	zero.Finish(0)
	if zero.Enabled() {
		t.Fatal("zero client enabled")
	}
	n := New(Options{})
	n.Track("x")
	n.Finish(0)
	if n.Enabled() {
		t.Fatal("no options enabled")
	}
}

func TestExitFinishesFirst(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("build")
	r.c.Exit(3)
	var rec struct{ Exit int }
	_ = json.Unmarshal(in.posts()[0].body.Records[0], &rec)
	if len(r.exits) != 1 || r.exits[0] != 3 || rec.Exit != 3 {
		t.Fatalf("exits %v, record %+v", r.exits, rec)
	}
}

func TestTrackFlags(t *testing.T) {
	in := newIngest(t)
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.Bool("prod", false, "")
	fs.Bool("y", false, "")
	fs.String("token", "", "")
	fs.Bool("unused", false, "")
	_ = fs.Parse([]string{"-prod", "--token", "hunter2", "-y", "./site"})
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.TrackFlags("deploy", fs)
	r.c.Finish(0)
	raw := string(in.posts()[0].body.Records[0])
	if !strings.Contains(raw, `"flags":["--prod","--token","-y"]`) || strings.Contains(raw, "hunter2") || strings.Contains(raw, "site") {
		t.Fatal(raw)
	}
}

func TestCleaning(t *testing.T) {
	for in, want := range map[string]string{
		"  deploy   preview ":       "deploy preview",
		`run "rm -rf"`:              "run",
		strings.Repeat("a", 200):    "",
		"":                          "",
		strings.Repeat("word ", 40): strings.TrimSpace(strings.Repeat("word ", 25)),
	} {
		if got := cleanCommand(in); got != want {
			t.Errorf("cleanCommand(%q) = %q", in, got)
		}
	}
	got := cleanFlags([]string{"--prod", "--token=abc", "-y", "value", "--prod", "", "--"})
	if strings.Join(got, " ") != "--prod --token -y" {
		t.Errorf("%v", got)
	}
	many := []string{}
	for i := 0; i < 50; i++ {
		many = append(many, "--f"+strings.Repeat("x", i%5)+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	if len(cleanFlags(many)) != 32 {
		t.Error("more than 32 flags")
	}
	h := hashSession(session, testKey)
	if len(h) != 16 || h != hashSession(session, testKey) || h == hashSession(session, "cr_pub_"+strings.Repeat("b", 32)) {
		t.Error(h)
	}
	// The same hash @coldread/cli computes (node: sha256("coldread:<key>:<id>")[:16]).
	if h != "badb5f1bc953ed70" {
		t.Errorf("hash %s isn't @coldread/cli's", h)
	}
	if got := cacheDirFor("Acme CLI", map[string]string{"XDG_CACHE_HOME": "/c"}, "/h"); got != filepath.Join("/c", "coldread", "acme-cli") {
		t.Error(got)
	}
	if got := cacheDirFor("acme", map[string]string{}, "/h"); got != filepath.Join("/h", ".cache", "coldread", "acme") {
		t.Error(got)
	}
	if cleanTool("acme") != "acme" || cleanTool("-my tool!") != "my-tool-" || cleanTool("!!") != "tool" {
		t.Error(cleanTool("-my tool!"))
	}
	if cleanVersion("1.2.0") != "1.2.0" || cleanVersion("latest; rm") != "" {
		t.Error("version")
	}
	if !strings.HasPrefix(runtimeTag(), "go/1.") {
		t.Error(runtimeTag())
	}
}
