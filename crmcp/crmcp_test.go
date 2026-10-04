package crmcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const key = "cr_pub_0123456789abcdef0123456789abcdef"

type record struct {
	Source     string `json:"source"`
	Command    string `json:"command"`
	Exit       int    `json:"exit"`
	DurationMs *int64 `json:"durationMs"`
	Client     *struct {
		Name    string  `json:"name"`
		Version *string `json:"version"`
	} `json:"client"`
	Agent     json.RawMessage `json:"agent"`
	Session   string          `json:"session"`
	Arguments json.RawMessage `json:"arguments"`
	Result    json.RawMessage `json:"result"`
}

type ingest struct {
	*httptest.Server
	mu   sync.Mutex
	recs []record
	raw  []string
}

func newIngest(t *testing.T) *ingest {
	in := &ingest{}
	in.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var b struct{ Records []record }
		_ = json.Unmarshal(body, &b)
		in.mu.Lock()
		in.recs = append(in.recs, b.Records...)
		in.raw = append(in.raw, string(body))
		in.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"accepted":1,"rejected":[]}`)
	}))
	t.Cleanup(in.Close)
	return in
}

func (in *ingest) records() []record {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]record(nil), in.recs...)
}

type args struct {
	Query string `json:"query"`
}

// server is an MCP server like a customer's: typed tools, a raw one, and one
// added after Coldread.
func server(cr *coldread.MCP, twice bool) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "acme", Version: "1.2.0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "search_docs"}, func(ctx context.Context, req *mcp.CallToolRequest, in args) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "secret result for " + in.Query}}}, nil, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "deploy"}, func(ctx context.Context, req *mcp.CallToolRequest, in args) (*mcp.CallToolResult, any, error) {
		return nil, nil, errors.New("no such project")
	})
	s.AddReceivingMiddleware(crmcp.Middleware(cr))
	if twice {
		s.AddReceivingMiddleware(crmcp.Middleware(cr))
	}
	s.AddTool(&mcp.Tool{Name: "raw", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("protocol-level failure")
	})
	return s
}

func newMCP(t *testing.T, in *ingest) *coldread.MCP {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("COLDREAD_VERIFY", "1") // a test binary sends only when verifying, never to production
	// Remote: this test's own environment (maybe a coding agent's) stays out of it.
	cr := coldread.NewMCP(coldread.MCPOptions{Key: key, Tool: "acme-mcp", Version: "1.2.0", Endpoint: in.URL + "/api/ingest", Remote: true})
	if !cr.Enabled() {
		t.Fatal("not enabled")
	}
	return cr
}

func TestToolCallsOverStdioLikeTransport(t *testing.T) {
	in := newIngest(t)
	cr := newMCP(t, in)
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server(cr, true).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "claude-code", Version: "2.1.281"}, nil)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatal(err)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "search_docs", Arguments: map[string]any{"query": "hunter2"}})
	if err != nil || res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, "hunter2") {
		t.Fatalf("search_docs: %v %+v", err, res)
	}
	if res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "deploy", Arguments: map[string]any{"query": "q"}}); err != nil || !res.IsError {
		t.Fatalf("deploy: %v %+v", err, res)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "raw", Arguments: map[string]any{}}); err == nil {
		t.Fatal("raw: no error")
	}
	// The stateless spec's per-request clientInfo wins over initialize's.
	meta := mcp.Meta{coldread.ClientInfoMeta: map[string]any{"name": "cursor-vscode", "version": "1.7.0"}}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Meta: meta, Name: "search_docs", Arguments: map[string]any{"query": "x"}}); err != nil {
		t.Fatal(err)
	}
	cr.Close()

	recs := in.records()
	// Verifying (as a test binary must), each call is its own POST: they
	// can land in any order.
	rank := map[string]int{"search_docs claude-code": 0, "deploy claude-code": 1, "raw claude-code": 2, "search_docs cursor-vscode": 3}
	sort.SliceStable(recs, func(i, j int) bool {
		key := func(r record) string {
			if r.Client == nil {
				return r.Command
			}
			return r.Command + " " + r.Client.Name
		}
		return rank[key(recs[i])] < rank[key(recs[j])]
	})
	if len(recs) != 4 {
		t.Fatalf("%d records (added twice, counted once; tools/list not at all): %+v", len(recs), recs)
	}
	for i, want := range []struct {
		command string
		exit    int
		client  string
	}{{"search_docs", 0, "claude-code"}, {"deploy", 1, "claude-code"}, {"raw", 1, "claude-code"}, {"search_docs", 0, "cursor-vscode"}} {
		r := recs[i]
		if r.Source != "mcp" || r.Command != want.command || r.Exit != want.exit || r.Client == nil || r.Client.Name != want.client || r.DurationMs == nil || r.Session != "" || string(r.Agent) != "null" {
			t.Errorf("%d: %+v (client %+v)", i, r, r.Client)
		}
	}
	if *recs[0].Client.Version != "2.1.281" {
		t.Errorf("client version %v", recs[0].Client.Version)
	}
	for _, raw := range in.raw {
		if strings.Contains(raw, "hunter2") || strings.Contains(raw, "secret result") || strings.Contains(raw, "no such project") {
			t.Errorf("sent arguments or results: %s", raw)
		}
	}
}

func TestStreamableHTTPSession(t *testing.T) {
	in := newIngest(t)
	cr := newMCP(t, in)
	ctx := context.Background()
	// A server per session, as stateless deployments do: one MCP for all.
	srv := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server(cr, false) }, nil))
	defer srv.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "some-script", Version: "0.0.1"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "search_docs", Arguments: map[string]any{"query": "x"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "search_docs", Arguments: map[string]any{"query": "y"}}); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	cr.Close()
	recs := in.records()
	if len(recs) != 2 || recs[0].Client.Name != "some-script" || len(recs[0].Session) != 16 || recs[0].Session != recs[1].Session {
		t.Fatalf("%+v", recs)
	}
}

func TestOptedOutChangesNothing(t *testing.T) {
	t.Setenv("DO_NOT_TRACK", "1")
	in := newIngest(t)
	cr := coldread.NewMCP(coldread.MCPOptions{Key: key, Tool: "acme-mcp", Endpoint: in.URL})
	next := func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) { return nil, nil }
	got := crmcp.Middleware(cr)(next)
	if cr.Enabled() || got == nil {
		t.Fatal("enabled")
	}
	_, _ = got(context.Background(), "tools/call", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "x"}})
	cr.Close()
	if len(in.records()) != 0 {
		t.Error("sent")
	}
	// A nil MCP is off too.
	_, _ = crmcp.Middleware(nil)(next)(context.Background(), "tools/call", nil)
}

// A server not marked Remote, started from a Claude Code terminal, serving
// HTTP: its calls never take the environment's agent (the operator's
// terminal), whoever called.
func TestHTTPCallsReadNoEnvironment(t *testing.T) {
	in := newIngest(t)
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("COLDREAD_VERIFY", "1")
	t.Setenv("CLAUDECODE", "1")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "operator-terminal")
	cr := coldread.NewMCP(coldread.MCPOptions{Key: key, Tool: "acme-mcp", Version: "1.2.0", Endpoint: in.URL + "/api/ingest"})
	if cr.Agent() == nil {
		t.Fatal("the environment should name an agent")
	}
	ctx := context.Background()
	srv := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server(cr, false) }, nil))
	defer srv.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "some-script", Version: "0.0.1"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "search_docs", Arguments: map[string]any{"query": "x"}}); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	cr.Close()
	recs := in.records()
	if len(recs) != 1 || string(recs[0].Agent) != "null" || recs[0].Client == nil || recs[0].Client.Name != "some-script" {
		t.Fatalf("%+v %s", recs, recs[0].Agent)
	}
}
