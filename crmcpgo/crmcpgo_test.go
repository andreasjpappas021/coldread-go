package crmcpgo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crmcpgo"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
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
	Agent   json.RawMessage `json:"agent"`
	Session string          `json:"session"`
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

func newMCP(t *testing.T, in *ingest) *coldread.MCP {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	// Remote: this test's own environment (maybe a coding agent's) stays out of it.
	cr := coldread.NewMCP(coldread.MCPOptions{Key: key, Tool: "acme-mcp", Version: "1.2.0", Endpoint: in.URL + "/api/ingest", Remote: true})
	if !cr.Enabled() {
		t.Fatal("not enabled")
	}
	return cr
}

// newServer is an MCP server like a customer's, with Coldread's middleware
// (twice, to show a call still counts once) and a tool added after it.
func newServer(cr *coldread.MCP) *server.MCPServer {
	s := server.NewMCPServer("acme", "1.2.0",
		server.WithToolHandlerMiddleware(crmcpgo.Middleware(cr)),
		server.WithToolHandlerMiddleware(crmcpgo.Middleware(cr)))
	s.AddTool(mcp.NewTool("search_docs", mcp.WithString("query")), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("secret result for " + req.GetString("query", "")), nil
	})
	s.AddTool(mcp.NewTool("deploy"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultError("no such project"), nil
	})
	s.AddTool(mcp.NewTool("crash"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("handler failed")
	})
	return s
}

func connect(t *testing.T, c *client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	init := mcp.InitializeRequest{}
	init.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	init.Params.ClientInfo = mcp.Implementation{Name: name, Version: "2.1.281"}
	if _, err := c.Initialize(ctx, init); err != nil {
		t.Fatal(err)
	}
}

func call(c *client.Client, name string, args map[string]any, meta *mcp.Meta) (*mcp.CallToolResult, error) {
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	req.Params.Meta = meta
	return c.CallTool(context.Background(), req)
}

// stdio connects a client to s's stdio server over pipes, in process.
func stdio(t *testing.T, s *server.MCPServer, name string) *client.Client {
	t.Helper()
	serverIn, clientOut := io.Pipe()
	clientIn, serverOut := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = server.NewStdioServer(s).Listen(ctx, serverIn, serverOut) }()
	c := client.NewClient(transport.NewIO(clientIn, clientOut, io.NopCloser(strings.NewReader(""))))
	connect(t, c, name)
	return c
}

func TestToolCallsOverStdio(t *testing.T) {
	in := newIngest(t)
	cr := newMCP(t, in)
	c := stdio(t, newServer(cr), "claude-code")

	if res, err := call(c, "search_docs", map[string]any{"query": "hunter2"}, nil); err != nil || res.IsError {
		t.Fatalf("search_docs: %v %+v", err, res)
	}
	if res, err := call(c, "deploy", nil, nil); err != nil || !res.IsError {
		t.Fatalf("deploy: %v %+v", err, res)
	}
	if _, err := call(c, "crash", nil, nil); err == nil {
		t.Fatal("crash: no error")
	}
	if _, err := c.ListTools(context.Background(), mcp.ListToolsRequest{}); err != nil {
		t.Fatal(err)
	}
	cr.Close()

	recs := in.records()
	if len(recs) != 3 {
		t.Fatalf("%d records (added twice, counted once): %+v", len(recs), recs)
	}
	for i, want := range []struct {
		command string
		exit    int
		client  string
	}{{"search_docs", 0, "claude-code"}, {"deploy", 1, "claude-code"}, {"crash", 1, "claude-code"}} {
		r := recs[i]
		// stdio's session id is always "stdio": not sent as a session.
		if r.Source != "mcp" || r.Command != want.command || r.Exit != want.exit || r.Client == nil || r.Client.Name != want.client || r.DurationMs == nil || string(r.Agent) != "null" || r.Session != "" {
			t.Errorf("%d: %+v (client %+v)", i, r, r.Client)
		}
	}
	if *recs[0].Client.Version != "2.1.281" {
		t.Errorf("client version %v", recs[0].Client.Version)
	}
	for _, raw := range in.raw {
		if strings.Contains(raw, "hunter2") || strings.Contains(raw, "secret result") || strings.Contains(raw, "no such project") || strings.Contains(raw, "handler failed") {
			t.Errorf("sent arguments or results: %s", raw)
		}
	}
}

func TestStreamableHTTPSession(t *testing.T) {
	in := newIngest(t)
	cr := newMCP(t, in)
	srv := httptest.NewServer(server.NewStreamableHTTPServer(newServer(cr)))
	defer srv.Close()
	c, err := client.NewStreamableHttpClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	connect(t, c, "some-script")
	for _, q := range []string{"x", "y"} {
		if _, err := call(c, "search_docs", map[string]any{"query": q}, nil); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()
	cr.Close()
	recs := in.records()
	if len(recs) != 2 || recs[0].Client == nil || recs[0].Client.Name != "some-script" || recs[0].Session != recs[1].Session {
		t.Fatalf("%+v", recs)
	}
	// Sessions are gone from the 2026-07-28 spec (mcp-go 1.x speaks it).
	if stateful := mcp.LATEST_PROTOCOL_VERSION < "2026"; stateful != (len(recs[0].Session) == 16) {
		t.Errorf("protocol %s, session %q", mcp.LATEST_PROTOCOL_VERSION, recs[0].Session)
	}
}

func TestOptedOutChangesNothing(t *testing.T) {
	t.Setenv("DO_NOT_TRACK", "1")
	in := newIngest(t)
	cr := coldread.NewMCP(coldread.MCPOptions{Key: key, Tool: "acme-mcp", Endpoint: in.URL})
	next := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) { return nil, nil }
	if cr.Enabled() {
		t.Fatal("enabled")
	}
	_, _ = crmcpgo.Middleware(cr)(next)(context.Background(), mcp.CallToolRequest{})
	_, _ = crmcpgo.Middleware(nil)(next)(context.Background(), mcp.CallToolRequest{})
	cr.Close()
	if len(in.records()) != 0 {
		t.Error("sent")
	}
}
