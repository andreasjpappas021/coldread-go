// Package crmcp reports an MCP server's tool calls to Coldread, for the
// official Go SDK (github.com/modelcontextprotocol/go-sdk).
//
//	cr := coldread.NewMCP(coldread.MCPOptions{Key: "cr_pub_...", Tool: "acme-mcp", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
//	defer cr.Close()
//	server := mcp.NewServer(&mcp.Implementation{Name: "acme", Version: "1.2.0"}, nil)
//	server.AddReceivingMiddleware(crmcp.Middleware(cr))
//
// Before or after tools are added. Every tools/call is one event: the tool
// name, whether it failed (an error, or a result with IsError), how long it
// took, and the client that called (clientInfo from initialize, or from the
// request's _meta). Never the arguments or the result.
package crmcp

import (
	"context"
	"time"

	"coldread.apappas.dev/go"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type counting struct{}

// Middleware reports each tools/call to cr. Add it with
// server.AddReceivingMiddleware. Opted out, it changes nothing.
func Middleware(cr *coldread.MCP) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		if !cr.Enabled() {
			return next
		}
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			// Added twice, a call still counts once.
			if method != "tools/call" || ctx.Value(counting{}) != nil {
				return next(ctx, method, req)
			}
			ctx = context.WithValue(ctx, counting{}, true)
			start := time.Now()
			failed := true
			defer func() {
				call := callOf(req)
				call.Failed, call.Start, call.Duration = failed, start, time.Since(start)
				cr.Record(call)
			}()
			res, err := next(ctx, method, req)
			if err == nil {
				r, ok := res.(*mcp.CallToolResult)
				failed = ok && r != nil && r.IsError
			}
			return res, err
		}
	}
}

// callOf is what Coldread reads from a tools/call request: the tool's name,
// the client (the request's _meta first, then the session's initialize)
// and the transport's session id. Never the arguments.
func callOf(req mcp.Request) coldread.ToolCall {
	var call coldread.ToolCall
	r, ok := req.(*mcp.CallToolRequest)
	if !ok || r == nil {
		return call
	}
	if r.Params != nil {
		call.Name = r.Params.Name
		// A map off the wire; the SDK's own type in process.
		// Codex names its session in _meta (it passes no environment).
		call.AgentSession = coldread.CodexSession(r.Params.Meta)
		switch info := r.Params.Meta[coldread.ClientInfoMeta].(type) {
		case map[string]any:
			call.Client.Name, _ = info["name"].(string)
			call.Client.Version, _ = info["version"].(string)
		case *mcp.Implementation:
			if info != nil {
				call.Client = coldread.ClientInfo{Name: info.Name, Version: info.Version}
			}
		}
	}
	if r.Session != nil {
		if call.Client.Name == "" {
			if p := r.Session.InitializeParams(); p != nil && p.ClientInfo != nil {
				call.Client = coldread.ClientInfo{Name: p.ClientInfo.Name, Version: p.ClientInfo.Version}
			}
		}
		call.SessionID = r.Session.ID()
	}
	// Over HTTP the request's header is there: the environment isn't the
	// caller's, and the User-Agent may name it.
	if r.Extra != nil && r.Extra.Header != nil {
		call.HTTP = true
		call.UserAgent = r.Extra.Header.Get("User-Agent")
	}
	// Codex's turn metadata and no client named (stateless HTTP): Codex.
	if call.Client.Name == "" && call.AgentSession != "" {
		call.Client.Name = coldread.CodexClientName
	}
	return call
}
