// Package crmcpgo reports an MCP server's tool calls to Coldread, for
// mcp-go (github.com/mark3labs/mcp-go).
//
//	cr := coldread.NewMCP(coldread.MCPOptions{Key: "cr_pub_...", Tool: "acme-mcp", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
//	defer cr.Close()
//	s := server.NewMCPServer("acme", "1.2.0", server.WithToolHandlerMiddleware(crmcpgo.Middleware(cr)), server.WithHooks(crmcpgo.Hooks(cr)))
//
// The hooks catch the calls mcp-go refuses before any middleware runs (a
// tool that doesn't exist): each is a failed call to the name asked for,
// as the Node and Python SDKs record it. Your own hooks: AddHooks.
//
// Every tools/call is one event: the tool name, whether it failed (an
// error, or a result with IsError), how long it took, and the client that
// called (clientInfo from initialize, or from the request's _meta). Never
// the arguments or the result.
package crmcpgo

import (
	"context"
	"errors"
	"time"

	"coldread.apappas.dev/go"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type counting struct{}

// Middleware reports each tool call to cr: pass it to
// server.WithToolHandlerMiddleware when making the server (or to s.Use in
// newer mcp-go). Opted out, it changes nothing.
func Middleware(cr *coldread.MCP) server.ToolHandlerMiddleware {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		if !cr.Enabled() {
			return next
		}
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Added twice, a call still counts once.
			if ctx.Value(counting{}) != nil {
				return next(ctx, req)
			}
			ctx = context.WithValue(ctx, counting{}, true)
			start := time.Now()
			failed := true
			defer func() {
				call := callOf(ctx, req)
				call.Failed, call.Start, call.Duration = failed, start, time.Since(start)
				cr.Record(call)
			}()
			res, err := next(ctx, req)
			failed = err != nil || (res != nil && res.IsError)
			return res, err
		}
	}
}

// callOf is what Coldread reads from a tool call: the tool's name, the
// client (the request's _meta first, then the session's initialize) and the
// session id. Never the arguments.
func callOf(ctx context.Context, req mcp.CallToolRequest) coldread.ToolCall {
	call := coldread.ToolCall{Name: req.Params.Name}
	if m := req.Params.Meta; m != nil {
		// Codex names its session in _meta (it passes no environment).
		call.AgentSession = coldread.CodexSession(m.AdditionalFields)
		// A map off the wire; the SDK's own type in process.
		switch info := m.AdditionalFields[coldread.ClientInfoMeta].(type) {
		case map[string]any:
			call.Client.Name, _ = info["name"].(string)
			call.Client.Version, _ = info["version"].(string)
		case mcp.Implementation:
			call.Client = coldread.ClientInfo{Name: info.Name, Version: info.Version}
		case *mcp.Implementation:
			if info != nil {
				call.Client = coldread.ClientInfo{Name: info.Name, Version: info.Version}
			}
		}
	}
	// Over HTTP the request's header is there: the environment isn't the
	// caller's, and the User-Agent may name it.
	if req.Header != nil {
		call.HTTP = true
		call.UserAgent = req.Header.Get("User-Agent")
	}
	// Codex's turn metadata and no client named (stateless HTTP): Codex.
	defer func() {
		if call.Client.Name == "" && call.AgentSession != "" {
			call.Client.Name = coldread.CodexClientName
		}
	}()
	session := server.ClientSessionFromContext(ctx)
	if session == nil {
		return call
	}
	if s, ok := session.(server.SessionWithClientInfo); ok && call.Client.Name == "" {
		info := s.GetClientInfo()
		call.Client = coldread.ClientInfo{Name: info.Name, Version: info.Version}
	}
	// stdio's session is always "stdio": not a session.
	if id := session.SessionID(); id != "stdio" {
		call.SessionID = id
	}
	return call
}

// Hooks are server hooks that report the tool calls mcp-go refuses before
// the middleware runs: a call to a tool that doesn't exist is a failed call
// to the name asked for. Pass them with server.WithHooks; if you have hooks
// of your own, AddHooks adds to them instead.
func Hooks(cr *coldread.MCP) *server.Hooks {
	h := &server.Hooks{}
	AddHooks(cr, h)
	return h
}

// AddHooks adds Coldread's hooks to h. Opted out, it adds nothing.
func AddHooks(cr *coldread.MCP, h *server.Hooks) {
	if h == nil || !cr.Enabled() {
		return
	}
	h.AddOnError(func(ctx context.Context, _ any, method mcp.MCPMethod, message any, err error) {
		if method != mcp.MethodToolsCall || !errors.Is(err, server.ErrToolNotFound) {
			return
		}
		req, ok := message.(*mcp.CallToolRequest)
		if !ok || req == nil {
			return
		}
		call := callOf(ctx, *req)
		call.Failed = true
		cr.Record(call)
	})
}
