package crmcpgo

import (
	"context"
	"testing"

	"coldread.apappas.dev/go"
	"github.com/mark3labs/mcp-go/mcp"
)

// The stateless spec's per-request clientInfo: off the wire (a map) or in
// process (the SDK's type). Real clients send their own, so this can't be
// driven through one.
func TestClientInfoFromMeta(t *testing.T) {
	for _, info := range []any{
		map[string]any{"name": "cursor-vscode", "version": "1.7.0"},
		mcp.Implementation{Name: "cursor-vscode", Version: "1.7.0"},
		&mcp.Implementation{Name: "cursor-vscode", Version: "1.7.0"},
	} {
		req := mcp.CallToolRequest{}
		req.Params.Name = "search_docs"
		req.Params.Meta = &mcp.Meta{AdditionalFields: map[string]any{coldread.ClientInfoMeta: info}}
		got := callOf(context.Background(), req)
		if got.Name != "search_docs" || got.Client != (coldread.ClientInfo{Name: "cursor-vscode", Version: "1.7.0"}) || got.SessionID != "" {
			t.Errorf("%T: %+v", info, got)
		}
	}
	if got := callOf(context.Background(), mcp.CallToolRequest{}); got != (coldread.ToolCall{}) {
		t.Errorf("empty: %+v", got)
	}
}
