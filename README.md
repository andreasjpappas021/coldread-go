# coldread.apappas.dev/go

Which AI agents use your Go website or API, CLI and MCP server. The Go counterpart of `@coldread/track` and `@coldread/cli`.

```sh
go get coldread.apappas.dev/go
```

Go 1.21+. No dependencies; the MCP helpers are their own modules.

## Website or API

`net/http` middleware. The site's secret key in `COLDREAD_KEY`, never in code.

```go
tr := coldread.NewTracker(coldread.TrackerOptions{}) // COLDREAD_KEY from env
defer tr.Close()                                     // sends what's queued
http.ListenAndServe(":8080", tr.Middleware(mux))
```

- chi: `r.Use(tr.Middleware)`. gin, echo, gorilla/mux: `tr.Middleware(router)` where the server is made.
- Graceful shutdown: `tr.Close()` after `srv.Shutdown(ctx)`.
- Not `net/http` (Fiber): `tr.Track(coldread.RequestFacts{...})` once each response is sent.
- IP: `cf-connecting-ip`, `x-real-ip`, the first `x-forwarded-for`, then the connection; `TrackerOptions.IP` for anything else.
- Sent: method, path (no query string), status, IP, host, and `user-agent`, `accept`, `accept-language`, `sec-fetch-dest`, `sec-ch-ua*`, `signature*`, `ai-agent`. Never cookies, auth, other headers or bodies. People's page views go as counts per path and minute (`People: "full"` sends them whole).
- Never delays a response: a goroutine sends batches (2s or 100 requests); when Coldread is down, batches are dropped with backoff, never retried.
- Verify: stderr says `[coldread] connected: first requests accepted` once Coldread accepts the first batch.

## MCP server

The site's public key, in code. One event per `tools/call`: tool, failed or not, duration, the client. Never arguments or results; nothing on stdout.

```go
cr := coldread.NewMCP(coldread.MCPOptions{Key: "cr_pub_...", Tool: "acme-mcp", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
defer cr.Close() // sends what's queued, 2s at most
```

Official SDK (`github.com/modelcontextprotocol/go-sdk` 1.0+):

```sh
go get coldread.apappas.dev/go/crmcp
```

```go
server.AddReceivingMiddleware(crmcp.Middleware(cr))
```

mcp-go (`github.com/mark3labs/mcp-go` 0.43.1+):

```sh
go get coldread.apappas.dev/go/crmcpgo
```

```go
s := server.NewMCPServer("acme", "1.2.0", server.WithToolHandlerMiddleware(crmcpgo.Middleware(cr)))
```

- Remote (HTTP) server: `Remote: true`. stdio: leave it out.
- Any other SDK: `cr.Record(coldread.ToolCall{...})` per call.
- `COLDREAD_VERIFY=1` sends each call at once and prints `[coldread] verify: accepted` (or why not) on stderr.

## CLI: cobra

```sh
go get coldread.apappas.dev/go/crcobra
```

```go
import (
	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crcobra"
)

func main() {
	cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
	crcobra.Execute(cr, rootCmd) // instead of rootCmd.Execute()
}
```

`Execute` tracks the command cobra ran (`deploy preview`, or the tool's name for the root command) with the flags set on it, and exits: 0, or 1 on an error.

A framework that wraps cobra (bep/simplecobra) or your own executor: after it runs, `crcobra.Track(cr, cmd)` with the `*cobra.Command` that ran, then `cr.Exit(code)`.

## CLI: urfave/cli v3

```sh
go get coldread.apappas.dev/go/crurfave
```

```go
cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
crurfave.Execute(context.Background(), cr, cmd, os.Args) // instead of cmd.Run(...)
```

Exits with the error's code (`cli.Exit`), else 1 after printing it.

## CLI: flag, or any parser

```go
cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
flag.Parse()
cr.TrackFlags("deploy", flag.CommandLine) // or cr.Track("deploy preview", "--prod")
// ... run it ...
cr.Exit(code)
```

The root command itself: `cr.Track("")` (sent as the tool's name).

## CLI exit codes

`os.Exit` skips everything, so the send can't hook it. Exit with `cr.Exit(code)` instead, or call `cr.Finish(code)` before main returns. The helpers do it for you. A command that calls `os.Exit`, `log.Fatal` or `cobra.CheckErr` itself exits before anything is sent: return an error instead.

## CLI speed

The connection opens in the background at startup, and what earlier runs couldn't send goes then too. At exit the send waits 300 ms at most (`coldread.ExitWait`); what isn't through by then waits on disk (`~/.cache/coldread/<tool>/spool.jsonl`, the same spool `@coldread/cli` uses) for the next run. Where the cache can't be written (Codex's sandbox), it waits in the temp folder (`$TMPDIR/coldread-<uid>/<tool>/spool.jsonl`), and the next run with network sends both. Durations count from process start (package init), not from `New`. After a send times out, runs skip that wait for 10 minutes and only the background send tries, so a network that drops packets costs one command 300 ms, not every command.

## CLI: also

- `cr.Agent()`: the agent running the CLI, or nil. Use it to skip prompts an agent can't answer.
- `cr.Headers("")`: headers for your CLI's own API calls (`User-Agent: ... AIAgent/<name>` and `AI-Agent`), so your server sees the agent too.
- `InferParent: true` also checks parent processes (Copilot CLI, Aider, Windsurf). It runs `ps` on macOS. Off by default.
- A one-line notice shows once, on stderr, only at a person's terminal. `NoNotice: true` hides it; `Notice` replaces it.
- Off in `go test` binaries and under JavaScript test runners, unless `COLDREAD_ENDPOINT` points somewhere other than Coldread (servers too).

## CLI verify

`COLDREAD_VERIFY=1`: sends for real, waits, and prints `[coldread] verify: accepted` (or why not) on stderr. `COLDREAD_DEBUG=1` prints the event and sends nothing. Unset, neither prints anything.

## For your docs

```md
## Telemetry

Acme sends anonymous usage data to its developers through Coldread: which
command ran, its flag names, exit code and duration,
whether an AI coding agent ran it and which one, and your OS and architecture.
Never arguments, flag values, file paths, your hostname or username, or
anything you type. Your IP address is not stored.

To opt out, set `ACME_NO_TELEMETRY=1` or `DO_NOT_TRACK=1`.
```

## Agents

Detection reads `@coldread/agents`' registry (`registry.json`, embedded), and who counts as a person reads classify()'s patterns (`people.json`); both pass the golden cases the TypeScript and Python SDKs run (`testdata/parity.json`). `go generate` copies all three.
