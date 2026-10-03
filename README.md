# coldread.apappas.dev/go

Which AI agents use your Go CLI, and for which commands. The Go counterpart of `@coldread/cli`.

```sh
go get coldread.apappas.dev/go
```

Go 1.21+. No dependencies.

## cobra

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

`Execute` tracks the command cobra ran (`deploy preview`) with the flags set on it, and exits: 0, or 1 on an error.

## urfave/cli v3

```sh
go get coldread.apappas.dev/go/crurfave
```

```go
cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
crurfave.Execute(context.Background(), cr, cmd, os.Args) // instead of cmd.Run(...)
```

Exits with the error's code (`cli.Exit`), else 1 after printing it.

## flag, or any parser

```go
cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: "1.2.0", OptOut: "ACME_NO_TELEMETRY"})
flag.Parse()
cr.TrackFlags("deploy", flag.CommandLine) // or cr.Track("deploy preview", "--prod")
// ... run it ...
cr.Exit(code)
```

## Exit codes

`os.Exit` skips everything, so the send can't hook it. Exit with `cr.Exit(code)` instead, or call `cr.Finish(code)` before main returns. The helpers do it for you. A command that calls `os.Exit`, `log.Fatal` or `cobra.CheckErr` itself exits before anything is sent: return an error instead.

## Speed

The connection opens in the background at startup, and what earlier runs couldn't send goes then too. At exit the send waits 300 ms at most (`coldread.ExitWait`); what isn't through by then waits on disk (`~/.cache/coldread/<tool>/spool.jsonl`, the same spool `@coldread/cli` uses) for the next run. After a send times out, runs skip that wait for 10 minutes and only the background send tries, so a network that drops packets costs one command 300 ms, not every command.

## Also

- `cr.Agent()`: the agent running the CLI, or nil. Use it to skip prompts an agent can't answer.
- `cr.Headers("")`: headers for your CLI's own API calls (`User-Agent: ... AIAgent/<name>` and `AI-Agent`), so your server sees the agent too.
- `InferParent: true` also checks parent processes (Copilot CLI, Aider, Windsurf). It runs `ps` on macOS. Off by default.
- A one-line notice shows once, on stderr, only at a person's terminal. `NoNotice: true` hides it; `Notice` replaces it.
- Off in `go test` binaries and under JavaScript test runners, unless `COLDREAD_ENDPOINT` points somewhere other than Coldread.

## Verify

`COLDREAD_VERIFY=1`: sends for real, waits, and prints `[coldread] verify: accepted` (or why not) on stderr. `COLDREAD_DEBUG=1` prints the event and sends nothing. Unset, neither prints anything.

## For your docs

```md
## Telemetry

Acme sends anonymous usage data to its developers through Coldread: which
command ran (or MCP tool was called), its flag names, exit code and duration,
whether an AI coding agent ran it and which one, and your OS and architecture.
Never arguments, flag values, file paths, your hostname or username, or
anything you type. Your IP address is not stored.

To opt out, set `ACME_NO_TELEMETRY=1` or `DO_NOT_TRACK=1`.
```

## Agents

Detection reads `@coldread/agents`' registry (`registry.json`, embedded) and passes the golden cases the TypeScript and Python detectors run (`testdata/parity.json`).
