//go:build !windows

package coldread

import (
	"bufio"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Killed runs, as real processes: this test binary, run again as the CLI.

// The killed-run child: a CLI that tracks `seed run`, says ready, and hangs.
// GO_KILLED_CHILD names its cache. GO_KILLED_MODE: "" (default), capture
// (CaptureSignals) or own (OwnSignals). GO_KILLED_HANDLER: the CLI handles
// SIGTERM itself, set up before or after Track, with a graceful shutdown
// that takes 300 ms (a server closing listeners, hugo cleaning up).
func TestKilledChild(t *testing.T) {
	cache := os.Getenv("GO_KILLED_CHILD")
	if cache == "" {
		t.Skip("run by TestKilledRuns")
	}
	mode, handler := os.Getenv("GO_KILLED_MODE"), os.Getenv("GO_KILLED_HANDLER")
	exited := make(chan int, 1)
	c := newClient(Options{Key: testKey, Tool: "acme", Version: "1", Endpoint: "http://127.0.0.1:9/api/ingest", NoNotice: true, CaptureSignals: mode == "capture", OwnSignals: mode == "own"}, internals{
		env: with(claudeEnv(), "COLDREAD_DEBUG", os.Getenv("COLDREAD_DEBUG"), "COLDREAD_VERIFY", os.Getenv("COLDREAD_VERIFY")), cacheDir: cache, tmpDir: cache, testRun: bptr(false),
		isTTY: bptr(false), stderrTTY: bptr(false), exit: func(code int) { exited <- code },
	})
	own := func() {
		ch := make(chan os.Signal, 1)
		signalNotify(ch, syscall.SIGTERM)
		go func() {
			<-ch
			if os.Getenv("GO_KILLED_FAST") == "" {
				time.Sleep(300 * time.Millisecond)
			}
			os.Stdout.WriteString("cleaned up\n")
			c.Exit(7)
		}()
	}
	if handler == "before" {
		own()
	}
	c.Track("seed run")
	if handler == "after" {
		own()
	}
	os.Stdout.WriteString("ready\n")
	select {
	case code := <-exited:
		os.Exit(code)
	case <-time.After(30 * time.Second):
	}
}

func TestKilledRuns(t *testing.T) {
	type result struct {
		cmd         *exec.Cmd
		out, stderr string
		spool       [][]byte
	}
	run := func(sig syscall.Signal, env ...string) result {
		cache := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestKilledChild$")
		cmd.Env = append(os.Environ(), append([]string{"GO_KILLED_CHILD=" + cache}, env...)...)
		out, _ := cmd.StdoutPipe()
		var stderr strings.Builder
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(out)
		var lines []string
		for sc.Scan() {
			lines = append(lines, sc.Text())
			if sc.Text() == "ready" {
				_ = cmd.Process.Signal(sig)
			}
		}
		_ = cmd.Wait()
		return result{cmd, strings.Join(lines, "\n"), stderr.String(), readSpool(filepath.Join(cache, "spool.jsonl"), time.Now())}
	}
	killedBy := func(r result, sig syscall.Signal) bool {
		ws := r.cmd.ProcessState.Sys().(syscall.WaitStatus)
		return ws.Signaled() && ws.Signal() == sig
	}
	only := func(r result, exit int) bool {
		return len(r.spool) == 1 && strings.Contains(string(r.spool[0]), `"command":"seed run"`) && strings.Contains(string(r.spool[0]), `"exit":`+itoa(exit))
	}

	// Default: nothing is caught. A CLI with no handler dies of the signal
	// as ever, and the run isn't recorded.
	if r := run(syscall.SIGTERM); !killedBy(r, syscall.SIGTERM) || len(r.spool) != 0 {
		t.Errorf("default: %v, spool %s", r.cmd.ProcessState, r.spool)
	}
	// Default, with a graceful shutdown of its own: it runs to the end and
	// the CLI's exit stands (its run reads its own exit, 0).
	for _, when := range []string{"before", "after"} {
		r := run(syscall.SIGTERM, "GO_KILLED_HANDLER="+when)
		if r.cmd.ProcessState.ExitCode() != 7 || !strings.Contains(r.out, "cleaned up") || !only(r, 7) {
			t.Errorf("default, own handler %s: %v %q spool %s", when, r.cmd.ProcessState, r.out, r.spool)
		}
	}
	// CaptureSignals: saved as 128+n, then dies of the signal.
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGHUP} {
		if r := run(sig, "GO_KILLED_MODE=capture"); !killedBy(r, sig) || !only(r, 128+int(sig)) {
			t.Errorf("capture %v: %v, spool %s", sig, r.cmd.ProcessState, r.spool)
		}
	}
	// SIGQUIT: Go's own way of dying on it (a goroutine dump, exit 2) is kept.
	if r := run(syscall.SIGQUIT, "GO_KILLED_MODE=capture"); r.cmd.ProcessState.ExitCode() != 2 || !strings.Contains(r.stderr, "SIGQUIT") || !only(r, 131) {
		t.Errorf("capture SIGQUIT: %v, spool %s", r.cmd.ProcessState, r.spool)
	}
	// OwnSignals, the handler set up before or after Track: the whole
	// graceful shutdown runs, the CLI exits 0, and the run reads 143.
	for _, when := range []string{"before", "after"} {
		r := run(syscall.SIGTERM, "GO_KILLED_MODE=own", "GO_KILLED_HANDLER="+when)
		if r.cmd.ProcessState.ExitCode() != 7 || !strings.Contains(r.out, "cleaned up") || !only(r, 143) {
			t.Errorf("own handler %s: %v %q spool %s", when, r.cmd.ProcessState, r.out, r.spool)
		}
	}
	// A handler that exits at once (no shutdown to speak of): still 143,
	// every time, though it may read the signal before Coldread does.
	for i := 0; i < 20; i++ {
		r := run(syscall.SIGTERM, "GO_KILLED_MODE=own", "GO_KILLED_HANDLER=before", "GO_KILLED_FAST=1")
		if r.cmd.ProcessState.ExitCode() != 7 || !only(r, 143) {
			t.Fatalf("own handler, fast exit, try %d: %v spool %s", i, r.cmd.ProcessState, r.spool)
		}
	}
	// SIGINT: nothing.
	if r := run(syscall.SIGINT, "GO_KILLED_MODE=capture"); len(r.spool) != 0 {
		t.Errorf("SIGINT sent %s", r.spool)
	}
	// COLDREAD_VERIFY says so.
	if r := run(syscall.SIGTERM, "GO_KILLED_MODE=capture", "COLDREAD_VERIFY=1"); !strings.Contains(r.stderr, VerifyPrefix+"killed (exit 143); saved for the next run.") {
		t.Errorf("verify: %s", r.stderr)
	}
}

func signalNotify(c chan<- os.Signal, sig ...os.Signal) { signal.Notify(c, sig...) }

// The MCP child: a stdio server that records one call, says ready, hangs.
func TestMCPKilledChild(t *testing.T) {
	endpoint := os.Getenv("GO_MCP_CHILD")
	if endpoint == "" {
		t.Skip("run by TestMCPShutDownByItsClient")
	}
	m := newMCP(MCPOptions{Key: testKey, Tool: "acme-mcp", Version: "1", Endpoint: endpoint, FlushInterval: time.Hour, FlushOnSignal: os.Getenv("GO_MCP_FLUSH") == "1"}, mcpInternals{
		env: map[string]string{}, cacheDir: os.Getenv("GO_MCP_CACHE"), tmpDir: os.Getenv("GO_MCP_CACHE"), testRun: bptr(false),
	})
	m.Record(ToolCall{Name: "search_docs", Client: ClientInfo{Name: "claude-code"}})
	os.Stdout.WriteString("ready\n")
	time.Sleep(30 * time.Second)
}

// Claude Code's SIGINT and Codex's SIGTERM: the server dies of the signal
// as it would have. By default nothing is caught and the call waits in the
// spool for the next start; with FlushOnSignal it's sent first.
func TestMCPShutDownByItsClient(t *testing.T) {
	for _, flush := range []bool{false, true} {
		for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
			in := newIngest(t)
			cache := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMCPKilledChild$")
			cmd.Env = append(os.Environ(), "GO_MCP_CHILD="+in.endpoint(), "GO_MCP_CACHE="+cache)
			if flush {
				cmd.Env = append(cmd.Env, "GO_MCP_FLUSH=1")
			}
			out, _ := cmd.StdoutPipe()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				if sc.Text() == "ready" {
					_ = cmd.Process.Signal(sig)
				}
			}
			_ = cmd.Wait()
			ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if !ws.Signaled() || ws.Signal() != sig {
				t.Errorf("flush=%v %v: not killed by it: %v", flush, sig, cmd.ProcessState)
			}
			got := strings.Join(in.commands(), ",")
			spooled := readSpool(filepath.Join(cache, "spool.jsonl"), time.Now())
			if flush && got != "search_docs" {
				t.Errorf("FlushOnSignal %v: sent %v", sig, got)
			}
			if !flush && (got != "" || len(spooled) != 1) {
				t.Errorf("default %v: sent %v, spool %s", sig, got, spooled)
			}
		}
	}
}
