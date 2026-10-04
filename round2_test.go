package coldread

import (
	"encoding/json"
	"strings"
	"testing"
)

// What the pressure tests of real CLIs found (hugo, vfox, goose, edgecli):
// leaks, failed parses, killed runs, help runs, install checks, panics.

func lastRecord(t *testing.T, in *ingest) map[string]any {
	t.Helper()
	p := in.posts()
	if len(p) == 0 {
		t.Fatal("nothing sent")
	}
	recs := p[len(p)-1].body.Records
	var m map[string]any
	if err := json.Unmarshal(recs[len(recs)-1], &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestArgvAsTheCommandKeepsOnlyTheCommand(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("postgres://deploy:hunter2@db.internal:5432/app status")
	r.c.Finish(0)
	if len(in.posts()) != 0 {
		t.Fatal("a URL as the command was sent")
	}
	r = start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy /Users/alice/clients/acme sk_live_51HxYzSECRET alice@example.com", "-p", "-hunter2SECRET", "--token=abc", "-dir=/Users/someone/secret")
	r.c.Finish(0)
	raw := string(in.posts()[0].body.Records[0])
	if !strings.Contains(raw, `"command":"deploy","flags":["-p","--token","-dir"]`) {
		t.Fatal(raw)
	}
	for _, leak := range []string{"/Users", "sk_live", "alice@", "hunter2", "postgres:"} {
		if strings.Contains(raw, leak) {
			t.Errorf("%s leaked: %s", leak, raw)
		}
	}
}

func TestParseErrorRecordsTheAttemptWithItsExit(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.ParseError("statsu", "--bogus")
	if len(in.posts()) != 0 {
		t.Fatal("sent before the run ended")
	}
	r.c.Exit(1) // the code the CLI exits with
	m := lastRecord(t, in)
	if m["command"] != "statsu" || m["exit"] != float64(1) || len(m["flags"].([]any)) != 1 || len(in.posts()) != 1 || r.exits[0] != 1 {
		t.Fatal(m)
	}
	// Nothing usable attempted: the root. A CLI that exits 0 anyway: 2.
	r = start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.ParseError("postgres://admin:hunter2@db/app")
	r.c.Finish(0)
	if m := lastRecord(t, in); m["command"] != "acme" || m["exit"] != float64(2) {
		t.Fatal(m)
	}
	// After Track: that command.
	r = start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("server", "--port")
	r.c.ParseError("")
	r.c.Exit(2)
	if m := lastRecord(t, in); m["command"] != "server" || m["exit"] != float64(2) {
		t.Fatal(m)
	}
}

func TestHelpAndVersionRunsSendNothing(t *testing.T) {
	in := newIngest(t)
	for _, c := range []struct {
		command string
		flags   []string
	}{{"", []string{"--help"}}, {"deploy", []string{"--help"}}, {"", []string{"--version"}}, {"help", nil}, {"help deploy", nil}} {
		r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
		r.c.Track(c.command, c.flags...)
		r.c.Finish(0)
	}
	if len(in.posts()) != 0 {
		t.Fatalf("help sent: %v", in.commands())
	}
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("install", "--version") // a subcommand's own --version is a real option
	r.c.Finish(0)
	if len(in.posts()) != 1 {
		t.Fatal("install --version not sent")
	}
	v := start(t, setup{env: with(claudeEnv(), "COLDREAD_VERIFY", "1"), opts: Options{Endpoint: in.endpoint()}})
	v.c.Track("", "--help")
	v.c.Finish(0)
	if got := v.lines(); len(got) != 1 || got[0] != VerifyPrefix+"nothing sent (help or version).\n" {
		t.Fatalf("%q", got)
	}
}

func TestAVerifyRunIsMarked(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{env: with(claudeEnv(), "COLDREAD_VERIFY", "1"), opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy")
	r.c.Finish(0)
	if m := lastRecord(t, in); m["verify"] != true {
		t.Fatal(m)
	}
	r = start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("deploy")
	r.c.Finish(0)
	if m := lastRecord(t, in); m["verify"] != nil {
		t.Fatal("a plain run marked verify")
	}
}

func TestRecoverRecordsAPanicAsExit2AndPanicsAgain(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("build")
	func() {
		defer func() {
			if v := recover(); v != "boom" {
				t.Fatalf("panic %v not raised again", v)
			}
		}()
		func() {
			defer r.c.Recover()
			panic("boom")
		}()
	}()
	if m := lastRecord(t, in); m["command"] != "build" || m["exit"] != float64(2) {
		t.Fatal(m)
	}
}
