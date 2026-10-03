package crurfave_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crurfave"
	"github.com/urfave/cli/v3"
)

const key = "cr_pub_0123456789abcdef0123456789abcdef"

type record struct {
	Command string   `json:"command"`
	Flags   []string `json:"flags"`
	Exit    int      `json:"exit"`
}

type server struct {
	*httptest.Server
	mu   sync.Mutex
	recs []record
}

func newServer(t *testing.T) *server {
	s := &server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Records []record }
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.recs = append(s.recs, body.Records...)
		s.mu.Unlock()
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"accepted":1,"rejected":[]}`)
	}))
	t.Cleanup(s.Close)
	return s
}

func app(ran *[]string) *cli.Command {
	return &cli.Command{
		Name:   "acme",
		Flags:  []cli.Flag{&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}}},
		Action: func(context.Context, *cli.Command) error { *ran = append(*ran, "acme"); return nil },
		Before: func(ctx context.Context, _ *cli.Command) (context.Context, error) {
			*ran = append(*ran, "before")
			return ctx, nil
		},
		Commands: []*cli.Command{{
			Name: "deploy",
			Commands: []*cli.Command{{
				Name:    "preview",
				Aliases: []string{"pv"},
				Flags:   []cli.Flag{&cli.BoolFlag{Name: "prod"}, &cli.StringFlag{Name: "token"}, &cli.BoolFlag{Name: "y"}},
				Action:  func(context.Context, *cli.Command) error { *ran = append(*ran, "preview"); return nil },
			}},
		}},
	}
}

func TestInstrument(t *testing.T) {
	for _, c := range []struct {
		args    []string
		command string
		flags   string
	}{
		{[]string{"acme", "deploy", "preview", "--prod", "--token=hunter2", "-y", "./site"}, "deploy preview", "--prod --token -y"},
		{[]string{"acme", "-v", "deploy", "pv"}, "deploy preview", "--verbose"},
		{[]string{"acme", "--verbose"}, "acme", "--verbose"},
	} {
		s := newServer(t)
		cr := coldread.New(coldread.Options{Key: key, Tool: "acme", Version: "1.2.0", Endpoint: s.URL, NoNotice: true})
		var ran []string
		root := app(&ran)
		crurfave.Instrument(cr, root)
		if err := root.Run(context.Background(), c.args); err != nil {
			t.Fatal(err)
		}
		cr.Finish(0)
		if len(s.recs) != 1 || s.recs[0].Command != c.command || strings.Join(s.recs[0].Flags, " ") != c.flags {
			t.Errorf("%v: got %+v", c.args, s.recs)
		}
		if len(ran) < 2 || ran[0] != "before" {
			t.Errorf("%v: the app's own Before didn't run first: %v", c.args, ran)
		}
	}
}

// Execute in its own process: the exit code, including urfave/cli's own
// exit for cli.Exit errors.
func TestExecute(t *testing.T) {
	if mode := os.Getenv("CRURFAVE_CHILD"); mode != "" {
		cr := coldread.New(coldread.Options{Key: key, Tool: "acme", Version: "1.2.0", NoNotice: true})
		root := &cli.Command{Name: "acme", Commands: []*cli.Command{
			{Name: "ok", Action: func(context.Context, *cli.Command) error { return nil }},
			{Name: "coded", Action: func(context.Context, *cli.Command) error { return cli.Exit("coded failure", 3) }},
			{Name: "plain", Action: func(context.Context, *cli.Command) error { return errors.New("plain failure") }},
		}}
		crurfave.Execute(context.Background(), cr, root, []string{"acme", mode})
		return
	}
	s := newServer(t)
	for mode, want := range map[string]int{"ok": 0, "coded": 3, "plain": 1} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestExecute$")
		cmd.Env = append(os.Environ(), "CRURFAVE_CHILD="+mode, "COLDREAD_ENDPOINT="+s.URL, "XDG_CACHE_HOME="+t.TempDir())
		out, err := cmd.CombinedOutput()
		code := 0
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		}
		if code != want {
			t.Errorf("%s: exit %d, want %d\n%s", mode, code, want, out)
		}
	}
	got := map[string]int{}
	for _, r := range s.recs {
		got[r.Command] = r.Exit
	}
	if len(got) != 3 || got["ok"] != 0 || got["coded"] != 3 || got["plain"] != 1 {
		t.Fatalf("sent %+v", s.recs)
	}
}
