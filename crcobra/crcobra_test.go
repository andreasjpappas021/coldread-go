package crcobra_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crcobra"
	"github.com/spf13/cobra"
)

const key = "cr_pub_0123456789abcdef0123456789abcdef"

type record struct {
	Command string   `json:"command"`
	Flags   []string `json:"flags"`
	Exit    int      `json:"exit"`
	Agent   *struct {
		Name string `json:"name"`
	} `json:"agent"`
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

func (s *server) records() []record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]record(nil), s.recs...)
}

func tree() (root, preview *cobra.Command) {
	root = &cobra.Command{Use: "acme", SilenceUsage: true, SilenceErrors: true, Run: func(*cobra.Command, []string) {}}
	root.PersistentFlags().BoolP("verbose", "v", false, "")
	deploy := &cobra.Command{Use: "deploy"}
	preview = &cobra.Command{Use: "preview", Aliases: []string{"pv"}, RunE: func(*cobra.Command, []string) error { return nil }}
	preview.Flags().Bool("prod", false, "")
	preview.Flags().String("token", "", "")
	fail := &cobra.Command{Use: "fail", RunE: func(*cobra.Command, []string) error { return errors.New("no") }}
	deploy.AddCommand(preview)
	root.AddCommand(deploy, fail)
	return root, preview
}

func TestTrack(t *testing.T) {
	for _, c := range []struct {
		args    []string
		command string
		flags   string
	}{
		{[]string{"deploy", "preview", "--prod", "--token=hunter2", "-v", "./site"}, "deploy preview", "--prod --token --verbose"},
		{[]string{"deploy", "pv"}, "deploy preview", ""},
		{[]string{"-v"}, "acme", "--verbose"},
		{[]string{"fail"}, "fail", ""},
	} {
		s := newServer(t)
		cr := coldread.New(coldread.Options{Key: key, Tool: "acme", Version: "1.2.0", Endpoint: s.URL, NoNotice: true})
		root, _ := tree()
		root.SetArgs(c.args)
		root.SetOut(io.Discard)
		cmd, _ := root.ExecuteC()
		crcobra.Track(cr, cmd)
		cr.Finish(0)
		recs := s.records()
		if len(recs) != 1 || recs[0].Command != c.command || strings.Join(recs[0].Flags, " ") != c.flags {
			t.Errorf("%v: got %+v", c.args, recs)
		}
	}
}

func TestPath(t *testing.T) {
	root, preview := tree()
	if crcobra.Path(preview) != "deploy preview" || crcobra.Path(root) != "acme" {
		t.Fatal(crcobra.Path(preview), crcobra.Path(root))
	}
}

// The example CLI as a user's shell runs it: its own process, a real exit.
func TestExecuteInARealBinary(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "acme")
	if out, err := exec.Command("go", "build", "-o", bin, "./example/acme").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	s := newServer(t)
	clean := func(extra ...string) []string {
		var env []string
		for _, kv := range os.Environ() {
			name := kv[:strings.IndexByte(kv+"=", '=')]
			if strings.HasPrefix(name, "CLAUDE") || strings.HasPrefix(name, "CODEX") || name == "AI_AGENT" || name == "AGENT" || strings.HasPrefix(name, "COLDREAD_") || name == "DO_NOT_TRACK" {
				continue
			}
			env = append(env, kv)
		}
		return append(env, append([]string{"COLDREAD_PUBLIC_KEY=" + key, "COLDREAD_ENDPOINT=" + s.URL, "XDG_CACHE_HOME=" + t.TempDir()}, extra...)...)
	}
	runIt := func(env []string, args ...string) (int, string) {
		cmd := exec.Command(bin, args...)
		cmd.Env = env
		var stderr strings.Builder
		cmd.Stderr = &stderr
		err := cmd.Run()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode(), stderr.String()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0, stderr.String()
	}

	if code, _ := runIt(clean("CLAUDE_CODE_CHILD_SESSION=1"), "deploy", "preview", "--prod"); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if code, _ := runIt(clean(), "fail"); code != 1 {
		t.Fatalf("fail exited %d", code)
	}
	code, stderr := runIt(clean("COLDREAD_VERIFY=1"), "status")
	if code != 0 || strings.TrimSpace(stderr) != coldread.VerifyAccepted {
		t.Fatalf("verify: exit %d, stderr %q", code, stderr)
	}
	recs := s.records()
	if len(recs) != 3 {
		t.Fatalf("%+v", recs)
	}
	if recs[0].Command != "deploy preview" || recs[0].Agent == nil || recs[0].Agent.Name != "claude-code" || recs[0].Exit != 0 {
		t.Errorf("as Claude Code: %+v", recs[0])
	}
	if recs[1].Command != "fail" || recs[1].Agent != nil || recs[1].Exit != 1 {
		t.Errorf("as nobody: %+v", recs[1])
	}
	if code, stderr := runIt(clean("COLDREAD_VERIFY=1", "ACME_NO_TELEMETRY=1"), "status"); code != 0 || strings.TrimSpace(stderr) != "[coldread] verify: not sending (ACME_NO_TELEMETRY)." {
		t.Errorf("opted out: %d %q", code, stderr)
	}
}
