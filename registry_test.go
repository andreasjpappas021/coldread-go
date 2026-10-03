package coldread

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// In the monorepo, the copies must be byte-for-byte @coldread/agents' own
// (`go generate`). Outside it, the source isn't there to compare.
func TestRegistryIsTheAgentsOne(t *testing.T) {
	for copy, source := range map[string]string{
		"registry.json":        "../agents/src/registry.json",
		"testdata/parity.json": "../agents/test/parity.json",
	} {
		want, err := os.ReadFile(source)
		if os.IsNotExist(err) {
			t.Skipf("%s: not in the monorepo", source)
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := mustRead(t, copy); !bytes.Equal(got, want) {
			t.Errorf("%s differs from %s: run `go generate` in packages/go", copy, source)
		}
	}
}

func TestRegistryLoads(t *testing.T) {
	if !bytes.Equal(registryJSON, mustRead(t, "registry.json")) {
		t.Fatal("the embedded registry isn't registry.json")
	}
	var raw struct {
		ProcessNames []json.RawMessage `json:"processNames"`
	}
	_ = json.Unmarshal(registryJSON, &raw)
	if len(registry.Rules) == 0 || len(registry.GenericEnv) == 0 || len(registry.CIVars) == 0 || len(registry.Aliases) == 0 {
		t.Fatal("registry didn't load")
	}
	// Every process pattern compiles in Go's regexp as well as JavaScript's.
	if len(registry.ProcessNames) != len(raw.ProcessNames) || len(raw.ProcessNames) == 0 {
		t.Fatalf("%d of %d process patterns compiled", len(registry.ProcessNames), len(raw.ProcessNames))
	}
	for _, rule := range registry.Rules {
		for _, m := range rule.Markers {
			if !map[string]bool{"set": true, "true": true, "no-tty": true, "equals": true}[m.When] {
				t.Errorf("%s: %s: when %q isn't one this detector knows", rule.Name, m.Env, m.When)
			}
		}
	}
	claude := registry.Rules[0]
	if claude.Name != "claude-code" || claude.Host == nil || len(claude.Host.Prefix) != 2 || claude.Host.Prefix[0][0] != "sdk" {
		t.Fatalf("claude-code rule %+v", claude)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type wantAgent struct {
	Name       string   `json:"name"`
	Raw        *string  `json:"raw"`
	Version    *string  `json:"version"`
	Host       *string  `json:"host"`
	Evidence   string   `json:"evidence"`
	Confidence string   `json:"confidence"`
	Signals    []string `json:"signals"`
}

func toWant(a *Agent) *wantAgent {
	if a == nil {
		return nil
	}
	signals := a.Signals
	if signals == nil {
		signals = []string{}
	}
	return &wantAgent{Name: a.Name, Raw: strOrNil(a.Raw), Version: strOrNil(a.Version), Host: strOrNil(a.Host), Evidence: a.Evidence, Confidence: a.Confidence, Signals: signals}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// The golden cases the TypeScript (parity.test.ts) and Python
// (test_registry.py) detectors run. The MCP-client, company and people
// groups are for MCP servers and the server side; a CLI SDK has none.
func TestParityCases(t *testing.T) {
	var cases struct {
		Detect []struct {
			Env   map[string]string `json:"env"`
			IsTTY bool              `json:"isTTY"`
			Want  struct {
				Agent           *wantAgent `json:"agent"`
				CI              bool       `json:"ci"`
				SessionID       *string    `json:"sessionId"`
				NetworkDisabled bool       `json:"networkDisabled"`
			} `json:"want"`
		} `json:"detect"`
		Normalize []struct {
			In   string  `json:"in"`
			Want *string `json:"want"`
		} `json:"normalizeAgentName"`
		ParseAgentID []struct {
			In   string `json:"in"`
			Want *struct {
				Name    string  `json:"name"`
				Version *string `json:"version"`
			} `json:"want"`
		} `json:"parseAgentId"`
		AgentFromProcess []struct {
			In   []string `json:"in"`
			Want *string  `json:"want"`
		} `json:"agentFromProcess"`
		FormatAgentHeader []struct {
			In struct {
				Name     string  `json:"name"`
				Version  *string `json:"version"`
				Host     *string `json:"host"`
				Evidence string  `json:"evidence"`
			} `json:"in"`
			Want string `json:"want"`
		} `json:"formatAgentHeader"`
	}
	if err := json.Unmarshal(mustRead(t, "testdata/parity.json"), &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.Detect) == 0 || len(cases.Normalize) == 0 || len(cases.ParseAgentID) == 0 || len(cases.AgentFromProcess) == 0 || len(cases.FormatAgentHeader) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases.Detect {
		d := detectAgent(c.Env, c.IsTTY, nil)
		if got := toWant(d.agent); !reflect.DeepEqual(got, c.Want.Agent) {
			g, _ := json.Marshal(got)
			w, _ := json.Marshal(c.Want.Agent)
			t.Errorf("%v tty=%v\n got %s\nwant %s", c.Env, c.IsTTY, g, w)
		}
		if d.ci != c.Want.CI || d.networkDisabled != c.Want.NetworkDisabled || d.sessionID != deref(c.Want.SessionID) {
			t.Errorf("%v: ci=%v networkDisabled=%v session=%q; want %v %v %q", c.Env, d.ci, d.networkDisabled, d.sessionID, c.Want.CI, c.Want.NetworkDisabled, deref(c.Want.SessionID))
		}
	}
	for _, c := range cases.Normalize {
		if got := normalizeAgentName(c.In); got != deref(c.Want) {
			t.Errorf("normalizeAgentName(%q) = %q, want %q", c.In, got, deref(c.Want))
		}
	}
	for _, c := range cases.ParseAgentID {
		got := parseAgentID(c.In)
		if (got == nil) != (c.Want == nil) || (got != nil && (got.name != c.Want.Name || got.version != deref(c.Want.Version))) {
			t.Errorf("parseAgentID(%q) = %+v, want %+v", c.In, got, c.Want)
		}
	}
	for _, c := range cases.AgentFromProcess {
		if got := agentFromProcess(c.In); got != deref(c.Want) {
			t.Errorf("agentFromProcess(%q) = %q, want %q", c.In, got, deref(c.Want))
		}
	}
	for _, c := range cases.FormatAgentHeader {
		a := Agent{Name: c.In.Name, Version: deref(c.In.Version), Host: deref(c.In.Host), Evidence: c.In.Evidence}
		if got := formatAgentHeader(&a); got != c.Want {
			t.Errorf("formatAgentHeader(%+v) = %s, want %s", c.In, got, c.Want)
		}
	}
}

// What the CLI SDK adds on top of the shared cases: the header it stamps,
// the parent walk's result, CI, and the CI override.
func TestHeaderAncestorsAndCI(t *testing.T) {
	for _, c := range []struct {
		a    Agent
		want string
	}{
		{Agent{Name: "claude-code", Version: "2.1.281", Host: "terminal", Evidence: "declared"}, `name="claude-code", version="2.1.281", host="terminal", evidence="declared"`},
		{Agent{Name: `a"b\c`, Host: "héllo", Evidence: "inferred"}, `name="a\"b\\c", host="hllo", evidence="inferred"`},
		{Agent{Name: "codex", Evidence: "declared"}, `name="codex", evidence="declared"`},
	} {
		if got := formatAgentHeader(&c.a); got != c.want {
			t.Errorf("formatAgentHeader = %s, want %s", got, c.want)
		}
	}
	got := toWant(inferFromAncestors([][]string{{"/bin/zsh"}, {"aider"}, {"copilot"}}))
	if want := (&wantAgent{Name: "aider", Evidence: "inferred", Confidence: "low", Signals: []string{}}); !reflect.DeepEqual(got, want) {
		t.Errorf("inferFromAncestors = %+v", got)
	}
	if inferFromAncestors([][]string{{"/bin/zsh"}, {"/sbin/launchd"}}) != nil {
		t.Error("inferred an agent from a shell")
	}
	for env, want := range map[string]bool{"CI": true, "GITLAB_CI": true, "BUILD_NUMBER": true} {
		if isCI(map[string]string{env: "1"}) != want {
			t.Errorf("isCI(%s)", env)
		}
	}
	if isCI(map[string]string{"CI": "false"}) || isCI(map[string]string{"VERCEL": "0"}) || isCI(nil) {
		t.Error("a falsy CI variable is CI")
	}
	no := false
	if detectAgent(map[string]string{"CI": "1"}, false, &no).ci {
		t.Error("the CI override was ignored")
	}
}
