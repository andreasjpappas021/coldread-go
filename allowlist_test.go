package coldread

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Agents run commands through login shells (Codex: `zsh -lc`), so whatever
// a profile exports, secrets included, reaches the CLI. A record carries
// only what the registry names: marker variables by name, and the values of
// the few that say who the agent is (AI_AGENT/AGENT, a version, a host),
// cleaned. Never any other variable's value, and never a session id (only
// its salted hash).

// registryNames: every variable the registry reads.
func registryNames() map[string]bool {
	names := map[string]bool{networkDisabledVar: true}
	for _, r := range registry.Rules {
		for _, m := range r.Markers {
			names[m.Env] = true
		}
		for _, n := range append(append([]string{r.VersionFromAgentID, r.VersionEnv}, r.Session...), r.Signals...) {
			if n != "" {
				names[n] = true
			}
		}
		if r.Host != nil {
			names[r.Host.Env] = true
		}
	}
	for _, n := range append(append([]string{}, registry.GenericEnv...), registry.CIVars...) {
		names[n] = true
	}
	return names
}

// valueNames: the registry variables whose values may appear, cleaned.
func valueNames() map[string]bool {
	names := map[string]bool{}
	for _, n := range registry.GenericEnv {
		names[n] = true
	}
	for _, r := range registry.Rules {
		for _, n := range []string{r.VersionFromAgentID, r.VersionEnv} {
			if n != "" {
				names[n] = true
			}
		}
		if r.Host != nil {
			names[r.Host.Env] = true
		}
	}
	return names
}

var secretNames = []string{
	"AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "GITHUB_TOKEN", "GH_TOKEN", "NPM_TOKEN", "OPENAI_API_KEY",
	"ANTHROPIC_API_KEY", "DATABASE_URL", "STRIPE_SECRET_KEY", "COLDREAD_KEY", "SSH_AUTH_SOCK", "HOME", "USER",
	"LOGNAME", "PWD", "OLDPWD", "HOSTNAME", "PATH", "SHELL", "TERM_SESSION_ID", "TMPDIR", "EDITOR", "LANG",
	"KUBECONFIG", "VAULT_TOKEN", "SENTRY_DSN", "PGPASSWORD", "HISTFILE", "MY_APP_PASSWORD", "SLACK_WEBHOOK_URL",
}

func TestRecordCarriesNoOtherEnvValues(t *testing.T) {
	all := registryNames()
	values := valueNames()
	var names []string
	for n := range all {
		names = append(names, n)
	}
	sort.Strings(names)

	// Every variable gets a value of its own that would survive cleaning
	// (letters and digits), so a leak shows up wherever it lands.
	sentinel := map[string]string{}
	for i, n := range append(append([]string{}, secretNames...), names...) {
		sentinel[n] = fmt.Sprintf("zq%dq", i)
	}

	type tc struct{ label, name, value string }
	var cases []tc
	for _, r := range registry.Rules {
		for _, m := range r.Markers {
			v := "1"
			switch m.When {
			case "equals":
				v = m.Value
			case "set":
				v = sentinel[m.Env]
			}
			cases = append(cases, tc{r.Name, m.Env, v})
		}
	}
	for _, n := range registry.GenericEnv {
		cases = append(cases, tc{"generic", n, sentinel[n]})
	}
	if len(cases) < 5 {
		t.Fatalf("registry has %d markers", len(cases))
	}

	for _, c := range cases {
		env := map[string]string{}
		for n, v := range sentinel {
			env[n] = v
		}
		// Falsy, so only the marker under test (and what it reads) counts.
		for n := range all {
			if !values[n] {
				env[n] = "0"
			}
		}
		for _, r := range registry.Rules {
			for _, s := range r.Session {
				env[s] = sentinel[s]
			}
		}
		for _, n := range registry.GenericEnv {
			if n != c.name {
				delete(env, n)
			}
		}
		env[c.name] = c.value
		cl := newClient(Options{Key: testKey, Tool: "acme", Version: "1.0.0"}, internals{
			env: env, isTTY: bptr(false), stderrTTY: bptr(false), cacheDir: t.TempDir(), tmpDir: t.TempDir(),
			testRun: bptr(false), write: func(string) {},
		})
		if cl.Agent() == nil {
			t.Errorf("%s (%s=%s): no agent detected", c.label, c.name, c.value)
			continue
		}
		rec := string(cl.record("deploy", []string{"--prod"}, 0))
		for n, v := range sentinel {
			if strings.Contains(rec, v) && !(values[n] && all[n]) {
				t.Errorf("%s (%s): the record carries %s's value: %s", c.label, c.name, n, rec)
			}
		}
		var parsed struct {
			Agent struct{ Signals []string }
		}
		if err := json.Unmarshal([]byte(rec), &parsed); err != nil {
			t.Fatal(err)
		}
		for _, s := range parsed.Agent.Signals {
			if !all[s] {
				t.Errorf("%s: signal %q isn't a registry variable", c.label, s)
			}
		}
	}
}
