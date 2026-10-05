package coldread

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Expected exits: declared at install (Options.Expected), sent once per
// change as a rules record, as @coldread/cli's (test/expected.test.ts).

// Pinned in packages/cli/test/expected.test.ts and packages/python/tests/test_expected.py too.
const rulesHashWant = "51ac9cf80c8c562d"

func TestCleanExpectedGoldenCases(t *testing.T) {
	var cases struct {
		CleanExpected []struct {
			Tool string          `json:"tool"`
			In   json.RawMessage `json:"in"`
			Want map[string][]int
			W    json.RawMessage `json:"want"`
		} `json:"cleanExpected"`
	}
	if err := json.Unmarshal(mustRead(t, "testdata/parity.json"), &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.CleanExpected) == 0 {
		t.Fatal("no cleanExpected cases")
	}
	for _, c := range cases.CleanExpected {
		// Options.Expected is typed: what JSON has that isn't a list of
		// integers can't be passed, so it's left out here (as the others drop it).
		var raw map[string]json.RawMessage
		in := map[string][]int{}
		if json.Unmarshal(c.In, &raw) == nil {
			for k, v := range raw {
				var list []any
				if json.Unmarshal(v, &list) != nil {
					continue
				}
				codes := []int{}
				for _, x := range list {
					if f, ok := x.(float64); ok && f == float64(int(f)) {
						codes = append(codes, int(f))
					}
				}
				in[k] = codes
			}
		}
		var want map[string][]int
		_ = json.Unmarshal(c.W, &want)
		if got := cleanExpected(in, c.Tool); !reflect.DeepEqual(got, want) {
			t.Errorf("cleanExpected(%s, %q) = %v, want %v", c.In, c.Tool, got, want)
		}
	}
}

func TestRulesHashIsTheNodeAndPythonSDKs(t *testing.T) {
	if got := rulesHash(testKey, map[string][]int{"gitleaks": {1}, "detect": {1}}); got != rulesHashWant {
		t.Fatalf("rulesHash = %s, want %s", got, rulesHashWant)
	}
}

func rulesIn(in *ingest) []string {
	var out []string
	for _, g := range in.posts() {
		for _, r := range g.body.Records {
			var x struct{ Source string }
			_ = json.Unmarshal(r, &x)
			if x.Source == "rules" {
				out = append(out, string(r))
			}
		}
	}
	return out
}

func gitleaks(t *testing.T, in *ingest, cache string, expected map[string][]int, env map[string]string) *run {
	if env == nil {
		env = map[string]string{}
	}
	r := start(t, setup{env: env, cache: cache, clock: true, opts: Options{Tool: "gitleaks", Version: "8.18.0", Endpoint: in.endpoint(), Expected: expected}})
	r.c.Track("detect")
	r.c.Finish(1)
	return r
}

func TestExpectedRidesWithTheFirstRunAndNotAgain(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	gitleaks(t, in, cache, map[string][]int{"detect": {1, 1}, "": {1}, "protect --staged": {1, 300}}, nil)
	got := rulesIn(in)
	want := `{"source":"rules","ts":1000000000000,"tool":{"name":"gitleaks","version":"8.18.0"},"expected":{"detect":[1],"gitleaks":[1],"protect":[1]}}`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("rules = %v, want [%s]", got, want)
	}
	posts := in.posts()
	if len(posts) == 0 || len(posts[len(posts)-1].body.Records) != 2 {
		t.Fatalf("want the run and its rules in one POST: %+v", posts)
	}
	b, _ := os.ReadFile(filepath.Join(cache, "rules"))
	if strings.TrimSpace(string(b)) != rulesHash(testKey, map[string][]int{"detect": {1}, "gitleaks": {1}, "protect": {1}}) {
		t.Fatalf("cache = %q", b)
	}
	gitleaks(t, in, cache, map[string][]int{"protect": {1}, "detect": {1}, "gitleaks": {1}}, nil)
	if n := len(rulesIn(in)); n != 1 {
		t.Fatalf("sent again: %d", n)
	}
	gitleaks(t, in, cache, map[string][]int{"detect": {1, 2}}, nil)
	if got := rulesIn(in); len(got) != 2 || !strings.Contains(got[1], `"expected":{"detect":[1,2]}`) {
		t.Fatalf("a changed declaration goes again: %v", got)
	}
}

func TestExpectedNothingUsableNothingSent(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	gitleaks(t, in, cache, map[string][]int{"/Users/alice": {1}, "detect": {0, 256}}, nil)
	gitleaks(t, in, cache, nil, nil)
	if got := rulesIn(in); len(got) != 0 {
		t.Fatalf("rules = %v", got)
	}
	if _, err := os.Stat(filepath.Join(cache, "rules")); err == nil {
		t.Fatal("cache written with nothing to send")
	}
}

func TestExpectedUnderVerifyAlwaysGoesAndSaysSo(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	gitleaks(t, in, cache, map[string][]int{"detect": {1}}, nil)
	r := gitleaks(t, in, cache, map[string][]int{"detect": {1}, "": {2}}, map[string]string{"COLDREAD_VERIFY": "1"})
	lines := strings.Join(r.lines(), "")
	if !strings.Contains(lines, VerifyPrefix+"accepted\n"+VerifyPrefix+"expected exits accepted (detect: 1; gitleaks: 2)\n") {
		t.Fatalf("verify said %q", lines)
	}
	r = gitleaks(t, in, cache, map[string][]int{"detect": {1}, "": {2}}, map[string]string{"COLDREAD_VERIFY": "1"})
	if n := len(rulesIn(in)); n != 3 {
		t.Fatalf("verify always carries them: %d", n)
	}
	in.set(202, `{"accepted":1,"rejected":[{"i":1,"error":"expected: nothing usable"}]}`)
	r = gitleaks(t, in, cache, map[string][]int{"detect": {1}}, map[string]string{"COLDREAD_VERIFY": "1"})
	if lines := strings.Join(r.lines(), ""); !strings.Contains(lines, VerifyPrefix+"expected exits rejected (expected: nothing usable)\n") {
		t.Fatalf("verify said %q", lines)
	}
}

func TestExpectedDebugPrintsAndRemembersNothing(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	r := gitleaks(t, in, cache, map[string][]int{"detect": {1}}, map[string]string{"COLDREAD_DEBUG": "1"})
	if lines := strings.Join(r.lines(), ""); !strings.Contains(lines, `[coldread] {"source":"rules","ts":1000000000000,"tool":{"name":"gitleaks","version":"8.18.0"},"expected":{"detect":[1]}}`) {
		t.Fatalf("debug said %q", lines)
	}
	if _, err := os.Stat(filepath.Join(cache, "rules")); err == nil {
		t.Fatal("debug wrote the cache")
	}
}

func TestExpectedWaitsInTheSpoolWhenIngestIsDown(t *testing.T) {
	in := newIngest(t)
	in.set(503, `{}`)
	cache := t.TempDir()
	gitleaks(t, in, cache, map[string][]int{"detect": {1}}, nil)
	b, _ := os.ReadFile(filepath.Join(cache, "spool.jsonl"))
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"source":"rules"`) {
		t.Fatalf("spool = %v", lines)
	}
}
