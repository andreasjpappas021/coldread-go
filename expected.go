package coldread

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Expected exits, as a CLI declares them at install (Options.Expected):
// command path → exit codes that are a normal result there, not a failure
// (gitleaks exits 1 when it finds leaks: {"detect": {1}}). As
// @coldread/agents/clean's cleanExpected, with the same golden cases
// (testdata/parity.json: cleanExpected): the root is "" or the tool's name,
// other keys go through cleanCommand, codes are 1-255, each once,
// ascending, 8 per command; places merged and sorted, 32 at most, 64 codes
// in all. nil when nothing is left.
const (
	maxExpectedPlaces = 32
	maxExpectedExits  = 8
	maxExpectedTotal  = 64
)

func cleanExpected(expected map[string][]int, tool string) map[string][]int {
	if len(expected) == 0 {
		return nil
	}
	merged := map[string]map[int]bool{}
	for raw, codes := range expected {
		k := strings.TrimSpace(raw)
		place := tool
		if k != "" && k != tool {
			place = cleanCommand(k)
		}
		if place == "" {
			continue
		}
		for _, c := range codes {
			if c >= 1 && c <= 255 {
				if merged[place] == nil {
					merged[place] = map[int]bool{}
				}
				merged[place][c] = true
			}
		}
	}
	places := make([]string, 0, len(merged))
	for p := range merged {
		places = append(places, p)
	}
	sort.Strings(places)
	if len(places) > maxExpectedPlaces {
		places = places[:maxExpectedPlaces]
	}
	out := map[string][]int{}
	total := 0
	for _, p := range places {
		codes := make([]int, 0, len(merged[p]))
		for c := range merged[p] {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		limit := maxExpectedExits
		if maxExpectedTotal-total < limit {
			limit = maxExpectedTotal - total
		}
		if len(codes) > limit {
			codes = codes[:limit]
		}
		if len(codes) == 0 {
			break
		}
		out[p] = codes
		total += len(codes)
	}
	if total == 0 {
		return nil
	}
	return out
}

// marshalExpected: JSON with sorted keys and no spaces, as the Node and
// Python SDKs write it (encoding/json sorts map keys).
func marshalExpected(expected map[string][]int) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(expected)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// rulesHash: what a declaration is, for the cache that sends it once per
// change; the same as @coldread/cli's rulesHash.
func rulesHash(key string, expected map[string][]int) string {
	sum := sha256.Sum256([]byte("coldread:rules:" + key + ":" + string(marshalExpected(expected))))
	return hex.EncodeToString(sum[:])[:16]
}

// rulesRecord is a rules record in a POST /api/ingest batch: the exits
// declared normal, stored as the site's expected outcomes ("exit N" at
// that command), the rule an owner sets with "Not a dead end".
type rulesRecord struct {
	Source   string          `json:"source"`
	TS       int64           `json:"ts"`
	Tool     wireTool        `json:"tool"`
	Expected json.RawMessage `json:"expected"`
}

// rulesToSend: the expected exits, when this run should carry them: once
// per change (a hash of key and declaration in the cache, `rules`; where
// the cache can't be written, every run, and ingest ignores the repeats),
// always under COLDREAD_VERIFY so the check says what came back. Handed to
// the send, it's as good as sent: what isn't sent is spooled with the run.
func (c *Client) rulesToSend() []byte {
	if c.expected == nil {
		return nil
	}
	hash := rulesHash(c.key, c.expected)
	file := filepath.Join(c.cacheDir, "rules")
	sent, _ := os.ReadFile(file)
	if strings.TrimSpace(string(sent)) == hash && !c.verifyMode {
		return nil
	}
	if !c.debug && os.MkdirAll(c.cacheDir, 0o700) == nil {
		_ = os.WriteFile(file, []byte(hash+"\n"), 0o600)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(rulesRecord{Source: "rules", TS: c.started.UnixMilli(), Tool: wireTool{Name: c.tool, Version: strOrNil(c.version)}, Expected: marshalExpected(c.expected)})
	return bytes.TrimRight(b.Bytes(), "\n")
}
