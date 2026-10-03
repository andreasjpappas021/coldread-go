package coldread

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"regexp"
	"strings"
)

// registry.json is @coldread/agents' registry (packages/agents/src/
// registry.json), the same file the TypeScript detector and the Python SDK
// read. It is a copy so this module builds on its own; registry_test.go and
// packages/agents/test/parity.test.ts fail when it differs from the source.
//
//go:generate cp ../agents/src/registry.json registry.json
//go:generate cp ../agents/test/parity.json testdata/parity.json

//go:embed registry.json
var registryJSON []byte

type marker struct {
	Env        string `json:"env"`
	When       string `json:"when"`
	Value      string `json:"value"`
	Confidence string `json:"confidence"`
}

type hostRule struct {
	Env   string            `json:"env"`
	Exact map[string]string `json:"exact"`
	// In the file's order, as JavaScript reads an object's keys.
	Prefix orderedMap `json:"prefix"`
}

type agentRule struct {
	Name               string    `json:"name"`
	Markers            []marker  `json:"markers"`
	VersionFromAgentID string    `json:"versionFromAgentId"`
	VersionEnv         string    `json:"versionEnv"`
	Host               *hostRule `json:"host"`
	Session            []string  `json:"session"`
	Signals            []string  `json:"signals"`
}

type processRule struct {
	Pattern string `json:"pattern"`
	Flags   string `json:"flags"`
	Agent   string `json:"agent"`
	re      *regexp.Regexp
}

type registryData struct {
	Aliases      map[string]string `json:"aliases"`
	Rules        []agentRule       `json:"rules"`
	GenericEnv   []string          `json:"genericEnv"`
	ProcessNames []processRule     `json:"processNames"`
	CIVars       []string          `json:"ciVars"`
}

var registry = loadRegistry(registryJSON)

func loadRegistry(data []byte) *registryData {
	r := &registryData{}
	if err := json.Unmarshal(data, r); err != nil {
		// Only a broken build gets here: detect nothing rather than panic.
		return &registryData{}
	}
	kept := r.ProcessNames[:0]
	for _, p := range r.ProcessNames {
		// JavaScript's flags as RE2's: only i is used, and the patterns are
		// plain enough for both engines (registry_test.go compiles them all).
		expr := p.Pattern
		if strings.Contains(p.Flags, "i") {
			expr = "(?i)" + expr
		}
		if re, err := regexp.Compile(expr); err == nil {
			p.re = re
			kept = append(kept, p)
		}
	}
	r.ProcessNames = kept
	return r
}

// orderedMap is a JSON object's pairs in the file's order.
type orderedMap [][2]string

func (m *orderedMap) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if _, err := dec.Token(); err != nil { // {
		return err
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return err
		}
		var v string
		if err := dec.Decode(&v); err != nil {
			return err
		}
		key, _ := k.(string)
		*m = append(*m, [2]string{key, v})
	}
	return nil
}
