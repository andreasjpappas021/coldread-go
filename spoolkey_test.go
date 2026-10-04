package coldread

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// A spooled event goes only with the key it was made with, and keeps its
// id, so ingest stores a resend once. The same rules as @coldread/cli's.

func TestSpooledEventsKeepTheirKey(t *testing.T) {
	in := newIngest(t)
	other := "cr_pub_" + "b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0"
	spool := filepath.Join(t.TempDir(), "spool.jsonl")
	rec := func(cmd string) []byte {
		return []byte(`{"source":"cli","ts":` + itoa(int(time.Now().UnixMilli())) + `,"tool":{"name":"acme"},"command":"` + cmd + `","id":"0123456789abcdef0123456789abcd` + cmd[:2] + `"}`)
	}
	old := newSender(in.endpoint(), other, "coldread-go/test", spool, time.Now)
	if _, err := old.save([][]byte{rec("old site")}); err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	_ = json.Unmarshal(readSpool(spool, time.Now())[0], &line)
	if line["k"] != other {
		t.Fatalf("spooled without its key: %v", line)
	}

	// A run with another key sends its own event, never this one.
	now := newSender(in.endpoint(), testKey, "coldread-go/test", spool, time.Now)
	now.sendOne(rec("new site"), time.Second)
	now.drain()
	for _, p := range in.posts() {
		for _, r := range p.body.Records {
			if bytes.Contains(r, []byte(`"old site"`)) {
				t.Fatalf("sent with %s: %s", p.auth, r)
			}
			if bytes.Contains(r, []byte(`"k"`)) {
				t.Fatalf("k left the machine: %s", r)
			}
		}
	}
	if got := spooledAt(t, spool); len(got) != 1 || got[0] != "old site" {
		t.Fatalf("spool %v", got)
	}

	// Its own key's next run sends it, with its id.
	again := newSender(in.endpoint(), other, "coldread-go/test", spool, time.Now)
	again.drain()
	p := in.posts()
	last := p[len(p)-1]
	if last.auth != "Bearer "+other || len(last.body.Records) != 1 || !bytes.Contains(last.body.Records[0], []byte(`"id":"0123456789abcdef0123456789abcdol"`)) || bytes.Contains(last.body.Records[0], []byte(`"k"`)) {
		t.Fatalf("sent %s %s", last.auth, last.body.Records)
	}
	if got := spooledAt(t, spool); len(got) != 0 {
		t.Fatalf("spool left %v", got)
	}
}
