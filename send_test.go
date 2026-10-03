package coldread

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func rec(command string, ts time.Time, pad string) []byte {
	if pad != "" {
		command += " " + pad
	}
	return []byte(fmt.Sprintf(`{"source":"cli","ts":%d,"tool":{"name":"acme","version":"1.0.0"},"command":%q,"flags":[],"exit":0,"durationMs":1,"agent":null,"os":"darwin","arch":"arm64","runtime":"go/1.26.2"}`, ts.UnixMilli(), command))
}

func TestSpoolsWhenDownAndSendsWithTheNextRun(t *testing.T) {
	in := newIngest(t)
	in.set(503, `{}`)
	cache := t.TempDir()
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r.c.drained
	r.c.Track("one")
	r.c.Finish(0)
	if got := spooled(t, cache); len(got) != 1 || got[0] != "one" {
		t.Fatalf("spool %v", got)
	}
	in.set(202, `{"accepted":1}`)
	r2 := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r2.c.drained // the spool goes at startup, in the background
	r2.c.Track("two")
	r2.c.Finish(0)
	if got := in.commands(); strings.Join(got, ",") != "one,one,two" {
		t.Fatalf("sent %v", got)
	}
	if spooled(t, cache) != nil {
		t.Fatal("spool left")
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*")); len(left) != 0 {
		t.Fatalf("files left: %v", left)
	}
}

func TestRetriesAndDrops(t *testing.T) {
	in := newIngest(t)
	in.set(429, `{"error":"slow down"}`)
	a := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	a.c.Track("limited")
	a.c.Finish(0)
	if got := spooled(t, a.cache); len(got) != 1 {
		t.Fatalf("429: %v", got)
	}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := "http://" + l.Addr().String() + "/api/ingest"
	l.Close()
	b := start(t, setup{opts: Options{Endpoint: closed}})
	b.c.Track("offline")
	b.c.Finish(0)
	if got := spooled(t, b.cache); len(got) != 1 {
		t.Fatalf("refused: %v", got)
	}
	in.set(400, `{"error":"records: must be a non-empty array."}`)
	c := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	c.c.Track("refused")
	c.c.Finish(0)
	if got := spooled(t, c.cache); got != nil {
		t.Fatalf("400 kept: %v", got)
	}
}

// The CLI never waits more than ExitWait on Coldread.
func TestAServerThatHangsCostsExitWaitAtMost(t *testing.T) {
	in := newIngest(t)
	in.delay = time.Second
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("slow")
	t0 := time.Now()
	r.c.Finish(0)
	took := time.Since(t0)
	if took > ExitWait+100*time.Millisecond {
		t.Fatalf("Finish took %v", took)
	}
	if got := spooled(t, r.cache); len(got) != 1 || got[0] != "slow" {
		t.Fatalf("spool %v", got)
	}
}

// A network that swallows packets costs one command the wait, not every
// command: after a timeout, runs spool without waiting until an answer comes.
func TestAfterATimeoutRunsDontWait(t *testing.T) {
	in := newIngest(t)
	in.delay = time.Second
	cache := t.TempDir()
	finish := func(command string, beforeFinish func(*run)) time.Duration {
		r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
		if beforeFinish != nil {
			beforeFinish(r)
		}
		r.c.Track(command)
		t0 := time.Now()
		r.c.Finish(0)
		return time.Since(t0)
	}
	if took := finish("one", nil); took < ExitWait-50*time.Millisecond {
		t.Fatalf("first run took %v", took)
	}
	if _, err := os.Stat(filepath.Join(cache, "backoff")); err != nil {
		t.Fatal("no backoff mark after a timeout")
	}
	// The next run's startup send is still hanging when it finishes: its
	// events go back, and it doesn't wait.
	if took := finish("two", nil); took > 50*time.Millisecond {
		t.Fatalf("a run after a timeout took %v", took)
	}
	got := spooled(t, cache)
	if len(got) != 2 {
		t.Fatalf("spool %v", got)
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*.sending")); len(left) != 0 {
		t.Fatalf("claims left: %v", left)
	}
	// Coldread answers again: a run whose startup send gets through clears
	// the mark, and the one after sends at exit as usual.
	in.mu.Lock()
	in.delay = 0
	in.mu.Unlock()
	finish("three", func(r *run) { <-r.c.drained })
	if _, err := os.Stat(filepath.Join(cache, "backoff")); err == nil {
		t.Fatal("mark kept after an answer")
	}
	before := len(in.posts())
	finish("four", nil)
	if len(in.posts()) != before+1 {
		t.Fatal("not sent at exit once the mark was cleared")
	}
	cmds := strings.Join(in.commands(), ",")
	for _, c := range []string{"one", "two", "three", "four"} {
		if !strings.Contains(cmds, c) {
			t.Errorf("%s never sent: %s", c, cmds)
		}
	}
}

func TestAWorkingServerIsQuick(t *testing.T) {
	in := newIngest(t)
	r := start(t, setup{opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("fast")
	t0 := time.Now()
	r.c.Finish(0)
	if took := time.Since(t0); took > 100*time.Millisecond {
		t.Fatalf("Finish took %v against a local server", took)
	}
	if len(in.posts()) != 1 || spooled(t, r.cache) != nil {
		t.Fatal("not sent")
	}
}

func TestOnePostIsAtMost8KB(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	spool := filepath.Join(cache, "spool.jsonl")
	var old [][]byte
	for i := 0; i < 40; i++ {
		old = append(old, rec(fmt.Sprintf("old%d", i), time.Now().Add(time.Duration(i-1000)*time.Millisecond), strings.Repeat("p", 100)))
	}
	toSpool(spool, old, time.Now())
	r := start(t, setup{cache: cache, env: with(claudeEnv(), "COLDREAD_VERIFY", "1"), opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("now")
	r.c.Finish(0)
	sent := in.posts()[0]
	var raw [][]byte
	for _, r := range sent.body.Records {
		raw = append(raw, r)
	}
	if n := len(batchBody(raw)); n > maxPostBytes {
		t.Fatalf("POST of %d bytes", n)
	}
	cmds := in.commands()
	if cmds[0] != "now" || !strings.HasPrefix(cmds[1], "old0 ") {
		t.Fatalf("order %v", cmds[:2])
	}
	left := spooled(t, cache)
	if len(left) == 0 || len(left) != 41-len(sent.body.Records) {
		t.Fatalf("%d left, %d sent", len(left), len(sent.body.Records))
	}
}

func TestSpoolCaps(t *testing.T) {
	cache := t.TempDir()
	spool := filepath.Join(cache, "acme", "spool.jsonl")
	records := [][]byte{rec("ancient", time.Now().Add(-maxSpoolAge-time.Second), "")}
	for i := 0; i < 120; i++ {
		records = append(records, rec(fmt.Sprintf("e%d", i), time.Now(), ""))
	}
	toSpool(spool, records, time.Now())
	left := readSpool(spool, time.Now())
	st, _ := os.Stat(spool)
	if len(left) > maxSpoolEvents || !strings.Contains(string(left[len(left)-1]), `"e119"`) || st.Size() > maxSpoolBytes {
		t.Fatalf("%d kept, %d bytes", len(left), st.Size())
	}
	for _, l := range left {
		if strings.Contains(string(l), "ancient") {
			t.Fatal("kept a week-old event")
		}
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("spool mode %v", st.Mode().Perm())
	}
	big := filepath.Join(cache, "big", "spool.jsonl")
	for i := 0; i < 90; i++ {
		toSpool(big, [][]byte{rec(fmt.Sprintf("big%d", i), time.Now(), strings.Repeat("x", 120))}, time.Now())
	}
	st, _ = os.Stat(big)
	all := readSpool(big, time.Now())
	if st.Size() > maxSpoolBytes || !strings.Contains(string(all[len(all)-1]), `"big89 `) {
		t.Fatalf("%d bytes", st.Size())
	}
}

func TestTwoRunsAtOnceNeverBothSendTheSpool(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	toSpool(filepath.Join(cache, "spool.jsonl"), [][]byte{rec("queued", time.Now(), "")}, time.Now())
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
			<-r.c.drained
			r.c.Track(name)
			r.c.Finish(0)
		}(name)
	}
	wg.Wait()
	n := 0
	for _, c := range in.commands() {
		if c == "queued" {
			n++
		}
	}
	if n != 1 || len(in.commands()) != 3 {
		t.Fatalf("sent %v", in.commands())
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*")); len(left) != 0 {
		t.Fatalf("files left: %v", left)
	}
}

func TestAClaimLeftByAKilledRunIsPickedUp(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	spool := filepath.Join(cache, "spool.jsonl")
	orphan := spool + ".999.1.sending"
	toSpool(orphan, [][]byte{rec("orphan", time.Now(), "")}, time.Now())
	fresh := spool + ".998.1.sending"
	toSpool(fresh, [][]byte{rec("in-flight", time.Now(), "")}, time.Now())
	old := time.Now().Add(-2 * staleClaim)
	_ = os.Chtimes(orphan, old, old)
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r.c.drained
	if got := in.commands(); len(got) != 1 || got[0] != "orphan" {
		t.Fatalf("sent %v", got)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("took another run's claim")
	}
}

func TestJunkInTheSpoolIsSkipped(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	spool := filepath.Join(cache, "spool.jsonl")
	toSpool(spool, [][]byte{rec("good", time.Now(), "")}, time.Now())
	f, _ := os.OpenFile(spool, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("not json\n{\"no\":\"ts\"}\n[1]\n{\"ts\":\"1\"}\n")
	f.Close()
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r.c.drained
	if got := in.commands(); len(got) != 1 || got[0] != "good" {
		t.Fatalf("sent %v", got)
	}
}

// A spool @coldread/cli wrote (a Node tool with the same name) is sent as
// is, fields this SDK doesn't write included.
func TestNodeSpoolRoundTrips(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	node := fmt.Sprintf(`{"source":"mcp","ts":%d,"tool":{"name":"acme","version":"1.0.0"},"command":"search_docs","flags":[],"exit":0,"durationMs":3,"agent":null,"client":{"name":"codex-mcp-client","version":"0.51.0"},"os":"linux","arch":"x64","runtime":"node/22.3.0"}`, time.Now().UnixMilli())
	_ = os.WriteFile(filepath.Join(cache, "spool.jsonl"), []byte(node+"\n"), 0o600)
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r.c.drained
	if got := string(in.posts()[0].body.Records[0]); got != node {
		t.Fatalf("%s", got)
	}
}

func TestWhyNamesErrors(t *testing.T) {
	s := newSender("http://127.0.0.1:1/api/ingest", testKey, "t", filepath.Join(t.TempDir(), "s"), time.Now)
	_, err := s.post(context.Background(), [][]byte{rec("x", time.Now(), "")})
	if err == nil || why(err) != "ECONNREFUSED" {
		t.Fatalf("%v → %q", err, why(err))
	}
	s = newSender("http://coldread-does-not-exist.invalid/api/ingest", testKey, "t", filepath.Join(t.TempDir(), "s"), time.Now)
	if _, err = s.post(context.Background(), [][]byte{rec("x", time.Now(), "")}); err == nil || why(err) != "ENOTFOUND" {
		t.Fatalf("%v → %q", err, why(err))
	}
}
