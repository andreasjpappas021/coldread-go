package coldread

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
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

// blackHole: an https endpoint whose TLS handshake never completes (it
// accepts and says nothing), like a network that swallows packets: no
// connection to Coldread ever opens.
func blackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return "https://" + ln.Addr().String() + "/api/ingest"
}

// A network that swallows packets costs one command the wait, not every
// command: after a timeout that never connected, runs spool without
// waiting, while their startup send keeps probing; the first connection
// clears the mark and that run sends at exit as usual. A slow answer over a
// connection that did open is latency, and leaves no mark.
func TestAfterATimeoutRunsDontWait(t *testing.T) {
	in := newIngest(t)
	hole := blackHole(t)
	cache := t.TempDir()
	finish := func(endpoint, command string, beforeFinish func(*run)) time.Duration {
		r := start(t, setup{cache: cache, opts: Options{Endpoint: endpoint}})
		if beforeFinish != nil {
			beforeFinish(r)
		}
		r.c.Track(command)
		t0 := time.Now()
		r.c.Finish(0)
		return time.Since(t0)
	}
	mark := filepath.Join(cache, "backoff")

	// Latency (it connects, then answers late): one wait, no mark.
	in.mu.Lock()
	in.delay = time.Second
	in.mu.Unlock()
	if took := finish(in.endpoint(), "slow", nil); took < ExitWait-50*time.Millisecond {
		t.Fatalf("first run took %v", took)
	}
	if _, err := os.Stat(mark); err == nil {
		t.Fatal("a slow answer marked the network as dropping packets")
	}
	in.mu.Lock()
	in.delay = 0
	in.mu.Unlock()

	if took := finish(hole, "one", nil); took < ExitWait-50*time.Millisecond {
		t.Fatalf("black-holed run took %v", took)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Fatal("no backoff mark after a timeout that never connected")
	}
	// The next run's startup send is still hanging when it finishes: its
	// events go back, and it doesn't wait (but for its probe's moment).
	if took := finish(hole, "two", nil); took > probeWait+70*time.Millisecond {
		t.Fatalf("a run after a timeout took %v", took)
	}
	got := spooled(t, cache)
	if len(got) != 3 {
		t.Fatalf("spool %v", got)
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*.sending")); len(left) != 0 {
		t.Fatalf("claims left: %v", left)
	}
	// The network works again: the run's own startup connection (the probe)
	// clears the mark, and that same run sends at exit as usual, however
	// fast it is.
	before := len(in.posts())
	finish(in.endpoint(), "three", func(r *run) {
		deadline := time.Now().Add(2 * time.Second)
		for !r.c.s.reached.Load() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	})
	if _, err := os.Stat(mark); err == nil {
		t.Fatal("mark kept after a connection opened")
	}
	if len(in.posts()) < before+1 {
		t.Fatal("not sent at exit once the mark was cleared")
	}
	// A fast run that starts backed off, on a network that works again: its
	// probe connects within the exit's moment, the mark goes, and it sends.
	_ = os.WriteFile(mark, nil, 0o600)
	before = len(in.posts())
	finish(in.endpoint(), "fast", nil)
	if _, err := os.Stat(mark); err == nil {
		t.Fatal("a fast run left the mark on a working network")
	}
	if len(in.posts()) < before+1 {
		t.Fatal("the fast run wasn't sent at exit")
	}
	finish(in.endpoint(), "four", nil)
	cmds := strings.Join(in.commands(), ",")
	for _, c := range []string{"slow", "one", "two", "three", "four"} {
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
	// A fresh claim by a live process (this test's parent) is left alone.
	fresh := spool + "." + itoa(os.Getppid()) + ".1.sending"
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

// A server killed mid-send (Codex: stdin EOF, then SIGTERM a moment later)
// leaves a fresh claim; its process is gone, so the next start takes it at
// once rather than 60 s later.
func TestAFreshClaimOfADeadProcessIsTakenAtOnce(t *testing.T) {
	in := newIngest(t)
	cache := t.TempDir()
	spool := filepath.Join(cache, "spool.jsonl")
	dead := exec.Command("true")
	if err := dead.Run(); err != nil {
		t.Skip("no true(1)")
	}
	stranded := spool + "." + itoa(dead.Process.Pid) + ".1.sending"
	toSpool(stranded, [][]byte{rec("stranded", time.Now(), "")}, time.Now())
	r := start(t, setup{cache: cache, opts: Options{Endpoint: in.endpoint()}})
	<-r.c.drained
	if got := in.commands(); len(got) != 1 || got[0] != "stranded" {
		t.Fatalf("sent %v", got)
	}
	if _, err := os.Stat(stranded); err == nil {
		t.Fatal("claim left behind")
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

// codexEnv: Codex's default sandbox, which has no network.
func codexEnv() map[string]string {
	return map[string]string{"CODEX_THREAD_ID": "019a-thread", "CODEX_SANDBOX_NETWORK_DISABLED": "1", "HOME": "/home/x"}
}

// readOnly makes dir unwritable for the test (Codex's Seatbelt blocks
// writes outside the workspace and the temp folder).
func readOnly(t *testing.T, dir string) {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("root writes anywhere")
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
}

func spooledAt(t *testing.T, file string) []string {
	t.Helper()
	var out []string
	for _, r := range readSpool(file, time.Now()) {
		var x struct{ Command string }
		_ = json.Unmarshal(r, &x)
		out = append(out, x.Command)
	}
	return out
}

// The cache can't be written (Codex's sandbox): the event waits in the
// temp folder, and the next run with network sends it from there.
func TestSandboxSpoolsToTempAndTheNextRunSendsIt(t *testing.T) {
	in := newIngest(t)
	cache, tmp := t.TempDir(), t.TempDir()
	readOnly(t, cache)
	r := start(t, setup{env: codexEnv(), cache: cache, tmp: tmp, opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("build")
	r.c.Finish(0)
	alt := tmpSpoolFor("acme", tmp)
	if got := spooledAt(t, alt); len(got) != 1 || got[0] != "build" {
		t.Fatalf("temp spool %v", got)
	}
	if st, err := os.Stat(tmpTop(alt)); err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("temp folder %v %v", st, err)
	}
	if len(in.posts()) != 0 {
		t.Fatal("sent without network")
	}
	// Later, with network and a writable cache: both spools go.
	_ = os.Chmod(cache, 0o700)
	if err := toSpool(filepath.Join(cache, "spool.jsonl"), [][]byte{rec("cached", time.Now(), "")}, time.Now()); err != nil {
		t.Fatal(err)
	}
	r2 := start(t, setup{cache: cache, tmp: tmp, opts: Options{Endpoint: in.endpoint()}})
	<-r2.c.drained
	r2.c.Track("serve")
	r2.c.Finish(0)
	got := in.commands()
	if len(got) != 3 || !contains(got, "build") || !contains(got, "cached") || !contains(got, "serve") {
		t.Fatalf("sent %v", got)
	}
	if spooledAt(t, alt) != nil || spooled(t, cache) != nil {
		t.Fatal("spool left")
	}
}

// COLDREAD_VERIFY says where the event waits, or why it couldn't.
func TestSandboxVerifySaysWhere(t *testing.T) {
	in := newIngest(t)
	cache, tmp := t.TempDir(), t.TempDir()
	readOnly(t, cache)
	r := start(t, setup{env: with(codexEnv(), "COLDREAD_VERIFY", "1"), cache: cache, tmp: tmp, opts: Options{Endpoint: in.endpoint()}})
	r.c.Track("build")
	r.c.Finish(0)
	want := "[coldread] verify: no network here; saved for the next run with network (" + tmpSpoolFor("acme", tmp) + ").\n"
	if got := r.lines(); len(got) != 1 || got[0] != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Neither folder writable: it says so.
	tmp2 := t.TempDir()
	readOnly(t, tmp2)
	r2 := start(t, setup{env: with(codexEnv(), "COLDREAD_VERIFY", "1"), cache: cache, tmp: tmp2, opts: Options{Endpoint: in.endpoint()}})
	r2.c.Track("build")
	r2.c.Finish(0)
	if got := r2.lines(); len(got) != 1 || got[0] != "[coldread] verify: no network here; not saved (EACCES), so this event is lost.\n" {
		t.Fatalf("got %q", got)
	}
}

// Someone else's folder in a shared /tmp is never written or read.
func TestTempSpoolOnlyInAPrivateFolder(t *testing.T) {
	if os.Getuid() < 0 {
		t.Skip("no uids")
	}
	cache, tmp := t.TempDir(), t.TempDir()
	readOnly(t, cache)
	alt := tmpSpoolFor("acme", tmp)
	if err := os.MkdirAll(filepath.Dir(alt), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmpTop(alt), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(alt, append(rec("planted", time.Now(), ""), '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	r := start(t, setup{env: with(codexEnv(), "COLDREAD_VERIFY", "1"), cache: cache, tmp: tmp})
	r.c.Track("build")
	r.c.Finish(0)
	if got := spooledAt(t, alt); len(got) != 1 || got[0] != "planted" {
		t.Fatalf("wrote to a shared folder: %v", got)
	}
	if got := r.lines(); len(got) != 1 || !strings.Contains(got[0], "not saved") {
		t.Fatalf("%q", got)
	}
	in := newIngest(t)
	_ = os.Chmod(cache, 0o700)
	r2 := start(t, setup{cache: cache, tmp: tmp, opts: Options{Endpoint: in.endpoint()}})
	<-r2.c.drained
	r2.c.Track("serve")
	r2.c.Finish(0)
	if got := in.commands(); len(got) != 1 || got[0] != "serve" {
		t.Fatalf("read a shared folder: %v", got)
	}
}
