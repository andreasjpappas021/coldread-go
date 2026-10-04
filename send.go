package coldread

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// How events leave the machine. Go has no detached child to hand them to,
// so the send happens in this process, and the CLI waits for it at most
// ExitWait:
//
//  1. At startup, in the background: connect to Coldread (TCP and TLS, so
//     the send at exit is one round trip), and send what earlier runs left
//     in the spool.
//  2. At exit (Finish or Exit): POST this run's event, waiting up to
//     ExitWait. Sent, or refused for good (a 4xx but 429): done. Not
//     answered in time, a network error, 429 or 5xx: the event goes to the
//     spool, and the next run sends it in the background. The startup send
//     gets what's left of the same wait; unfinished, its events go back.
//  3. A send that timed out without ever reaching Coldread (no connection
//     opened in this run) leaves a mark (<cache>/backoff) for 10 minutes: a
//     network that drops packets would otherwise cost every command the
//     whole wait. A slow answer over a connection that did open is just
//     latency (120 ms away, a cold TLS send takes longer than the wait), and
//     leaves no mark. Marked, runs spool at exit without waiting; the
//     background send at startup still runs, and its connection is the
//     probe: once one opens, or Coldread answers anything, the mark goes and
//     that same run waits at exit as usual.
//
// The spool is @coldread/cli's, same file and format: one JSON event per
// line in <cache>/spool.jsonl, the newest 100 within 64 KB (past that, the
// oldest go first), nothing older than 7 days. One POST is at most 8 KB. Where the cache can't be written
// (Codex's sandbox allows only the workspace and the temp folder), events
// wait in the temp folder instead (<tmp>/coldread-<uid>/<tool>/spool.jsonl),
// and every send takes them from both.

const (
	maxPostBytes   = 8192
	maxSpoolEvents = 100
	maxSpoolBytes  = 65536
	maxSpoolAge    = 7 * 24 * time.Hour
	sendTimeout    = 2 * time.Second
	// A claimed spool left by a run that exited mid-send is picked up again
	// after this long.
	staleClaim = 60 * time.Second
	// A connection opened at startup is used for the send only while fresh.
	warmFor = 20 * time.Second
	// After a send times out, runs don't wait at exit for this long.
	backoffFor = 10 * time.Minute
)

// ExitWait is the longest Finish and Exit wait for the event to be sent.
// Past it, the event waits in the spool for the next run.
const ExitWait = 300 * time.Millisecond

// The prefix of every COLDREAD_VERIFY line, and the line that means the
// event arrived. The same as @coldread/cli's.
const (
	VerifyPrefix   = "[coldread] verify: "
	VerifyAccepted = "[coldread] verify: accepted"
)

type sender struct {
	endpoint string
	key      string
	ua       string
	spool    string
	// alt is the temp folder's spool, for when spool's folder can't be
	// written. "" for none.
	alt     string
	backoff string // "" for an MCP server, which never waits at exit
	now     func() time.Time
	client  *http.Client
	// reached: a connection to Coldread opened in this run. A timeout after
	// that is latency, not a network that drops packets.
	reached atomic.Bool

	// The startup send: what it claimed, and whether it (or Finish, handing
	// its events back) has settled them.
	drainMu      sync.Mutex
	drainDone    bool
	drainClaimed []string
	drainRecords [][]byte

	warmMu    sync.Mutex
	warmAddr  string
	warmReady chan struct{}
	warmConn  net.Conn
	warmAt    time.Time
	warmUsed  bool
}

func newSender(endpoint, key, ua, spool string, now func() time.Time) *sender {
	s := &sender{endpoint: endpoint, key: key, ua: ua, spool: spool, backoff: filepath.Join(filepath.Dir(spool), "backoff"), now: now}
	dialer := &net.Dialer{Timeout: sendTimeout, KeepAlive: 30 * time.Second}
	tlsDialer := &tls.Dialer{NetDialer: dialer, Config: &tls.Config{NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if c := s.takeWarm(ctx, addr); c != nil {
				return c, nil
			}
			return s.connected(dialer.DialContext(ctx, network, addr))
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if c := s.takeWarm(ctx, addr); c != nil {
				return c, nil
			}
			return s.connected(tlsDialer.DialContext(ctx, network, addr))
		},
		TLSHandshakeTimeout: sendTimeout,
		MaxIdleConns:        2,
		IdleConnTimeout:     30 * time.Second,
	}
	s.client = &http.Client{
		Transport: tr,
		// A redirect is an error, as in @coldread/cli: the key never follows one.
		CheckRedirect: func(*http.Request, []*http.Request) error { return errRedirect },
	}
	return s
}

var errRedirect = errors.New("redirect")

// connected notes that a connection to Coldread opened: the network works,
// so any backoff mark goes.
func (s *sender) connected(c net.Conn, err error) (net.Conn, error) {
	if err == nil && !s.reached.Swap(true) {
		s.clearSlow()
	}
	return c, err
}

// prewarm opens the connection the send will use, in the background. Not
// through a proxy (the transport dials the proxy itself), and never twice.
func (s *sender) prewarm() {
	u, err := url.Parse(s.endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return
	}
	if p, err := http.ProxyFromEnvironment(&http.Request{URL: u}); err != nil || p != nil {
		return
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"https": "443", "http": "80"}[u.Scheme]
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	s.warmMu.Lock()
	if s.warmReady != nil {
		s.warmMu.Unlock()
		return
	}
	s.warmAddr, s.warmReady = addr, make(chan struct{})
	s.warmMu.Unlock()
	go func() {
		defer close(s.warmReady)
		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()
		var c net.Conn
		var err error
		if u.Scheme == "https" {
			c, err = (&tls.Dialer{NetDialer: &net.Dialer{KeepAlive: 30 * time.Second}, Config: &tls.Config{NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}}).DialContext(ctx, "tcp", addr)
		} else {
			c, err = (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext(ctx, "tcp", addr)
		}
		s.warmMu.Lock()
		if err == nil {
			s.warmConn, s.warmAt = c, s.now()
		}
		s.warmMu.Unlock()
		// The probe: connected, the network works, and the mark goes before
		// this run's exit decides whether to wait.
		_, _ = s.connected(c, err)
	}()
}

// probeWait: how long a backed-off run's exit waits for its startup probe.
// Only a network that still drops packets pays it.
const probeWait = 50 * time.Millisecond

// probeAtExit waits up to d for the startup probe to connect (which clears
// the backoff mark); a run with no probe (behind a proxy) dials once.
func (s *sender) probeAtExit(d time.Duration) {
	s.warmMu.Lock()
	ready, addr := s.warmReady, s.warmAddr
	s.warmMu.Unlock()
	if ready != nil {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ready:
		case <-t.C:
		}
		return
	}
	u, err := url.Parse(s.endpoint)
	if err != nil || u.Hostname() == "" {
		return
	}
	if addr = u.Host; u.Port() == "" {
		addr = net.JoinHostPort(u.Hostname(), map[string]string{"https": "443", "http": "80"}[u.Scheme])
	}
	if c, err := s.connected((&net.Dialer{Timeout: d}).Dial("tcp", addr)); err == nil {
		c.Close()
	}
}

// takeWarm hands the startup connection to the first request for its
// address, waiting for it if it is still being opened.
func (s *sender) takeWarm(ctx context.Context, addr string) net.Conn {
	s.warmMu.Lock()
	ready, want := s.warmReady, s.warmAddr
	s.warmMu.Unlock()
	if ready == nil || addr != want {
		return nil
	}
	select {
	case <-ready:
	case <-ctx.Done():
		return nil
	}
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.warmUsed || s.warmConn == nil {
		return nil
	}
	s.warmUsed = true
	if s.now().Sub(s.warmAt) > warmFor {
		s.warmConn.Close()
		return nil
	}
	return s.warmConn
}

type reply struct {
	status   int
	accepted int
	why      string
}

// post sends one batch. err is a network error (or a timeout); otherwise the
// reply says what ingest made of it.
func (s *sender) post(ctx context.Context, records [][]byte) (reply, error) {
	body := batchBody(records)
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.key)
	req.Header.Set("User-Agent", s.ua)
	res, err := s.client.Do(req)
	if err != nil {
		if why(err) == "timed out" && !s.reached.Load() {
			s.markSlow()
		}
		return reply{}, err
	}
	defer res.Body.Close()
	s.clearSlow()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	_, _ = io.Copy(io.Discard, res.Body)
	r := reply{status: res.StatusCode}
	var parsed struct {
		Accepted *float64 `json:"accepted"`
		Error    *string  `json:"error"`
		Rejected []struct {
			Error string `json:"error"`
		} `json:"rejected"`
	}
	if json.Unmarshal(raw, &parsed) == nil {
		if parsed.Accepted != nil {
			r.accepted = int(*parsed.Accepted)
		}
		if parsed.Error != nil {
			r.why = *parsed.Error
		} else if len(parsed.Rejected) > 0 {
			r.why = parsed.Rejected[0].Error
		}
	}
	r.why = jsSlice(r.why, 200)
	return r, nil
}

func (r reply) retry() bool { return r.status == 429 || r.status >= 500 }

// backedOff: a send timed out in the last backoffFor.
func (s *sender) backedOff() bool {
	if s.backoff == "" {
		return false
	}
	st, err := os.Stat(s.backoff)
	return err == nil && s.now().Sub(st.ModTime()) < backoffFor
}

func (s *sender) markSlow() {
	if s.backoff == "" {
		return
	}
	if os.MkdirAll(filepath.Dir(s.backoff), 0o700) == nil {
		_ = os.WriteFile(s.backoff, nil, 0o600)
		now := s.now()
		_ = os.Chtimes(s.backoff, now, now)
	}
}

func (s *sender) clearSlow() {
	if s.backoff != "" {
		_ = os.Remove(s.backoff)
	}
}

func batchBody(records [][]byte) []byte {
	var b bytes.Buffer
	b.WriteString(`{"v":1,"records":[`)
	for i, r := range records {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(r)
	}
	b.WriteString("]}")
	return b.Bytes()
}

// fit: this run's events first, then spooled ones oldest first, while the
// POST stays within 8 KB. The rest waits.
func fit(records [][]byte) (batch, rest [][]byte) {
	size := len(`{"v":1,"records":[]}`)
	for _, r := range records {
		n := len(r) + 1
		if size+n <= maxPostBytes {
			batch = append(batch, r)
			size += n
		} else {
			rest = append(rest, r)
		}
	}
	return batch, rest
}

// why names a network error the way @coldread/cli does: "timed out", an
// error code (ECONNREFUSED), or the message.
func why(err error) string {
	var dns *net.DNSError
	var errno syscall.Errno
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timed out"
	case errors.Is(err, errRedirect):
		return "redirect"
	case errors.As(err, &dns):
		if dns.IsNotFound {
			return "ENOTFOUND"
		}
		if dns.IsTemporary {
			return "EAI_AGAIN"
		}
	case errors.As(err, &errno):
		if name, ok := errnoNames[errno]; ok {
			return name
		}
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	return jsSlice(err.Error(), 100)
}

var errnoNames = map[syscall.Errno]string{
	syscall.ECONNREFUSED: "ECONNREFUSED",
	syscall.ECONNRESET:   "ECONNRESET",
	syscall.ECONNABORTED: "ECONNABORTED",
	syscall.EHOSTUNREACH: "EHOSTUNREACH",
	syscall.ENETUNREACH:  "ENETUNREACH",
	syscall.ETIMEDOUT:    "ETIMEDOUT",
	syscall.EPIPE:        "EPIPE",
}

// --- the spool ---

// readSpool: the file's events, fresh ones only; junk lines are skipped.
func readSpool(file string, now time.Time) [][]byte {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var out [][]byte
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var r struct {
			TS *float64 `json:"ts"`
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(line, &obj) != nil || json.Unmarshal(line, &r) != nil || r.TS == nil {
			continue
		}
		if now.Sub(time.UnixMilli(int64(*r.TS))) >= maxSpoolAge {
			continue
		}
		out = append(out, append([]byte(nil), line...))
	}
	return out
}

// toSpool adds events to the spool, keeping the newest that fit its caps.
// The error says why they couldn't be written.
func toSpool(file string, records [][]byte, now time.Time) error {
	if len(records) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return err
	}
	add := 0
	for _, r := range records {
		add += len(r) + 1
	}
	size, count := int64(0), 0
	if st, err := os.Stat(file); err == nil {
		size = st.Size()
		if data, err := os.ReadFile(file); err == nil {
			for _, l := range bytes.Split(data, []byte("\n")) {
				if len(l) > 0 {
					count++
				}
			}
		}
	}
	if size+int64(add) <= maxSpoolBytes && count+len(records) <= maxSpoolEvents {
		f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		var b bytes.Buffer
		for _, r := range records {
			b.Write(r)
			b.WriteByte('\n')
		}
		_, err = f.Write(b.Bytes())
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		return err
	}
	// Over a cap: keep the newest that fit.
	all := append(readSpool(file, now), records...)
	var kept [][]byte
	bytesKept := 0
	for i := len(all) - 1; i >= 0 && len(kept) < maxSpoolEvents; i-- {
		n := len(all[i]) + 1
		if bytesKept+n > maxSpoolBytes {
			break
		}
		kept = append([][]byte{all[i]}, kept...)
		bytesKept += n
	}
	var b bytes.Buffer
	for _, r := range kept {
		b.Write(r)
		b.WriteByte('\n')
	}
	tmp := fmt.Sprintf("%s.%d.tmp", file, os.Getpid())
	if err := os.WriteFile(tmp, b.Bytes(), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// newEventID: 16 random bytes, hex, made once per event.
func newEventID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// withKey: the event as the spool keeps it, with the key it was made with
// (k, never sent): it's only ever sent with that key. One that has its k
// already keeps it.
func withKey(record []byte, key string) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(record, &obj) != nil {
		return record
	}
	if _, ok := obj["k"]; ok {
		return record
	}
	k, _ := json.Marshal(key)
	obj["k"] = k
	out, err := json.Marshal(obj)
	if err != nil {
		return record
	}
	return out
}

// spooled: the claimed events made with this sender's key, without their
// k, ready to send. Events made with another key go back to the spool
// untouched, for a run that has it (a week at most).
func (s *sender) spooled(files []string, now time.Time) [][]byte {
	var mine, theirs [][]byte
	for _, r := range readClaimed(files, now) {
		var obj map[string]json.RawMessage
		if json.Unmarshal(r, &obj) != nil {
			continue
		}
		if raw, ok := obj["k"]; ok {
			var k string
			if json.Unmarshal(raw, &k) == nil && k != s.key {
				theirs = append(theirs, r)
				continue
			}
			delete(obj, "k")
			if out, err := json.Marshal(obj); err == nil {
				r = out
			}
		}
		mine = append(mine, r)
	}
	if len(theirs) > 0 {
		_, _ = s.save(theirs)
	}
	return mine
}

// save puts events in the spool, else in the temp folder's, each with the
// key it was made with. It returns the file they went to, or why the spool
// couldn't take them.
func (s *sender) save(records [][]byte) (string, error) {
	if len(records) == 0 {
		return "", nil
	}
	tagged := make([][]byte, len(records))
	for i, r := range records {
		tagged[i] = withKey(r, s.key)
	}
	records = tagged
	err := toSpool(s.spool, records, s.now())
	if err == nil {
		return s.spool, nil
	}
	if s.alt == "" || privateDir(tmpTop(s.alt), true) != nil {
		return "", err
	}
	if toSpool(s.alt, records, s.now()) != nil {
		return "", err
	}
	return s.alt, nil
}

// claim takes both spools for this process (claimSpool); the temp folder's
// only while its folder is this user's own.
func (s *sender) claim(now time.Time) []string {
	claimed := claimSpool(s.spool, now)
	if s.alt != "" && privateDir(tmpTop(s.alt), false) == nil {
		claimed = append(claimed, claimSpool(s.alt, now)...)
	}
	return claimed
}

// tmpTop: <tmp>/coldread-<uid> for <tmp>/coldread-<uid>/<tool>/spool.jsonl.
func tmpTop(alt string) string { return filepath.Dir(filepath.Dir(alt)) }

// tmpSpoolFor: a tool's spool in the temp folder. Per user, since the temp
// folder can be shared (/tmp on Linux).
func tmpSpoolFor(tool, tmp string) string {
	top := "coldread"
	if uid := os.Getuid(); uid >= 0 {
		top = fmt.Sprintf("coldread-%d", uid)
	}
	return filepath.Join(tmp, top, cacheName(tool), "spool.jsonl")
}

// saveWhy names why events couldn't be written: EPERM, EACCES, EROFS, or
// the message.
func saveWhy(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if name, ok := saveErrnos[errno]; ok {
			return name
		}
	}
	var pe *os.PathError
	if errors.As(err, &pe) {
		err = pe.Err
	}
	return jsSlice(err.Error(), 100)
}

var saveErrnos = map[syscall.Errno]string{
	syscall.EACCES:  "EACCES",
	syscall.EPERM:   "EPERM",
	syscall.EROFS:   "EROFS",
	syscall.ENOSPC:  "ENOSPC",
	syscall.ENOENT:  "ENOENT",
	syscall.ENOTDIR: "ENOTDIR",
}

// claimSpool takes the spool for this process by renaming it (atomic: of
// two runs at once, one gets it), plus any claim a run left behind when it
// exited mid-send: at once when the process that made it is gone (a server
// killed mid-send: Codex's stdin EOF then SIGTERM), after staleClaim while
// it's alive (it may still be sending). Returns the claimed files.
func claimSpool(file string, now time.Time) []string {
	var claimed []string
	mine := func() string {
		return fmt.Sprintf("%s.%d.%d.sending", file, os.Getpid(), now.UnixNano()+int64(len(claimed)))
	}
	if name := mine(); os.Rename(file, name) == nil {
		claimed = append(claimed, name)
	}
	old, _ := filepath.Glob(file + ".*.sending")
	for _, o := range old {
		if contains(claimed, o) {
			continue
		}
		st, err := os.Stat(o)
		if err != nil || (now.Sub(st.ModTime()) < staleClaim && !processGone(claimPID(file, o))) {
			continue
		}
		if name := mine(); os.Rename(o, name) == nil {
			claimed = append(claimed, name)
		}
	}
	return claimed
}

// claimPID: the pid in a claim's name (<spool>.<pid>.<...>.sending, as
// every SDK names them), 0 when there isn't one.
func claimPID(file, claim string) int {
	rest := strings.TrimPrefix(claim, file+".")
	if rest == claim {
		return 0
	}
	pid, err := strconv.Atoi(strings.SplitN(rest, ".", 2)[0])
	if err != nil || pid == os.Getpid() {
		return 0
	}
	return pid
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func readClaimed(files []string, now time.Time) [][]byte {
	var out [][]byte
	for _, f := range files {
		out = append(out, readSpool(f, now)...)
	}
	return out
}

func removeAll(files []string) {
	for _, f := range files {
		os.Remove(f)
	}
}

// drain sends what earlier runs spooled: the startup half of the send.
func (s *sender) drain() {
	s.drainMu.Lock()
	if s.drainDone {
		s.drainMu.Unlock()
		return
	}
	now := s.now()
	s.drainClaimed = s.claim(now)
	if len(s.drainClaimed) == 0 {
		s.drainDone = true
		s.drainMu.Unlock()
		return
	}
	s.drainRecords = s.spooled(s.drainClaimed, now)
	claimed := s.drainClaimed
	batch, rest := fit(s.drainRecords)
	s.drainMu.Unlock()

	retry := false
	if len(batch) > 0 {
		r, err := s.post(context.Background(), batch)
		retry = err != nil || r.retry()
	}
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	if s.drainDone {
		return // Finish gave up on it and put its events back
	}
	s.drainDone = true
	if retry {
		rest = append(batch, rest...)
	}
	_, _ = s.save(rest)
	removeAll(claimed)
}

// abandonDrain: the process is about to exit with the startup send still
// going. Its events go back to the spool, within the spool's caps, and its
// claim is dropped, so nothing piles up on a network that never answers.
func (s *sender) abandonDrain() {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	if s.drainDone {
		return
	}
	s.drainDone = true
	_, _ = s.save(s.drainRecords)
	removeAll(s.drainClaimed)
}

// sendOne sends this run's event, waiting until deadline at most. What
// isn't sent by then is spooled; the POST, if it is still going, is left to
// finish or die with the process.
func (s *sender) sendOne(record []byte, deadline time.Duration) {
	var mu sync.Mutex
	resolved := false
	resolve := func(spool bool) {
		mu.Lock()
		defer mu.Unlock()
		if resolved {
			return
		}
		resolved = true
		if spool {
			_, _ = s.save([][]byte{record})
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, err := s.post(context.Background(), [][]byte{record})
		resolve(err != nil || r.retry())
	}()
	t := time.NewTimer(deadline)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		if !s.reached.Load() {
			s.markSlow() // never connected: the network drops packets
		}
		resolve(true)
	}
}

// verify is COLDREAD_VERIFY=1: this run's event and the spool in one POST,
// waited for, and one line on stderr saying what came back and, for an
// event that wasn't sent, where it waits (or why it couldn't).
func (s *sender) verify(record []byte, offline bool, say func(string)) {
	now := s.now()
	if offline {
		where, err := s.save([][]byte{record})
		if err != nil {
			say("no network here; not saved (" + saveWhy(err) + "), so this event is lost.")
			return
		}
		say("no network here; saved for the next run with network (" + where + ").")
		return
	}
	claimed := s.claim(now)
	spooled := s.spooled(claimed, now)
	removeAll(claimed)
	batch, rest := fit(append([][]byte{record}, spooled...))
	if len(batch) == 0 {
		_, _ = s.save(rest)
		say("not sent (the event is over 8 KB).")
		return
	}
	r, err := s.post(context.Background(), batch)
	if err != nil {
		say("not sent (" + why(err) + "); " + saved(s.save(append(batch, rest...))))
		return
	}
	if r.retry() {
		say(fmt.Sprintf("not sent (%d); %s", r.status, saved(s.save(append(batch, rest...)))))
		return
	}
	_, _ = s.save(rest)
	switch {
	case r.status >= 200 && r.status < 300 && r.accepted > 0:
		say("accepted")
	case r.why != "":
		say(fmt.Sprintf("rejected (%d: %s)", r.status, r.why))
	default:
		say(fmt.Sprintf("rejected (%d)", r.status))
	}
}

// saved ends a verify line for an event left for the next run.
func saved(where string, err error) string {
	if err != nil {
		return "not saved (" + saveWhy(err) + "), so this event is lost."
	}
	return "saved for the next run (" + where + ")."
}
