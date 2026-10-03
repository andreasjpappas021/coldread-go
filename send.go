package coldread

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
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
//  3. A send that timed out leaves a mark (<cache>/backoff) for 10 minutes:
//     a network that drops packets would otherwise cost every command the
//     whole wait. Marked, runs spool at exit without waiting and only the
//     background send tries; any answer from Coldread clears the mark.
//
// The spool is @coldread/cli's, same file and format: one JSON event per
// line in <cache>/spool.jsonl, the newest 100 within 64 KB, nothing older
// than 7 days. One POST is at most 8 KB.

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
	backoff  string
	now      func() time.Time
	client   *http.Client

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
			return dialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if c := s.takeWarm(ctx, addr); c != nil {
				return c, nil
			}
			return tlsDialer.DialContext(ctx, network, addr)
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
	}()
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
		if why(err) == "timed out" {
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
	st, err := os.Stat(s.backoff)
	return err == nil && s.now().Sub(st.ModTime()) < backoffFor
}

func (s *sender) markSlow() {
	if os.MkdirAll(filepath.Dir(s.backoff), 0o700) == nil {
		_ = os.WriteFile(s.backoff, nil, 0o600)
		now := s.now()
		_ = os.Chtimes(s.backoff, now, now)
	}
}

func (s *sender) clearSlow() { _ = os.Remove(s.backoff) }

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
func toSpool(file string, records [][]byte, now time.Time) {
	if len(records) == 0 {
		return
	}
	if os.MkdirAll(filepath.Dir(file), 0o700) != nil {
		return
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
			return
		}
		defer f.Close()
		var b bytes.Buffer
		for _, r := range records {
			b.Write(r)
			b.WriteByte('\n')
		}
		_, _ = f.Write(b.Bytes())
		return
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
	if os.WriteFile(tmp, b.Bytes(), 0o600) == nil {
		if os.Rename(tmp, file) != nil {
			os.Remove(tmp)
		}
	}
}

// claimSpool takes the spool for this process by renaming it (atomic: of
// two runs at once, one gets it), plus any claim a run left behind when it
// exited mid-send. Returns the claimed files.
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
		if err != nil || now.Sub(st.ModTime()) < staleClaim {
			continue
		}
		if name := mine(); os.Rename(o, name) == nil {
			claimed = append(claimed, name)
		}
	}
	return claimed
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
	s.drainClaimed = claimSpool(s.spool, now)
	if len(s.drainClaimed) == 0 {
		s.drainDone = true
		s.drainMu.Unlock()
		return
	}
	s.drainRecords = readClaimed(s.drainClaimed, now)
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
	toSpool(s.spool, rest, s.now())
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
	toSpool(s.spool, s.drainRecords, s.now())
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
			toSpool(s.spool, [][]byte{record}, s.now())
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
		s.markSlow()
		resolve(true)
	}
}

// verify is COLDREAD_VERIFY=1: this run's event and the spool in one POST,
// waited for, and one line on stderr saying what came back.
func (s *sender) verify(record []byte, offline bool, say func(string)) {
	now := s.now()
	if offline {
		toSpool(s.spool, [][]byte{record}, now)
		say("no network here; saved for the next run with network.")
		return
	}
	claimed := claimSpool(s.spool, now)
	spooled := readClaimed(claimed, now)
	removeAll(claimed)
	batch, rest := fit(append([][]byte{record}, spooled...))
	if len(batch) == 0 {
		toSpool(s.spool, rest, now)
		say("not sent (the event is over 8 KB).")
		return
	}
	r, err := s.post(context.Background(), batch)
	if err != nil {
		toSpool(s.spool, append(batch, rest...), s.now())
		say("not sent (" + why(err) + "); saved for the next run.")
		return
	}
	if r.retry() {
		toSpool(s.spool, append(batch, rest...), s.now())
	} else {
		toSpool(s.spool, rest, s.now())
	}
	switch {
	case r.status >= 200 && r.status < 300 && r.accepted > 0:
		say("accepted")
	case r.retry():
		say(fmt.Sprintf("not sent (%d); saved for the next run.", r.status))
	case r.why != "":
		say(fmt.Sprintf("rejected (%d: %s)", r.status, r.why))
	default:
		say(fmt.Sprintf("rejected (%d)", r.status))
	}
}
