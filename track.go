package coldread

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// The website and API half: the Go counterpart of @coldread/track and
// coldread-sdk's website middleware, line for line where Go allows.
//
//	tr := coldread.NewTracker(coldread.TrackerOptions{}) // COLDREAD_KEY from env
//	defer tr.Close()
//	http.ListenAndServe(":8080", tr.Middleware(mux))
//
// Server-side on purpose: the big AI crawlers don't run JavaScript, so a
// browser snippet never sees them. It never delays or breaks a response:
// a request is queued in memory and a background goroutine sends batches.
// If Coldread is slow or down, a batch times out and is dropped, sending
// pauses with backoff, and nothing is retried or piled up.
//
// People are counted, not sent. Each request is classified here, by the
// rule Coldread's server applies. A person's page view becomes part of a
// count: path, method, status, minute, how many. A person's other requests
// (prefetches, assets, API calls), which Coldread never keeps, aren't sent
// at all. Anything that might be an agent is sent whole: the method, the
// path (no query string), the response's status, the client IP, the host,
// and the headers in SentHeaders, so Coldread can check the agent is who it
// says. Never cookies, auth, other headers or bodies.

// SentHeaders are the only request headers sent: who's asking (user-agent,
// and accept-language, which real browsers always send), whether it's a page
// load (accept, sec-fetch-dest), the browser's own client hints (sec-ch-ua*:
// a cloud browser dressed as a laptop gives itself away), Web Bot Auth
// signatures, and ai-agent, which CLIs a coding agent drives stamp on their
// API calls.
var SentHeaders = []string{"user-agent", "accept", "accept-language", "sec-fetch-dest", "sec-ch-ua", "sec-ch-ua-platform", "sec-ch-ua-mobile", "signature", "signature-input", "signature-agent", "ai-agent"}

// ConnectedLine is the line an install is verified by. Said once per
// process, on stderr, the first time Coldread confirms it accepted records.
const ConnectedLine = "[coldread] connected: first requests accepted"

// Mirrors the ingest endpoint's caps, so records aren't rejected there.
const (
	maxBatch  = 500
	maxCount  = 100_000
	minuteMs  = 60_000
	maxPath   = 2048
	maxHeader = 2048
	// Distinct (minute, method, status, path) counts held at once. Past it,
	// every count goes out early: memory stays bounded however many paths.
	maxBuckets = 500
)

var signatureHeaders = map[string]bool{"signature": true, "signature-input": true, "signature-agent": true}

// TrackerOptions configure a Tracker. All optional.
type TrackerOptions struct {
	// Key is your site's secret key (cr_sec_...). Defaults to COLDREAD_KEY.
	Key string
	// Endpoint defaults to COLDREAD_ENDPOINT, then DefaultEndpoint.
	Endpoint string
	// FlushInterval is the longest a request waits before its batch is
	// sent. Default 2s.
	FlushInterval time.Duration
	// BatchSize is records per batch, at most 500. Default 100.
	BatchSize int
	// MaxQueue is records waiting to be sent; past it, new ones are
	// dropped. Default 1000.
	MaxQueue int
	// Timeout abandons a batch that takes longer. Default 3s.
	Timeout time.Duration
	// People is "count" (the default: people's page views go as counts,
	// never their IP or headers) or "full" (every request is sent whole).
	People string
	// IP returns the visitor's address when your host puts it somewhere
	// other than cf-connecting-ip, x-real-ip, x-forwarded-for or the
	// connection (e.g. Fly's fly-client-ip).
	IP func(*http.Request) string
}

// RequestFacts is one request, for Track: what Middleware reads from an
// *http.Request, for servers that aren't net/http (fasthttp, Fiber).
type RequestFacts struct {
	Method string
	// URL is a path or a full URL; only the path is sent.
	URL    string
	Header http.Header
	// IP is the visitor's address. Coldread uses it to check that a bot
	// claiming to be, say, OpenAI's comes from OpenAI's published ranges,
	// then discards it.
	IP string
	// Host is the host the visitor asked for (signatures cover it).
	Host string
	// Status is the response's status code; 0 when it isn't known.
	Status int
	// Time defaults to now.
	Time time.Time
}

// TrackerStats count requests (a count is as many requests as it counts),
// not records.
type TrackerStats struct{ Sent, Dropped int }

// Tracker sends a server's requests to Coldread. The zero value and nil do
// nothing. One per process is plenty.
type Tracker struct {
	key, endpoint string
	flushInterval time.Duration
	batchSize     int
	maxQueue      int
	timeout       time.Duration
	countPeople   bool
	ip            func(*http.Request) string
	enabled       bool
	now           func() time.Time
	write         func(string)
	client        *http.Client

	start sync.Once
	wake  chan struct{}

	mu         sync.Mutex
	changed    chan struct{} // closed and replaced whenever the queue or a send moves
	queue      []any         // *httpRecord or *peopleRecord
	counts     map[countKey]*peopleRecord
	firstAt    time.Time
	flushing   int
	inFlight   bool
	closed     bool
	pausedTill time.Time
	failures   int
	stats      TrackerStats
	warned     map[string]bool
}

type countKey struct {
	minute int64
	method string
	status int
	path   string
}

// httpRecord is the wire format of one request in a POST /api/ingest batch.
type httpRecord struct {
	Source  string            `json:"source"`
	TS      int64             `json:"ts"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	IP      string            `json:"ip,omitempty"`
	Host    string            `json:"host,omitempty"`
	Status  int               `json:"status,omitempty"`
}

// peopleRecord is how many page views by people one path had, with one
// method and status, in one minute. Nothing about who.
type peopleRecord struct {
	Source string `json:"source"`
	Minute int64  `json:"minute"`
	Method string `json:"method"`
	Path   string `json:"path"`
	Status int    `json:"status,omitempty"`
	N      int    `json:"n"`
}

func weight(r any) int {
	if p, ok := r.(*peopleRecord); ok {
		return p.N
	}
	return 1
}

// Seams for tests; none are needed in real use.
type trackerInternals struct {
	env     map[string]string
	write   func(string)
	now     func() time.Time
	testRun *bool
}

// NewTracker starts a Tracker. Without a usable secret key, opted out
// (DO_NOT_TRACK, COLDREAD_DISABLED) or in a test binary sending to
// Coldread's own endpoint, it says why once on stderr and sends nothing.
func NewTracker(opts TrackerOptions) *Tracker { return newTracker(opts, trackerInternals{}) }

func newTracker(opts TrackerOptions, in trackerInternals) *Tracker {
	env := in.env
	if env == nil {
		env = environ()
	}
	t := &Tracker{
		key: opts.Key, endpoint: resolveEndpoint(opts.Endpoint, env),
		flushInterval: opts.FlushInterval, batchSize: opts.BatchSize, maxQueue: opts.MaxQueue, timeout: opts.Timeout,
		countPeople: opts.People != "full", ip: opts.IP,
		now: in.now, write: in.write,
		wake: make(chan struct{}, 1), changed: make(chan struct{}),
		counts: map[countKey]*peopleRecord{}, warned: map[string]bool{},
	}
	if t.key == "" {
		t.key = env["COLDREAD_KEY"]
	}
	if t.flushInterval <= 0 {
		t.flushInterval = 2 * time.Second
	}
	if t.batchSize <= 0 {
		t.batchSize = 100
	}
	if t.batchSize > maxBatch {
		t.batchSize = maxBatch
	}
	if t.maxQueue <= 0 {
		t.maxQueue = 1000
	}
	if t.timeout <= 0 {
		t.timeout = 3 * time.Second
	}
	if t.now == nil {
		t.now = time.Now
	}
	if t.write == nil {
		t.write = func(s string) { _, _ = os.Stderr.WriteString(s) }
	}
	testRun := isTestBinary()
	if in.testRun != nil {
		testRun = *in.testRun
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 2
	t.client = &http.Client{
		Transport: tr,
		// Never follow a redirect: the key goes to the endpoint and nowhere else.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	switch {
	case truthy(env, "DO_NOT_TRACK"):
		t.warn("key", "disabled (DO_NOT_TRACK); not sending.")
	case truthy(env, "COLDREAD_DISABLED"):
		t.warn("key", "disabled (COLDREAD_DISABLED); not sending.")
	case t.key == "":
		t.warn("key", "no key (set COLDREAD_KEY); not sending.")
	case strings.HasPrefix(t.key, "cr_pub_"):
		// A site's public key is for MCP servers and CLIs; ingest refuses it for requests.
		t.warn("key", "that's a public key (cr_pub_), which is for MCP servers and CLIs. Use the site's secret key (cr_sec_); not sending.")
	case t.endpoint == DefaultEndpoint && (testRun || isTestRun(env)):
		t.warn("test", "test run; not sending (set COLDREAD_ENDPOINT to send).")
	default:
		t.enabled = true
	}
	return t
}

// Enabled is false when nothing will ever be sent: no key, a public key,
// opted out, or a test run.
func (t *Tracker) Enabled() bool { return t != nil && t.enabled }

// Stats says how many requests were sent and dropped so far.
func (t *Tracker) Stats() TrackerStats {
	if t == nil || t.warned == nil {
		return TrackerStats{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

// Middleware reports every request next serves, with its response's
// status, once the handler writes its header (or returns). It never touches
// the response. Works with anything net/http: http.ServeMux, chi's r.Use,
// and gin, echo or gorilla by wrapping the router where the server is made.
func (t *Tracker) Middleware(next http.Handler) http.Handler {
	if !t.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		facts, ok := t.factsOf(r)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusWriter{ResponseWriter: w}
		rec.report = func(status int) {
			facts.Status = status
			t.Track(facts)
		}
		returned := false
		defer func() {
			// The handler returned without writing: net/http sends a 200.
			// It panicked before writing: no response, so no status.
			if returned {
				rec.done(http.StatusOK)
			} else {
				rec.done(0)
			}
		}()
		next.ServeHTTP(rec, r)
		returned = true
	})
}

// factsOf reads what Coldread needs from r. Never panics.
func (t *Tracker) factsOf(r *http.Request) (f RequestFacts, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	url := r.RequestURI // the target as the client sent it, before any router rewrote r.URL
	if url == "" && r.URL != nil {
		url = r.URL.RequestURI()
	}
	ip := ""
	if t.ip != nil {
		ip = t.ip(r)
	} else {
		ip = ClientIP(r)
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	return RequestFacts{Method: r.Method, URL: url, Header: r.Header, IP: ip, Host: host, Time: t.now()}, true
}

// ClientIP is the visitor's address as Middleware reads it: Cloudflare's
// cf-connecting-ip, then x-real-ip, the first x-forwarded-for, then the
// connection's own address. Only trust the headers if your proxy sets them.
func ClientIP(r *http.Request) string {
	if v := r.Header.Get("Cf-Connecting-Ip"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Real-Ip"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if first := jsTrim(strings.SplitN(v, ",", 2)[0]); first != "" {
			return first
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// --- the response's status ---

// statusWriter sees the status a handler sends and reports the request
// once. Everything else goes straight through; Unwrap lets
// http.ResponseController reach the server's own writer.
type statusWriter struct {
	http.ResponseWriter
	reported bool
	report   func(status int)
}

func (w *statusWriter) done(status int) {
	if w.reported {
		return
	}
	w.reported = true
	func() {
		defer func() { _ = recover() }() // tracking never breaks a response
		w.report(status)
	}()
}

func (w *statusWriter) WriteHeader(code int) {
	// 1xx informational headers (103 Early Hints) come before the real one.
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.done(code)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.done(http.StatusOK)
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) ReadFrom(src io.Reader) (int64, error) {
	w.done(http.StatusOK)
	return io.Copy(w.ResponseWriter, src)
}

func (w *statusWriter) Flush() {
	w.done(http.StatusOK)
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Hijack hands the connection over (websockets). What's sent on it isn't
// seen, so the request is reported without a status.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.done(0)
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *statusWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// --- the record ---

var (
	methodRe   = regexp.MustCompile(`^[A-Z]{1,16}$`)
	ipv4Re     = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	ipv6Re     = regexp.MustCompile(`(?i)^[0-9a-f:.]+$`)
	hostRe     = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:\d{1,5})?$`)
	absoluteRe = regexp.MustCompile(`(?s)^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*(.*)$`)
)

// pathOf is the path alone: no scheme, host, query or fragment, with spaces
// and control characters percent-encoded.
func pathOf(url string) string {
	p := url
	if p == "" {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		if m := absoluteRe.FindStringSubmatch(p); m != nil {
			p = m[1]
		} else {
			p = ""
		}
		if p == "" {
			p = "/"
		}
	}
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	var b strings.Builder
	for _, r := range p {
		if r <= 0x1f || r == 0x7f || jsSpace(r) {
			var buf [utf8.UTFMax]byte
			for _, c := range buf[:utf8.EncodeRune(buf[:], r)] {
				fmt.Fprintf(&b, "%%%02X", c)
			}
			continue
		}
		b.WriteRune(r)
	}
	p = b.String()
	if p == "" {
		p = "/"
	}
	return jsSlice(p, maxPath)
}

// headerValue reads a header by its lowercase name: repeated headers join
// with ", ". Non-canonical keys (a header map built by hand) work too.
func headerValue(h http.Header, name string) string {
	vs := h.Values(name)
	if len(vs) == 0 {
		vs = h[name]
	}
	return strings.Join(vs, ", ")
}

// cut is s cut to n bytes, never mid-character.
func cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for i := 1; i < utf8.UTFMax && i <= len(s); i++ {
		if utf8.RuneStart(s[len(s)-i]) {
			if !utf8.FullRuneInString(s[len(s)-i:]) {
				s = s[:len(s)-i]
			}
			break
		}
	}
	return s
}

// toRecord: a request as a wire record, keeping only what Coldread reads.
// nil if it can't be one (a method that isn't a method).
func toRecord(f RequestFacts, now time.Time) *httpRecord {
	method := strings.ToUpper(f.Method)
	if method == "" {
		method = "GET"
	}
	if !methodRe.MatchString(method) {
		return nil
	}
	headers := map[string]string{}
	for _, name := range SentHeaders {
		v := headerValue(f.Header, name)
		if v == "" {
			continue
		}
		if len(v) <= maxHeader {
			headers[name] = v
		} else if !signatureHeaders[name] {
			// A cut signature is worthless; other headers are still useful cut.
			headers[name] = cut(v, maxHeader)
		}
	}
	ts := f.Time
	if ts.IsZero() {
		ts = now
	}
	r := &httpRecord{Source: "http", TS: ts.UnixMilli(), Method: method, Path: pathOf(f.URL), Headers: headers}
	// A raw x-forwarded-for is "visitor, proxy, ...": the first is the visitor.
	if ip := jsTrim(strings.SplitN(f.IP, ",", 2)[0]); ip != "" && len(ip) <= 64 && (ipv4Re.MatchString(ip) || (strings.Contains(ip, ":") && ipv6Re.MatchString(ip))) {
		r.IP = ip
	}
	if host := strings.ToLower(jsTrim(strings.SplitN(f.Host, ",", 2)[0])); host != "" && hostRe.MatchString(host) {
		r.Host = host
	}
	if f.Status >= 100 && f.Status <= 599 {
		r.Status = f.Status
	}
	return r
}

// --- queueing ---

// Track queues one request. It returns at once and never panics. Middleware
// calls it for you; call it yourself on servers that aren't net/http.
func (t *Tracker) Track(f RequestFacts) {
	if !t.Enabled() {
		return
	}
	defer func() { _ = recover() }() // tracking never breaks a response
	now := t.now()
	t.mu.Lock()
	if t.closed || now.Before(t.pausedTill) || len(t.queue) >= t.maxQueue {
		t.stats.Dropped++
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	rec := toRecord(f, now)
	verdict := verdictSend
	if rec != nil && t.countPeople {
		verdict = peopleRule(rec.Method, rec.Path, func(n string) string { return rec.Headers[n] })
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec == nil {
		t.stats.Dropped++
		return
	}
	first := len(t.queue) == 0
	hadCounts := len(t.counts) > 0
	t.drain(now, false)
	switch verdict {
	case verdictSend:
		t.enqueue(rec, now)
	case verdictCount:
		t.count(rec, now)
	}
	t.start.Do(func() { go t.run() })
	// Wake the sender to start the flush wait, send a full batch, or wait
	// for a count's minute to close.
	if first || len(t.queue) >= t.batchSize || (verdict == verdictCount && !hadCounts) {
		t.signal()
	}
}

// Called with the lock held, from here down to post.

func (t *Tracker) enqueue(r any, now time.Time) {
	if len(t.queue) >= t.maxQueue {
		t.stats.Dropped += weight(r)
		return
	}
	if len(t.queue) == 0 {
		t.firstAt = now
	}
	t.queue = append(t.queue, r)
}

// drain moves counts whose minute has closed (all of them, with all) to
// the queue. A count's time is its minute, so one sent late still lands
// where it was, and Coldread adds up the parts.
func (t *Tracker) drain(now time.Time, all bool) int {
	open := now.UnixMilli() / minuteMs * minuteMs
	moved := 0
	for k, r := range t.counts {
		if !all && r.Minute >= open {
			continue
		}
		delete(t.counts, k)
		t.enqueue(r, now)
		moved++
	}
	return moved
}

func (t *Tracker) count(r *httpRecord, now time.Time) {
	minute := r.TS / minuteMs * minuteMs
	k := countKey{minute, r.Method, r.Status, r.Path}
	had := t.counts[k]
	if had != nil && had.N < maxCount {
		had.N++
		return
	}
	if had != nil {
		delete(t.counts, k)
		t.enqueue(had, now)
	}
	if len(t.counts) >= maxBuckets {
		t.drain(now, true)
	}
	t.counts[k] = &peopleRecord{Source: "people", Minute: minute, Method: r.Method, Path: r.Path, Status: r.Status, N: 1}
}

func (t *Tracker) signal() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *Tracker) broadcast() {
	close(t.changed)
	t.changed = make(chan struct{})
}

// sleep waits d, or until woken, with the lock released.
func (t *Tracker) sleep(d time.Duration) {
	t.mu.Unlock()
	timer := time.NewTimer(d)
	select {
	case <-t.wake:
	case <-timer.C:
	}
	timer.Stop()
	t.mu.Lock()
}

// run is the sender: one goroutine per Tracker, started by the first Track.
func (t *Tracker) run() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		for len(t.queue) == 0 {
			if t.closed {
				return
			}
			// Idle, or holding counts until their minute closes.
			now := t.now()
			if t.drain(now, false) > 0 {
				continue
			}
			wait := 30 * time.Second
			if len(t.counts) > 0 {
				wait = time.Duration(minuteMs-now.UnixMilli()%minuteMs+50) * time.Millisecond
			}
			t.sleep(wait)
		}
		// A full batch, the flush interval, or a Flush.
		for len(t.queue) < t.batchSize && t.flushing == 0 && !t.closed {
			left := t.firstAt.Add(t.flushInterval).Sub(t.now())
			if left <= 0 {
				break
			}
			t.sleep(left)
		}
		if t.now().Before(t.pausedTill) {
			for _, r := range t.queue {
				t.stats.Dropped += weight(r)
			}
			t.queue = nil
			t.broadcast()
			continue
		}
		n := min(t.batchSize, len(t.queue))
		batch := append([]any(nil), t.queue[:n]...)
		t.queue = append([]any(nil), t.queue[n:]...)
		t.inFlight = true
		t.mu.Unlock()
		t.post(batch)
		t.mu.Lock()
		t.inFlight = false
		// What's left waits its own flush interval, unless it's a full batch.
		t.firstAt = t.now()
		t.broadcast()
	}
}

// Flush sends everything queued now, people's counts included, waiting
// until it's sent or ctx is done. Never returns an error.
func (t *Tracker) Flush(ctx context.Context) {
	if !t.Enabled() {
		return
	}
	defer func() { _ = recover() }()
	t.mu.Lock()
	t.drain(t.now(), true)
	if len(t.queue) == 0 && !t.inFlight {
		t.mu.Unlock()
		return
	}
	t.flushing++
	t.start.Do(func() { go t.run() })
	t.signal()
	for len(t.queue) > 0 || t.inFlight {
		ch := t.changed
		t.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			t.mu.Lock()
			t.flushing--
			t.mu.Unlock()
			return
		}
		t.mu.Lock()
	}
	t.flushing--
	t.mu.Unlock()
}

// Close sends what's queued, waiting the send timeout plus a second at
// most, and stops the sender. Requests tracked after it are dropped. Call
// it on shutdown, after http.Server.Shutdown.
func (t *Tracker) Close() {
	if !t.Enabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout+time.Second)
	defer cancel()
	t.Flush(ctx)
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.signal()
}

// --- sending ---

func (t *Tracker) warn(id, message string) {
	if t.warned[id] {
		return
	}
	t.warned[id] = true
	t.write("[coldread] " + message + "\n")
}

func (t *Tracker) pause(d time.Duration) {
	if until := t.now().Add(d); until.After(t.pausedTill) {
		t.pausedTill = until
	}
}

// backoff: no retries. A failed batch is dropped and sending pauses, 1s
// doubling to a minute.
func (t *Tracker) backoff() {
	t.failures++
	d := time.Minute
	if t.failures <= 7 {
		d = min(time.Minute, time.Second<<(t.failures-1))
	}
	t.pause(d)
}

var connectedSaid atomic.Bool

func (t *Tracker) post(records []any) {
	total := 0
	for _, r := range records {
		total += weight(r)
	}
	body, err := json.Marshal(struct {
		V       int   `json:"v"`
		Records []any `json:"records"`
	}{1, records})
	if err != nil {
		t.mu.Lock()
		t.stats.Dropped += total
		t.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		t.mu.Lock()
		t.stats.Dropped += total
		t.warn("endpoint", "the endpoint isn't a URL; not sending.")
		t.mu.Unlock()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.key)
	req.Header.Set("User-Agent", "coldread-go/"+Version)
	res, err := t.client.Do(req)
	if err != nil {
		cause := why(err)
		if cause == "timed out" {
			cause = fmt.Sprintf("timed out after %dms", t.timeout.Milliseconds())
		}
		t.mu.Lock()
		t.stats.Dropped += total
		t.backoff()
		t.warn("net:"+cause, fmt.Sprintf("couldn't reach %s (%s); retrying with the next requests.", t.endpoint, cause))
		t.mu.Unlock()
		return
	}
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
	res.Body.Close()

	t.mu.Lock()
	defer t.mu.Unlock()
	status := res.StatusCode
	switch {
	case status >= 300 && status < 400:
		t.stats.Dropped += total
		t.backoff()
		t.warn("redirect", "the endpoint redirected; not following it.")
	case status >= 200 && status < 300:
		t.failures = 0
		// Accepted as a batch, refused record by record: say why, once.
		accepted, refusedAt, why := outcomeOf(raw)
		refused := 0
		for _, i := range refusedAt {
			if i >= 0 && i < len(records) {
				refused += weight(records[i])
			} else {
				refused++
			}
		}
		t.stats.Sent += total - refused
		t.stats.Dropped += refused
		if len(refusedAt) > 0 {
			reason := ""
			if why != "" {
				reason = " (" + why + ")"
			}
			t.warn("rejected", fmt.Sprintf("%d of %d records refused%s.", len(refusedAt), len(records), reason))
		}
		// Only the server's own count confirms anything: a 202 that refused
		// every record, or a body that doesn't say, isn't a connection.
		if accepted > 0 && connectedSaid.CompareAndSwap(false, true) {
			t.write(ConnectedLine + "\n")
		}
	default:
		t.stats.Dropped += total
		auth := status == 401 || status == 403
		ctype := res.Header.Get("Content-Type")
		switch {
		case auth && errorIn(ctype, raw) != "":
			t.pause(10 * time.Minute)
			t.warn("auth", fmt.Sprintf("key rejected (%s); paused 10 minutes.", errorIn(ctype, raw)))
		case auth:
			// Coldread always names its error, as JSON; anything else (a
			// firewall's or bot challenge's page) came from something in
			// front of it and says nothing about the key.
			kind := strings.TrimSpace(strings.SplitN(ctype, ";", 2)[0])
			if kind == "" {
				kind = "no content type"
			}
			t.backoff()
			t.warn(fmt.Sprintf("blocked:%d", status), fmt.Sprintf("blocked before reaching Coldread (HTTP %d, %s; a firewall or bot challenge?). Not a key problem.", status, kind))
		case status == 429:
			after, err := strconv.ParseFloat(strings.TrimSpace(res.Header.Get("Retry-After")), 64)
			if err != nil || after == 0 {
				after = 60
			}
			t.pause(time.Duration(max(0, min(after, 300)) * float64(time.Second)))
		case status >= 500:
			t.backoff()
		default:
			t.warn(fmt.Sprintf("status:%d", status), fmt.Sprintf("batch refused (%d).", status))
		}
	}
}

// outcomeOf: what a 202 says. How many records it accepted (0 when it
// doesn't say), which it refused (by index, -1 when unsaid), and the first
// reason. A body that isn't the expected JSON refused nothing and confirmed
// nothing.
func outcomeOf(body []byte) (accepted int, refused []int, why string) {
	var b struct {
		Accepted *float64 `json:"accepted"`
		Rejected []struct {
			I     *float64 `json:"i"`
			Error *string  `json:"error"`
		} `json:"rejected"`
	}
	if json.Unmarshal(body, &b) != nil {
		return 0, nil, ""
	}
	if b.Accepted != nil {
		accepted = int(*b.Accepted)
	}
	for _, r := range b.Rejected {
		if r.I != nil && *r.I == float64(int(*r.I)) {
			refused = append(refused, int(*r.I))
		} else {
			refused = append(refused, -1)
		}
	}
	if len(b.Rejected) > 0 && b.Rejected[0].Error != nil {
		why = jsSlice(*b.Rejected[0].Error, 200)
	}
	return accepted, refused, why
}

// errorIn: the error an auth failure names, if any. Coldread always names
// one, as JSON.
func errorIn(contentType string, body []byte) string {
	if !strings.Contains(strings.ToLower(contentType), "json") {
		return ""
	}
	var b struct {
		Error *string `json:"error"`
	}
	if json.Unmarshal(body, &b) != nil || b.Error == nil {
		return ""
	}
	return jsSlice(*b.Error, 200)
}
