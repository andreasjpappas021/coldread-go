package coldread

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const secretKey = "cr_sec_0123456789abcdef0123456789abcdef"

const (
	chrome = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
	gptbot = "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)"
)

// lines collects what a Tracker writes to stderr.
type lines struct {
	mu  sync.Mutex
	got []string
}

func (l *lines) write(s string) {
	l.mu.Lock()
	l.got = append(l.got, strings.TrimSuffix(s, "\n"))
	l.mu.Unlock()
}

func (l *lines) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got...)
}

func (l *lines) has(sub string) bool {
	for _, s := range l.all() {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func testTracker(t *testing.T, endpoint string, opts TrackerOptions, env map[string]string) (*Tracker, *lines) {
	t.Helper()
	out := &lines{}
	if opts.Key == "" {
		opts.Key = secretKey
	}
	opts.Endpoint = endpoint
	if env == nil {
		env = map[string]string{}
	}
	tr := newTracker(opts, trackerInternals{env: env, write: out.write, testRun: bptr(false)})
	t.Cleanup(tr.Close)
	return tr, out
}

// wire is one record as ingest got it.
type wire struct {
	Source  string            `json:"source"`
	TS      int64             `json:"ts"`
	Minute  int64             `json:"minute"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	IP      string            `json:"ip"`
	Host    string            `json:"host"`
	Status  int               `json:"status"`
	N       int               `json:"n"`
}

func (in *ingest) records(t *testing.T) []wire {
	t.Helper()
	var out []wire
	for _, p := range in.posts() {
		for _, raw := range p.body.Records {
			var w wire
			if err := json.Unmarshal(raw, &w); err != nil {
				t.Fatal(err)
			}
			out = append(out, w)
		}
	}
	return out
}

func get(t *testing.T, srv *httptest.Server, path string, header map[string]string) int {
	t.Helper()
	req, _ := http.NewRequest("GET", srv.URL+path, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return res.StatusCode
}

func site(tr *Tracker) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/login" {
			w.WriteHeader(401)
			return
		}
		_, _ = io.WriteString(w, "hello")
	})
	return httptest.NewServer(tr.Middleware(mux))
}

var person = map[string]string{"User-Agent": chrome, "Accept": "text/html,*/*", "Accept-Language": "en-US", "Cookie": "session=secret"}

func TestAgentsGoWholePeopleAsCounts(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: time.Minute}, nil)
	srv := site(tr)
	defer srv.Close()

	paths := []string{"/", "/pricing", "/docs"}
	for i := 0; i < 30; i++ {
		get(t, srv, paths[i%3]+"?utm=x", person)
	}
	bot := map[string]string{"User-Agent": gptbot, "Accept": "*/*", "Cookie": "a=b", "Authorization": "Bearer hunter2", "X-Secret": "nope", "Cf-Connecting-Ip": "20.171.207.130", "X-Real-Ip": "10.0.0.1"} // forged: the connection's address goes
	if s := get(t, srv, "/pricing?utm=x#top", bot); s != 200 {
		t.Fatal(s)
	}
	get(t, srv, "/missing", bot)
	get(t, srv, "/login", bot)
	get(t, srv, "/logo.png", map[string]string{"User-Agent": chrome, "Accept": "image/avif,*/*", "Accept-Language": "en"}) // a person's asset: not sent
	get(t, srv, "/api/me", person)                                                                                         // a person's API call: not sent
	get(t, srv, "/", map[string]string{"User-Agent": "curl/8.7.1"})
	tr.Close()

	recs := in.records(t)
	var counts, whole []wire
	for _, r := range recs {
		if r.Source == "people" {
			counts = append(counts, r)
		} else {
			whole = append(whole, r)
		}
	}
	n := 0
	seen := map[string]bool{}
	for _, c := range counts {
		n += c.N
		seen[c.Path] = true
		if c.Minute%minuteMs != 0 || c.Method != "GET" || c.Status != 200 || c.IP != "" || c.Headers != nil {
			t.Errorf("count %+v", c)
		}
	}
	if n != 30 || len(seen) != 3 || len(counts) > 6 {
		t.Errorf("counts %+v", counts)
	}
	if len(whole) != 4 {
		t.Fatalf("whole %+v", whole)
	}
	first := whole[0]
	if first.Path != "/pricing" || first.Status != 200 || first.IP != "127.0.0.1" || first.Host != strings.TrimPrefix(srv.URL, "http://") || first.Method != "GET" {
		t.Errorf("bot record %+v", first)
	}
	if !reflect.DeepEqual(first.Headers, map[string]string{"user-agent": gptbot, "accept": "*/*"}) {
		t.Errorf("headers %v", first.Headers)
	}
	if whole[1].Status != 404 || whole[2].Status != 401 || whole[3].Headers["user-agent"] != "curl/8.7.1" || whole[3].IP != "127.0.0.1" {
		t.Errorf("records %+v", whole[1:])
	}
	for _, p := range in.posts() {
		if p.auth != "Bearer "+secretKey || p.ua != "coldread-go/"+Version || p.body.V != 1 {
			t.Errorf("post %+v", p)
		}
	}
	raw, _ := json.Marshal(recs)
	for _, never := range []string{"secret", "hunter2", "nope", "utm", "a=b"} {
		if strings.Contains(string(raw), never) {
			t.Errorf("sent %q", never)
		}
	}
	if s := tr.Stats(); s.Sent != 34 || s.Dropped != 0 {
		t.Errorf("stats %+v", s)
	}
}

func TestPeopleFullSendsEveryone(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{People: "full", FlushInterval: time.Minute}, nil)
	srv := site(tr)
	defer srv.Close()
	get(t, srv, "/", person)
	get(t, srv, "/logo.png", person)
	tr.Close()
	recs := in.records(t)
	if len(recs) != 2 || recs[0].Source != "http" || recs[0].Headers["accept-language"] != "en-US" || recs[0].Headers["cookie"] != "" {
		t.Errorf("%+v", recs)
	}
}

// queued: what the Tracker holds, without sending.
func queued(tr *Tracker) []any {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]any(nil), tr.queue...)
}

func TestStatusAsSent(t *testing.T) {
	tr, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, People: "full"}, nil)
	for _, c := range []struct {
		name    string
		handler http.HandlerFunc
		want    int
	}{
		{"nothing written: net/http sends 200", func(w http.ResponseWriter, r *http.Request) {}, 200},
		{"a body", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("x")) }, 200},
		{"404", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404); w.WriteHeader(500) }, 404},
		{"early hints, then 201", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(103); w.WriteHeader(201) }, 201},
		{"flushed", func(w http.ResponseWriter, r *http.Request) { w.(http.Flusher).Flush() }, 200},
		{"ReadFrom", func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(w, strings.NewReader("x")) }, 200},
		{"redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/b", 308) }, 308},
	} {
		before := len(queued(tr))
		rec := httptest.NewRecorder()
		tr.Middleware(c.handler).ServeHTTP(rec, httptest.NewRequest("GET", "/a?q=1", nil))
		q := queued(tr)
		if len(q) != before+1 {
			t.Fatalf("%s: queued %d", c.name, len(q)-before)
		}
		if got := q[len(q)-1].(*httpRecord); got.Status != c.want || got.Path != "/a" {
			t.Errorf("%s: %+v", c.name, got)
		}
	}

	// A panic: re-raised untouched, reported without a status unless one was sent.
	for want, h := range map[int]http.HandlerFunc{
		0:   func(w http.ResponseWriter, r *http.Request) { panic("boom") },
		500: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); panic("boom") },
	} {
		before := len(queued(tr))
		func() {
			defer func() {
				if p := recover(); p != "boom" {
					t.Errorf("panic %v", p)
				}
			}()
			tr.Middleware(h).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/p", nil))
		}()
		q := queued(tr)
		if len(q) != before+1 || q[len(q)-1].(*httpRecord).Status != want {
			t.Errorf("panic after %d: %+v", want, q[len(q)-1])
		}
	}
}

func TestStreamingAndHijackStillWork(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: time.Minute, People: "full"}, nil)
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Errorf("ResponseController can't reach the server's writer: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: 1\n\n")
		if err := rc.Flush(); err != nil {
			t.Error(err)
		}
		<-release
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: Upgrade\r\n\r\nhi")
		_ = buf.Flush()
		conn.Close()
	})
	srv := httptest.NewServer(tr.Middleware(mux))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(res.Body).ReadString('\n')
	if line != "data: 1\n" {
		t.Errorf("first event %q", line)
	}
	// Reported once the header went out, while the stream is still open.
	eventually(t, "the stream reported", func() bool { return len(queued(tr)) == 1 })
	close(release)
	res.Body.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nUser-Agent: "+gptbot+"\r\n\r\n")
	got, _ := io.ReadAll(conn)
	conn.Close()
	if !strings.HasSuffix(string(got), "hi") {
		t.Errorf("hijacked %q", got)
	}
	tr.Close()
	recs := in.records(t)
	sort.Slice(recs, func(i, j int) bool { return recs[i].Path < recs[j].Path })
	if len(recs) != 2 || recs[0].Path != "/events" || recs[0].Status != 200 || recs[1].Path != "/ws" || recs[1].Status != 0 {
		t.Errorf("%+v", recs)
	}
}

func TestClientIP(t *testing.T) {
	t.Setenv("COLDREAD_TRUST_PROXY", "")
	t.Setenv("VERCEL", "")
	for _, c := range []struct {
		h      map[string]string
		remote string
		want   string
	}{
		// A visitor can write any header: a forged address never wins over the connection's.
		{map[string]string{"Cf-Connecting-Ip": "66.249.66.1", "X-Real-Ip": "2.2.2.2", "X-Forwarded-For": "3.3.3.3"}, "4.4.4.4:5", "4.4.4.4"},
		{nil, "4.4.4.4:5", "4.4.4.4"},
		{nil, "[2001:db8::1]:443", "2001:db8::1"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		for k, v := range c.h {
			r.Header.Set(k, v)
		}
		if got := ClientIP(r); got != c.want {
			t.Errorf("ClientIP(%v, %s) = %s, want %s", c.h, c.remote, got, c.want)
		}
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.2:5"
	r.Header.Set("X-Forwarded-For", "66.249.66.1, 3.3.3.3")
	t.Setenv("COLDREAD_TRUST_PROXY", "1")
	if got := ClientIP(r); got != "3.3.3.3" {
		t.Errorf("trusted: %s", got)
	}

	// TrustProxy, the option.
	t.Setenv("COLDREAD_TRUST_PROXY", "")
	trusting, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, People: "full", TrustProxy: 2}, nil)
	trusting.Middleware(http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), r)
	if got := queued(trusting)[0].(*httpRecord); got.IP != "66.249.66.1" {
		t.Errorf("%+v", got)
	}
	plain, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, People: "full"}, nil)
	plain.Middleware(http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), r)
	if got := queued(plain)[0].(*httpRecord); got.IP != "10.0.0.2" {
		t.Errorf("%+v", got)
	}

	// The shared cases.
	var cases struct {
		ClientIP []struct {
			Headers    map[string]string `json:"headers"`
			Peer       *string           `json:"peer"`
			TrustProxy int               `json:"trustProxy"`
			Vercel     bool              `json:"vercel"`
			Want       *string           `json:"want"`
		} `json:"clientIp"`
		Hops []struct {
			In   any `json:"in"`
			Want int `json:"want"`
		} `json:"trustProxyHops"`
	}
	if err := json.Unmarshal(mustRead(t, "testdata/parity.json"), &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.ClientIP) < 5 || len(cases.Hops) < 5 {
		t.Fatal("no cases")
	}
	for _, c := range cases.ClientIP {
		h := http.Header{}
		for k, v := range c.Headers {
			h.Set(k, v)
		}
		peer, want := "", ""
		if c.Peer != nil {
			peer = *c.Peer
		}
		if c.Want != nil {
			want = *c.Want
		}
		if got := clientIP(h, peer, c.TrustProxy, c.Vercel); got != want {
			t.Errorf("clientIP(%+v) = %q, want %q", c, got, want)
		}
	}
	for _, c := range cases.Hops {
		if got := trustProxyHops(c.In); got != c.Want {
			t.Errorf("trustProxyHops(%#v) = %d, want %d", c.In, got, c.Want)
		}
	}

	// The IP option, for hosts that put it elsewhere.
	tr, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, People: "full", IP: func(r *http.Request) string { return r.Header.Get("Fly-Client-Ip") }}, nil)
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Fly-Client-Ip", "5.5.5.5")
	r.Header.Set("Cf-Connecting-Ip", "1.1.1.1")
	r.Header.Set("X-Forwarded-Host", "Example.COM")
	tr.Middleware(http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), r)
	if got := queued(tr)[0].(*httpRecord); got.IP != "5.5.5.5" || got.Host != "example.com" {
		t.Errorf("%+v", got)
	}
}

func TestToRecord(t *testing.T) {
	now := time.UnixMilli(1_760_000_000_000)
	h := http.Header{}
	h.Set("User-Agent", strings.Repeat("a", 3000))
	h.Set("Signature", strings.Repeat("s", 3000))
	h.Set("Signature-Input", "sig1=();created=1")
	h.Add("Accept", "text/html")
	h.Add("Accept", "*/*")
	h["sec-fetch-dest"] = []string{"document"} // a hand-built map
	r := toRecord(RequestFacts{Method: "get", URL: "https://example.com/a b?x=1", Header: h, IP: "203.0.113.9, 10.0.0.1", Host: "Example.com:8443", Status: 404}, now)
	want := &httpRecord{Source: "http", TS: now.UnixMilli(), Method: "GET", Path: "/a%20b", IP: "203.0.113.9", Host: "example.com:8443", Status: 404,
		Headers: map[string]string{"user-agent": strings.Repeat("a", 2048), "signature-input": "sig1=();created=1", "accept": "text/html, */*", "sec-fetch-dest": "document"}}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("got  %+v\nwant %+v", r, want)
	}
	if toRecord(RequestFacts{Method: "NOT A METHOD"}, now) != nil || toRecord(RequestFacts{Method: "PROPFIND", URL: "/"}, now) == nil {
		t.Error("method check")
	}
	empty := toRecord(RequestFacts{IP: "not-an-ip", Host: "bad host!", Status: 99, Time: now.Add(time.Second)}, now)
	if empty.IP != "" || empty.Host != "" || empty.Status != 0 || empty.Method != "GET" || empty.Path != "/" || empty.TS != now.UnixMilli()+1000 || len(empty.Headers) != 0 {
		t.Errorf("%+v", empty)
	}
	if v6 := toRecord(RequestFacts{IP: "2001:DB8::1"}, now); v6.IP != "2001:DB8::1" {
		t.Errorf("v6 %+v", v6)
	}
	if got := cut("aé", 2); got != "a" {
		t.Errorf("cut mid-character: %q", got)
	}
}

func TestPathOf(t *testing.T) {
	for in, want := range map[string]string{
		"/a?b=1":                        "/a",
		"/a#frag":                       "/a",
		"":                              "/",
		"*":                             "/",
		"https://example.com":           "/",
		"https://example.com/x/y?z":     "/x/y",
		"/tab\there":                    "/tab%09here",
		"/nb sp":                        "/nb%C2%A0sp",
		"/é":                            "/é",
		"/" + strings.Repeat("p", 3000): "/" + strings.Repeat("p", 2047),
	} {
		if got := pathOf(in); got != want {
			t.Errorf("pathOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCountsPerPathMethodStatusAndMinute(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: time.Hour}, nil)
	// Minutes still to come, so none closes mid-test.
	m := time.Now().UnixMilli()/minuteMs*minuteMs + 2*minuteMs
	h := http.Header{"User-Agent": {chrome}, "Accept": {"text/html"}, "Accept-Language": {"en"}}
	for _, c := range []struct {
		method string
		status int
	}{{"GET", 200}, {"GET", 200}, {"GET", 404}, {"HEAD", 200}} {
		tr.Track(RequestFacts{Method: c.method, URL: "/a?utm=x", Header: h, Status: c.status, Time: time.UnixMilli(m + 1000)})
	}
	tr.Track(RequestFacts{Method: "GET", URL: "/a", Header: h, Status: 200, Time: time.UnixMilli(m + minuteMs)})
	if len(queued(tr)) != 0 {
		t.Fatal("open minutes were queued")
	}
	tr.Flush(context.Background())
	var got []string
	for _, r := range in.records(t) {
		got = append(got, strings.Join([]string{r.Method, itoa(r.Status), itoa(int(r.Minute - m)), itoa(r.N)}, " "))
	}
	sort.Strings(got)
	if want := []string{"GET 200 0 2", "GET 200 60000 1", "GET 404 0 1", "HEAD 200 0 1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestCountsAreBounded(t *testing.T) {
	tr, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, MaxQueue: 5000}, nil)
	m := time.Now().UnixMilli()/minuteMs*minuteMs + 2*minuteMs
	h := http.Header{"User-Agent": {chrome}, "Accept": {"text/html"}, "Accept-Language": {"en"}}
	for i := 0; i < maxBuckets+1; i++ {
		tr.Track(RequestFacts{URL: "/p" + itoa(i), Header: h, Time: time.UnixMilli(m)})
	}
	tr.mu.Lock()
	held, q := len(tr.counts), len(tr.queue)
	tr.mu.Unlock()
	if held != 1 || q != maxBuckets {
		t.Errorf("held %d, queued %d", held, q)
	}
}

func TestQueueIsBounded(t *testing.T) {
	tr, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, MaxQueue: 3, BatchSize: 500}, nil)
	for i := 0; i < 5; i++ {
		tr.Track(RequestFacts{URL: "/", Header: http.Header{"User-Agent": {gptbot}}})
	}
	if len(queued(tr)) != 3 || tr.Stats().Dropped != 2 {
		t.Errorf("queued %d, %+v", len(queued(tr)), tr.Stats())
	}
}

func TestSendsAFullBatchWithoutWaiting(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: time.Hour, BatchSize: 3}, nil)
	for i := 0; i < 7; i++ {
		tr.Track(RequestFacts{URL: "/", Header: http.Header{"User-Agent": {gptbot}}})
	}
	eventually(t, "two full batches", func() bool { return len(in.posts()) == 2 })
	time.Sleep(50 * time.Millisecond)
	if len(in.posts()) != 2 || len(queued(tr)) != 1 {
		t.Errorf("posts %d, queued %d", len(in.posts()), len(queued(tr)))
	}
	start := time.Now()
	tr.Close() // sends the rest at once
	if len(in.posts()) != 3 || time.Since(start) > time.Second {
		t.Errorf("close: posts %d in %s", len(in.posts()), time.Since(start))
	}
	tr.Track(RequestFacts{URL: "/", Header: http.Header{"User-Agent": {gptbot}}})
	if len(queued(tr)) != 0 {
		t.Error("tracked after Close")
	}
}

func TestFlushIntervalSends(t *testing.T) {
	in := newIngest(t)
	tr, _ := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: 50 * time.Millisecond}, nil)
	tr.Track(RequestFacts{URL: "/", Header: http.Header{"User-Agent": {gptbot}}})
	eventually(t, "sent within the interval", func() bool { return len(in.records(t)) == 1 })
}

func bot() RequestFacts { return RequestFacts{URL: "/", Header: http.Header{"User-Agent": {gptbot}}} }

func send(t *testing.T, tr *Tracker) {
	t.Helper()
	tr.Track(bot())
	tr.Flush(context.Background())
}

func pausedFor(tr *Tracker) time.Duration {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return time.Until(tr.pausedTill)
}

func TestKeyRejectedPausesTenMinutes(t *testing.T) {
	in := newIngest(t)
	in.set(401, `{"error":"Unknown or revoked key."}`)
	tr, out := testTracker(t, in.endpoint(), TrackerOptions{}, nil)
	send(t, tr)
	if !out.has("[coldread] key rejected (Unknown or revoked key.); paused 10 minutes.") || pausedFor(tr) < 9*time.Minute {
		t.Errorf("%v, paused %s", out.all(), pausedFor(tr))
	}
	tr.Track(bot())
	if tr.Stats().Dropped != 2 || len(in.posts()) != 1 {
		t.Errorf("%+v", tr.Stats())
	}
}

func TestBlockedBeforeColdreadIsNotAKeyProblem(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(403)
		_, _ = io.WriteString(w, "<html>Just a moment...</html>")
	}))
	defer srv.Close()
	tr, out := testTracker(t, srv.URL+"/api/ingest", TrackerOptions{}, nil)
	send(t, tr)
	if want := "[coldread] blocked before reaching Coldread (HTTP 403, text/html; a firewall or bot challenge?). Not a key problem."; !reflect.DeepEqual(out.all(), []string{want}) {
		t.Errorf("%q", out.all())
	}
	if p := pausedFor(tr); p > 2*time.Second {
		t.Errorf("paused %s: backoff, not the key's 10 minutes", p)
	}
}

func TestNetworkFailuresWarnOncePerCause(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	tr, out := testTracker(t, "http://"+addr+"/api/ingest", TrackerOptions{}, nil)
	send(t, tr)
	tr.mu.Lock()
	tr.pausedTill = time.Time{}
	tr.mu.Unlock()
	send(t, tr)
	want := "[coldread] couldn't reach http://" + addr + "/api/ingest (ECONNREFUSED); retrying with the next requests."
	if !reflect.DeepEqual(out.all(), []string{want}) {
		t.Errorf("%q", out.all())
	}
	tr.mu.Lock()
	failures := tr.failures
	tr.mu.Unlock()
	if failures != 2 || pausedFor(tr) < time.Second || pausedFor(tr) > 2*time.Second {
		t.Errorf("failures %d, paused %s", failures, pausedFor(tr))
	}
}

func TestATimeoutIsDroppedNotRetried(t *testing.T) {
	in := newIngest(t)
	in.delay = 300 * time.Millisecond
	tr, out := testTracker(t, in.endpoint(), TrackerOptions{Timeout: 50 * time.Millisecond}, nil)
	send(t, tr)
	if !out.has("(timed out after 50ms); retrying with the next requests.") || tr.Stats().Dropped != 1 {
		t.Errorf("%q %+v", out.all(), tr.Stats())
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	in := newIngest(t)
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, in.endpoint(), 307)
	}))
	defer srv.Close()
	tr, out := testTracker(t, srv.URL, TrackerOptions{}, nil)
	send(t, tr)
	if hits != 1 || len(in.posts()) != 0 || !out.has("the endpoint redirected; not following it.") {
		t.Errorf("hits %d, followed %d, %q", hits, len(in.posts()), out.all())
	}
}

func TestRateLimitedPausesAsAsked(t *testing.T) {
	in := newIngest(t)
	in.set(429, `{"error":"slow down"}`)
	tr, out := testTracker(t, in.endpoint(), TrackerOptions{}, nil)
	send(t, tr)
	if p := pausedFor(tr); p < 50*time.Second || p > 61*time.Second {
		t.Errorf("paused %s, %q", p, out.all())
	}
	// Round 5: what a pause drops is said, once per cause.
	for i := 0; i < 50; i++ {
		tr.Track(bot())
	}
	if s := tr.Stats(); s.Dropped != 51 || !reflect.DeepEqual(out.all(), []string{"[coldread] dropped 1 record (paused after 429)."}) {
		t.Errorf("%+v %q", s, out.all())
	}
}

func TestDropsAreNeverSilent(t *testing.T) {
	in := newIngest(t)
	in.set(503, "")
	tr, out := testTracker(t, in.endpoint(), TrackerOptions{FlushInterval: time.Hour}, nil)
	tr.Track(bot())
	tr.Track(bot())
	tr.Flush(context.Background())
	tr.Track(bot())
	if !reflect.DeepEqual(out.all(), []string{"[coldread] dropped 2 records (paused after HTTP 503)."}) {
		t.Errorf("%q", out.all())
	}
	full, out2 := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour, MaxQueue: 3}, nil)
	for i := 0; i < 10; i++ {
		full.Track(bot())
	}
	if s := full.Stats(); s.Dropped != 7 || !reflect.DeepEqual(out2.all(), []string{"[coldread] dropped 1 record (queue full)."}) {
		t.Errorf("%+v %q", s, out2.all())
	}
}

func TestRefusedRecordsAndTheConnectedLine(t *testing.T) {
	connectedSaid.Store(false)
	in := newIngest(t)
	in.set(202, `{"accepted":0,"rejected":[{"i":0,"error":"bad record"}]}`)
	tr, out := testTracker(t, in.endpoint(), TrackerOptions{}, nil)
	send(t, tr)
	if !reflect.DeepEqual(out.all(), []string{"[coldread] 1 of 1 records refused (bad record)."}) {
		t.Errorf("%q", out.all())
	}
	if s := tr.Stats(); s.Sent != 0 || s.Dropped != 1 {
		t.Errorf("%+v", s)
	}
	in.set(202, `{"accepted":1,"rejected":[]}`)
	send(t, tr)
	send(t, tr)
	other, out2 := testTracker(t, in.endpoint(), TrackerOptions{}, nil)
	send(t, other)
	n := 0
	for _, l := range append(out.all(), out2.all()...) {
		if l == ConnectedLine {
			n++
		}
	}
	if n != 1 {
		t.Errorf("connected said %d times: %q %q", n, out.all(), out2.all())
	}
}

type marked struct{ http.Handler }

func TestWhenItSendsNothing(t *testing.T) {
	in := newIngest(t)
	for _, c := range []struct {
		key, endpoint string
		env           map[string]string
		testRun       bool
		want          string
	}{
		{"", in.endpoint(), nil, false, "[coldread] no key (set COLDREAD_KEY); not sending."},
		{"cr_pub_0123456789abcdef0123456789abcdef", in.endpoint(), nil, false, "[coldread] that's a public key (cr_pub_), which is for MCP servers and CLIs. Use the site's secret key (cr_sec_); not sending."},
		{secretKey, in.endpoint(), map[string]string{"DO_NOT_TRACK": "1"}, false, "[coldread] disabled (DO_NOT_TRACK); not sending."},
		{secretKey, in.endpoint(), map[string]string{"COLDREAD_DISABLED": "true"}, false, "[coldread] disabled (COLDREAD_DISABLED); not sending."},
		{secretKey, "", nil, true, "[coldread] test run; not sending (set COLDREAD_ENDPOINT to send)."},
		{secretKey, "", map[string]string{"NODE_ENV": "test"}, false, "[coldread] test run; not sending (set COLDREAD_ENDPOINT to send)."},
	} {
		out := &lines{}
		env := c.env
		if env == nil {
			env = map[string]string{}
		}
		tr := newTracker(TrackerOptions{Key: c.key, Endpoint: c.endpoint}, trackerInternals{env: env, write: out.write, testRun: bptr(c.testRun)})
		h := &marked{http.NotFoundHandler()}
		if tr.Enabled() || tr.Middleware(h) != http.Handler(h) {
			t.Errorf("%s: enabled", c.want)
		}
		tr.Track(bot())
		tr.Flush(context.Background())
		tr.Close()
		if !reflect.DeepEqual(out.all(), []string{c.want}) {
			t.Errorf("got %q, want %q", out.all(), c.want)
		}
	}
	if len(in.posts()) != 0 {
		t.Error("sent")
	}
	// Off switches set to a falsy word don't count; the key and endpoint come from env.
	tr := newTracker(TrackerOptions{}, trackerInternals{env: map[string]string{"DO_NOT_TRACK": "0", "COLDREAD_KEY": secretKey, "COLDREAD_ENDPOINT": in.endpoint()}, write: (&lines{}).write, testRun: bptr(true)})
	if !tr.Enabled() || tr.endpoint != in.endpoint() {
		t.Error("env key and endpoint")
	}
	tr.Close()
	var nilTracker *Tracker
	nilTracker.Track(bot())
	nilTracker.Close()
	_ = nilTracker.Middleware(http.NotFoundHandler())
	_ = (&Tracker{}).Stats()
}

func TestTrackNeverPanics(t *testing.T) {
	tr, _ := testTracker(t, "http://127.0.0.1:1/api/ingest", TrackerOptions{FlushInterval: time.Hour}, nil)
	tr.Track(RequestFacts{}) // nil header
	tr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(httptest.NewRecorder(), &http.Request{Method: "GET", Header: http.Header{}})
	if len(queued(tr)) != 2 {
		t.Errorf("queued %d", len(queued(tr)))
	}
}
