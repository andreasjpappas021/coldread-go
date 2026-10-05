//go:build !windows && !plan9 && !js && !wasip1

package coldread

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// The signals that end a run someone (or an agent's timeout) killed. SIGINT
// isn't one: Ctrl-C is a person stopping it, and sends nothing.
var kills = []os.Signal{syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT}

// watchSignals, from the first Track to Finish (c.mu held), only when the
// CLI asked (signal.Notify turns a signal's default exit off, so Coldread
// never catches one unasked). CaptureSignals: a kill is saved to the spool
// as 128+n, then raised again at once, so the process dies as it would
// have. OwnSignals: the CLI's own handler runs as ever and the kill is only
// noted (the run reads 128+n when it ends), never raised again.
func (c *Client) watchSignals() {
	if c.sigCh != nil || c.off != "" || (!c.captureSignals && !c.ownSignals) {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, kills...)
	c.sigCh = ch
	go func() {
		sig, ok := <-ch
		if !ok {
			return
		}
		n := signalNumber(sig)
		// Seen, before waiting on the lock: a CLI whose own handler is
		// already in Finish reads it from here.
		c.sigSeen.Store(int32(n))
		c.mu.Lock()
		if c.killedBy == 0 {
			c.killedBy = n
		}
		c.mu.Unlock()
		if c.ownSignals {
			return
		}
		c.killed(n)
		signal.Stop(ch)
		_ = syscall.Kill(os.Getpid(), sig.(syscall.Signal))
	}()
}

// watchSignals, for a stdio MCP server that asked (FlushOnSignal) and has
// no handler of its own: its client shutting it down (SIGINT, then
// SIGTERM; or SIGTERM with stdin EOF) gets the spool sent first, 100 ms at
// most, then the signal is raised again. Without it nothing is caught: the
// events are in the spool already and go with the next start.
func (m *MCP) watchSignals() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-ch
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		m.Flush(ctx)
		cancel()
		signal.Stop(ch)
		_ = syscall.Kill(os.Getpid(), sig.(syscall.Signal))
	}()
}

// signalNumber is a signal's number (SIGTERM 15), 0 for anything else.
func signalNumber(s os.Signal) int {
	if n, ok := s.(syscall.Signal); ok {
		return int(n)
	}
	return 0
}

func (c *Client) unwatchSignals() {
	c.mu.Lock()
	ch := c.sigCh
	c.sigCh = nil
	c.mu.Unlock()
	if ch != nil {
		signal.Stop(ch)
		close(ch)
	}
}

// killed saves the run to the spool as exit 128+n, now (no time to send).
func (c *Client) killed(n int) {
	defer func() { _ = recover() }()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.command == "" {
		return
	}
	c.done = true
	command, flags := c.command, c.flags
	if c.off != "" || (!c.parseFailed && isHelpRun(command, flags, c.tool)) {
		return
	}
	record := c.record(command, flags, 128+n)
	if c.debug {
		c.write("[coldread] " + string(record) + "\n")
	}
	if c.verifyMode {
		c.write(VerifyPrefix + "killed (exit " + strconv.Itoa(128+n) + "); saved for the next run.\n")
	}
	if !c.debug || c.verifyMode {
		records := [][]byte{record}
		if rules := c.rulesToSend(); rules != nil {
			records = append(records, rules)
		}
		_, _ = c.s.save(records)
	}
}
