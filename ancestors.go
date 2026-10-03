//go:build !windows

package coldread

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// The opt-in parent-process walk (Options.InferParent): the command lines
// of this process's ancestors, nearest first, for agents that set no marker
// in the environment (Copilot CLI, Aider, Windsurf). Only matched against
// the registry's process names; nothing here is ever sent.
//
// Linux reads /proc (no process spawned). macOS and other Unixes run ps
// once, plus once more for interpreters (node, bun, python) whose script is
// the real name. Windows: nothing. Bounded: 8 levels, 500 ms.

const maxAncestors = 8

var interpreterRe = regexp.MustCompile(`(?i)^(node|nodejs|bun|deno|python[\d.]*)$`)

func ancestors() [][]string {
	if runtime.GOOS == "linux" {
		return fromProc()
	}
	return fromPs()
}

func fromProc() [][]string {
	var out [][]string
	pid := os.Getppid()
	for i := 0; i < maxAncestors && pid > 1; i++ {
		raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
		if err != nil {
			break
		}
		var argv []string
		for _, a := range strings.Split(string(raw), "\x00") {
			if a != "" {
				argv = append(argv, a)
			}
		}
		out = append(out, argv)
		// "pid (comm) state ppid ...": comm can hold spaces and parens.
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			break
		}
		s := string(stat)
		i := strings.LastIndexByte(s, ')')
		if i < 0 || i+2 > len(s) {
			break
		}
		fields := strings.Fields(s[i+2:])
		if len(fields) < 2 {
			break
		}
		if pid, err = strconv.Atoi(fields[1]); err != nil {
			break
		}
	}
	return out
}

func ps(args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

var psRow = regexp.MustCompile(`^\s*(\d+)\s+(\d+)\s+(.+?)\s*$`)
var psArgs = regexp.MustCompile(`^\s*(\d+)\s+\S+\s+(\S+)`)

func fromPs() [][]string {
	type row struct {
		ppid int
		comm string
	}
	table := map[int]row{}
	for _, line := range strings.Split(ps("-A", "-o", "pid=,ppid=,comm="), "\n") {
		if m := psRow.FindStringSubmatch(line); m != nil {
			pid, _ := strconv.Atoi(m[1])
			ppid, _ := strconv.Atoi(m[2])
			table[pid] = row{ppid, m[3]}
		}
	}
	type link struct {
		pid  int
		comm string
	}
	var chain []link
	pid := os.Getppid()
	for i := 0; i < maxAncestors && pid > 1; i++ {
		r, ok := table[pid]
		if !ok {
			break
		}
		chain = append(chain, link{pid, r.comm})
		pid = r.ppid
	}
	// An interpreter's name says nothing; its first argument (the script) does.
	scripts := map[int]string{}
	var interpreted []string
	for _, c := range chain {
		base := c.comm
		if i := strings.LastIndexByte(base, '/'); i >= 0 {
			base = base[i+1:]
		}
		if interpreterRe.MatchString(base) {
			interpreted = append(interpreted, strconv.Itoa(c.pid))
		}
	}
	if len(interpreted) > 0 {
		for _, line := range strings.Split(ps("-o", "pid=,args=", "-p", strings.Join(interpreted, ",")), "\n") {
			if m := psArgs.FindStringSubmatch(line); m != nil {
				pid, _ := strconv.Atoi(m[1])
				scripts[pid] = m[2]
			}
		}
	}
	out := make([][]string, 0, len(chain))
	for _, c := range chain {
		if s, ok := scripts[c.pid]; ok {
			out = append(out, []string{c.comm, s})
		} else {
			out = append(out, []string{c.comm})
		}
	}
	return out
}
