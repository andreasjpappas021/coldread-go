// Package crurfave reports urfave/cli (v3) commands to Coldread.
//
//	func main() {
//		cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: version})
//		crurfave.Execute(context.Background(), cr, cmd, os.Args)
//	}
//
// Execute runs the command, tracks the one that ran with the flags set on
// it, and exits through the client with its code: 0; an error's own code
// when it has one (cli.Exit); else 1, after printing the error. A command
// line urfave/cli refused (a bad flag, an unknown command, a required flag
// not set) is recorded as the command that was tried, with the code the
// process exits with, as urfave/cli would. A panic is recorded (exit 2) and
// raised again. Help and version runs (help, h, help <cmd>, <group> help,
// a group called bare) aren't sent.
package crurfave

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"coldread.apappas.dev/go"
	"github.com/urfave/cli/v3"
)

// Execute is root.Run(ctx, args) reported, then an exit with its code. It
// does not return.
func Execute(ctx context.Context, cr *coldread.Client, root *cli.Command, args []string) {
	ran := false
	// Commands with no Action of their own: called bare, urfave/cli shows
	// their help.
	bare := map[*cli.Command]bool{}
	var walk func(*cli.Command)
	walk = func(c *cli.Command) {
		if c.Action == nil {
			bare[c] = true
		}
		for _, sub := range c.Commands {
			walk(sub)
		}
	}
	walk(root)
	instrument(cr, root, &ran)
	defer func() {
		if r := recover(); r != nil {
			cr.Finish(2)
			panic(r)
		}
	}()
	// urfave/cli exits on its own for errors that carry a code: name the
	// run first (a refused command line is a parse error), then send.
	exitErr := root.ExitErrHandler
	root.ExitErrHandler = func(ctx context.Context, cmd *cli.Command, err error) {
		classify(cr, root, args, err, ran)
		if exitErr != nil {
			exitErr(ctx, cmd, err)
			return
		}
		cli.HandleExitCoder(err)
	}
	exiter := cli.OsExiter
	cli.OsExiter = func(code int) {
		cr.Finish(code)
		exiter(code)
	}
	err := root.Run(ctx, args)
	cli.OsExiter = exiter
	if err == nil {
		if helpRun(root, args, bare) {
			cr.Track("help") // a help run: not sent
		}
		cr.Exit(0)
	}
	classify(cr, root, args, err, ran)
	var coder cli.ExitCoder
	if errors.As(err, &coder) {
		cr.Exit(coder.ExitCode())
	}
	var w io.Writer = os.Stderr
	if root.ErrWriter != nil {
		w = root.ErrWriter
	}
	fmt.Fprintln(w, err)
	cr.Exit(1)
}

// classify: an error urfave/cli returned before any command's Before ran
// is a refused command line (a flag it didn't know, a bad value, flags
// that exclude each other); "No help topic for 'x'" is a command it didn't
// know; a required flag not set is the tracked command's. Those are
// cr.ParseError (exit 2). Anything else is the command failing.
func classify(cr *coldread.Client, root *cli.Command, args []string, err error, ran bool) {
	if err == nil {
		return
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "No help topic for '"):
		cr.ParseError(strings.TrimSpace(Attempt(root, args) + " " + unknownWord(msg)))
	case !ran:
		cr.ParseError(Attempt(root, args))
	case strings.HasPrefix(msg, "Required flag"):
		cr.ParseError("")
	}
}

// unknownWord: the command urfave/cli didn't know, from its "No help topic
// for 'x'" (a suggestion may follow it).
func unknownWord(msg string) string {
	word := strings.TrimPrefix(msg, "No help topic for '")
	if end := strings.IndexByte(word, '\''); end >= 0 {
		word = word[:end]
	}
	return word
}

// helpRun: urfave/cli's help command ran (help, h, help <cmd>, <group>
// help), or a command with no Action of its own was called with no word
// after it (urfave/cli shows its help).
func helpRun(root *cli.Command, args []string, bare map[*cli.Command]bool) bool {
	if len(args) == 0 {
		return false
	}
	cmd := root
	for _, a := range args[1:] {
		if a == "--" {
			break
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		sub := cmd.Command(a)
		if sub == nil {
			return false
		}
		if sub.Name == "help" { // urfave/cli's own (alias h), at any level
			return true
		}
		cmd = sub
	}
	return bare[cmd]
}

// Attempt is the command path args name, as far as root's commands go:
// each word matched to a command urfave/cli defines (aliases included), so
// nothing but command names comes out; flags are skipped. "" for the root.
func Attempt(root *cli.Command, args []string) string {
	if len(args) == 0 {
		return ""
	}
	cmd := root
	var path []string
	for _, a := range args[1:] {
		if a == "--" {
			break
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		sub := cmd.Command(a)
		if sub == nil {
			break
		}
		path = append(path, sub.Name)
		cmd = sub
	}
	return strings.Join(path, " ")
}

// Instrument gives root and every command under it a Before that tracks
// the command as it runs (keeping any Before it had). The innermost runs
// last, so it's the one sent.
func Instrument(cr *coldread.Client, root *cli.Command) { instrument(cr, root, nil) }

func instrument(cr *coldread.Client, root *cli.Command, ran *bool) {
	var walk func(*cli.Command)
	walk = func(c *cli.Command) {
		before := c.Before
		c.Before = func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if ran != nil {
				*ran = true
			}
			Track(cr, cmd)
			if before != nil {
				return before(ctx, cmd)
			}
			return ctx, nil
		}
		for _, sub := range c.Commands {
			walk(sub)
		}
	}
	walk(root)
}

// Track names cmd to the client: its path below the root ("deploy
// preview"; the root's own name when it ran itself) and the flags set on it
// or its parents, by name ("--prod"), never their values.
func Track(cr *coldread.Client, cmd *cli.Command) {
	if cmd == nil {
		return
	}
	var flags []string
	for _, c := range cmd.Lineage() {
		for _, f := range c.Flags {
			if names := f.Names(); len(names) > 0 && f.IsSet() {
				flags = append(flags, dashed(names[0]))
			}
		}
	}
	cr.Track(Path(cmd), flags...)
}

// Path is cmd's full name without the root's, or the root's name for the
// root itself.
func Path(cmd *cli.Command) string {
	full := cmd.FullName()
	root := cmd.Root()
	if root == cmd {
		return full
	}
	return strings.TrimPrefix(full, root.Name+" ")
}

func dashed(name string) string {
	if len(name) == 1 {
		return "-" + name
	}
	return "--" + strings.ToLower(name)
}
