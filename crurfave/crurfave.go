// Package crurfave reports urfave/cli (v3) commands to Coldread.
//
//	func main() {
//		cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: version})
//		crurfave.Execute(context.Background(), cr, cmd, os.Args)
//	}
//
// Execute runs the command, tracks the one that ran with the flags set on
// it, and exits through the client with its code: 0; an error's own code
// when it has one (cli.Exit); else 1, after printing the error. Help and
// usage errors aren't tracked (no Before runs for them).
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
	Instrument(cr, root)
	// urfave/cli exits on its own for errors that carry a code: send first.
	exiter := cli.OsExiter
	cli.OsExiter = func(code int) {
		cr.Finish(code)
		exiter(code)
	}
	err := root.Run(ctx, args)
	cli.OsExiter = exiter
	if err == nil {
		cr.Exit(0)
	}
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

// Instrument gives root and every command under it a Before that tracks
// the command as it runs (keeping any Before it had). The innermost runs
// last, so it's the one sent.
func Instrument(cr *coldread.Client, root *cli.Command) {
	var walk func(*cli.Command)
	walk = func(c *cli.Command) {
		before := c.Before
		c.Before = func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
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
	return "--" + name
}
