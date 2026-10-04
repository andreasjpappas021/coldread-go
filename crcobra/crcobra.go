// Package crcobra reports cobra commands to Coldread.
//
//	func main() {
//		cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: version})
//		crcobra.Execute(cr, rootCmd)
//	}
//
// Execute runs the root command, tracks the command cobra ran with the
// flags set on it, and exits through the client: 0, or 1 when the command
// returned an error (cobra has printed it). A command line cobra refused
// (an unknown command or flag, a missing or bad argument, a required flag
// not set) is recorded as the command that was tried, with exit 2; the
// process still exits 1, as cobra's own Execute does. A panic is recorded
// (exit 2, Go's code for it) and raised again. Help and version runs aren't
// sent. A command that calls os.Exit itself (cobra.CheckErr, log.Fatal)
// exits before anything is sent: return an error from RunE instead, or
// call cr.Exit.
package crcobra

import (
	"os"
	"strconv"
	"strings"

	"coldread.apappas.dev/go"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Execute is root.Execute() reported: it tracks the command that ran and
// exits with its code. It does not return.
func Execute(cr *coldread.Client, root *cobra.Command) {
	defer recovered(cr, root)
	// Named before it runs, so a run killed midway (an agent's timeout,
	// with CaptureSignals or OwnSignals) is recorded as that command;
	// Report names it again, with its flags.
	if cmd, _, err := root.Find(argsOf(root)); err == nil && cmd != nil {
		cr.Track(Path(cmd))
	}
	cmd, err := root.ExecuteC()
	cr.Exit(Report(cr, cmd, err))
}

// Report names the run to the client once cobra is done with it, and
// returns the exit code to use: 0, or 1 for an error. A command line cobra
// refused is cr.ParseError (the word it didn't know for an unknown
// command); anything else is Track. For a framework that wraps cobra
// (bep/simplecobra) or your own executor: Report the *cobra.Command that
// ran and the error, then cr.Exit(code).
func Report(cr *coldread.Client, cmd *cobra.Command, err error) int {
	if err == nil {
		switch {
		case cmd != nil && helpOnly(cmd):
			// Shell completion, or a parent with no Run of its own called
			// bare: cobra printed help. A help run: not sent.
			cr.Track("help")
		case cmd != nil && !cmd.Runnable() && len(cmd.Flags().Args()) > 0:
			// A parent with no Run of its own, given a word it doesn't know
			// ("db nosuch"): cobra prints its help and exits 0. Recorded
			// as that unknown command (exit 2).
			cr.ParseError(Path(cmd) + " " + cmd.Flags().Args()[0])
		default:
			Track(cr, cmd)
		}
		return 0
	}
	if word, ok := UnknownCommand(err); ok {
		path := ""
		if cmd != nil && cmd.HasParent() {
			path = Path(cmd) + " "
		}
		cr.ParseError(path + word)
		return 1
	}
	if IsParseError(err) {
		if cmd == nil {
			cr.ParseError("")
		} else {
			cr.ParseError(Path(cmd), setFlags(cmd)...)
		}
		return 1
	}
	Track(cr, cmd)
	return 1
}

// helpOnly: what cobra ran prints help or completions and does nothing
// else: shell completion (completion <shell>, __complete) or a parent with
// no Run of its own called with no word after it.
func helpOnly(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		switch c.Name() {
		case "completion", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
			if c.Parent() != nil && !c.Parent().HasParent() {
				return true
			}
		}
	}
	return !cmd.Runnable() && len(cmd.Flags().Args()) == 0
}

// cobra's own words for a command line it refused: pflag's flag errors,
// cobra's argument validators, and required flags. Flag groups say "flags
// in the group [".
var parseErrors = []string{
	"unknown flag: ", "unknown shorthand flag: ", "flag needs an argument: ", "invalid argument ", "bad flag syntax: ",
	"accepts ", "requires at least ", "requires at most ", "required flag(s) ",
}

// IsParseError: err is cobra refusing the command line (an unknown command
// or flag, a missing or bad argument, a required flag not set), not the
// command failing.
func IsParseError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := UnknownCommand(err); ok {
		return true
	}
	msg := err.Error()
	for _, p := range parseErrors {
		if strings.HasPrefix(msg, p) {
			return true
		}
	}
	return strings.Contains(msg, "flags in the group [")
}

// UnknownCommand: the word cobra didn't know, from its `unknown command "x"
// for "acme"`.
func UnknownCommand(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "unknown command ") {
		return "", false
	}
	q := strings.TrimPrefix(msg, "unknown command ")
	if end := strings.Index(q, `" for "`); end >= 0 {
		if word, err := strconv.Unquote(q[:end+1]); err == nil {
			return word, true
		}
	}
	return "", true
}

// recovered: a panic in a command is recorded as exit 2, under the command
// cobra was running, and raised again.
func recovered(cr *coldread.Client, root *cobra.Command) {
	r := recover()
	if r == nil {
		return
	}
	if cmd, _, err := root.Find(argsOf(root)); err == nil && cmd != nil {
		Track(cr, cmd)
	}
	cr.Finish(2)
	panic(r)
}

// argsOf: what root.Execute parses (os.Args, or SetArgs's; cobra doesn't
// say, so the process's).
func argsOf(*cobra.Command) []string {
	if len(os.Args) > 1 {
		return os.Args[1:]
	}
	return nil
}

// Track names cmd to the client: its path below the root ("deploy
// preview"; the root's own name when it ran itself) and the flags set on
// it, inherited ones included, by name ("--prod"), never their values. Call
// it yourself when you run cobra your own way: a framework that wraps it
// (bep/simplecobra: the *cobra.Command of the Commandeer that ran) or your
// own executor, with the command that ran, then cr.Exit(code). Report does
// it, and tells refused command lines apart.
func Track(cr *coldread.Client, cmd *cobra.Command) {
	if cmd == nil {
		return
	}
	cr.Track(Path(cmd), setFlags(cmd)...)
}

func setFlags(cmd *cobra.Command) []string {
	var flags []string
	cmd.Flags().Visit(func(f *pflag.Flag) { flags = append(flags, "--"+strings.ToLower(f.Name)) })
	return flags
}

// Path is cmd's command path without the root's name, or the root's name
// for the root itself.
func Path(cmd *cobra.Command) string {
	path := cmd.CommandPath()
	if !cmd.HasParent() {
		return path
	}
	return strings.TrimPrefix(path, cmd.Root().Name()+" ")
}
