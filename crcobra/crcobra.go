// Package crcobra reports cobra commands to Coldread.
//
//	func main() {
//		cr := coldread.New(coldread.Options{Key: "cr_pub_...", Tool: "acme", Version: version})
//		crcobra.Execute(cr, rootCmd)
//	}
//
// Execute runs the root command, tracks the command cobra ran with the
// flags set on it, and exits through the client: 0, or 1 when the command
// returned an error (cobra has printed it). A command that calls os.Exit
// itself (cobra.CheckErr, log.Fatal) exits before anything is sent: return
// an error from RunE instead, or call cr.Exit.
package crcobra

import (
	"strings"

	"coldread.apappas.dev/go"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Execute is root.Execute() reported: it tracks the command that ran and
// exits with its code. It does not return.
func Execute(cr *coldread.Client, root *cobra.Command) {
	cmd, err := root.ExecuteC()
	Track(cr, cmd)
	if err != nil {
		cr.Exit(1)
	}
	cr.Exit(0)
}

// Track names cmd to the client: its path below the root ("deploy
// preview"; the root's own name when it ran itself) and the flags set on
// it, inherited ones included, by name ("--prod"), never their values. Call
// it yourself when you run cobra your own way.
func Track(cr *coldread.Client, cmd *cobra.Command) {
	if cmd == nil {
		return
	}
	var flags []string
	cmd.Flags().Visit(func(f *pflag.Flag) { flags = append(flags, "--"+f.Name) })
	cr.Track(Path(cmd), flags...)
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
