//go:build windows || plan9 || js || wasip1

package coldread

import "os"

// No kill signals to watch here (Windows ends a process without one Go can
// catch and raise again). An MCP server's events are in the spool already.
func (c *Client) watchSignals()    {}
func (c *Client) unwatchSignals()  {}
func (m *MCP) watchSignals()       {}
func signalNumber(s os.Signal) int { return 0 }
