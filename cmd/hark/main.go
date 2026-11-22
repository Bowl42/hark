// Command hark is the official binary: it includes only the built-in engines.
// See package harkcmd for how to plug in private engines.
package main

import "github.com/Bowl42/hark/harkcmd"

func main() {
	harkcmd.Run(harkcmd.Builtin())
}
