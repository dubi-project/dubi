package command

import (
	"os"
	"testing"
)

// CLI tests exercise context publication but must never change the developer's
// real ~/.dubi selection, even when they spin up several Nodes in one process.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "dubi-command-home-")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("HOME", home); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}
