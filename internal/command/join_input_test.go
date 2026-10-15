package command

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"golang.org/x/sys/unix"
)

func TestJoinCredentialInputIsBoundedPrivateAndNeverEchoed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ticket")
	if err := os.WriteFile(path, []byte("djt1.test-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readJoinTicket(path, nil); err != nil || got != "djt1.test-secret" {
		t.Fatal("private file not read", err)
	}
	if got, err := readJoinTicket("-", strings.NewReader("djt1.test-secret\n")); err != nil || got != "djt1.test-secret" {
		t.Fatal("stdin not read", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readJoinTicket(path, nil); err == nil {
		t.Fatal("world-readable credential accepted")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{dir, link, fifo, "djt1.accidentally-pasted-secret"} {
		if _, err := readJoinTicket(bad, nil); err == nil || strings.Contains(err.Error(), "djt1.") {
			t.Fatal("unsafe input accepted/echoed")
		}
	}
	for _, input := range []string{"", strings.Repeat("a", identityenrollment.MaxJoinTicketSize+3)} {
		if _, err := readJoinTicket("-", strings.NewReader(input)); err == nil {
			t.Fatal("unbounded/empty stdin accepted")
		}
	}
}
