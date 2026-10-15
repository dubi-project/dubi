package command

import (
	"errors"
	"io"
	"os"
	"strings"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"golang.org/x/sys/unix"
)

// Credential input is file/stdin only, never a command-line bearer argument.
// Suppress raw path/IO errors: an accidentally pasted ticket used as a path
// must not be echoed into diagnostics. Reject devices/FIFOs before reading.
func readJoinTicket(path string, stdin io.Reader) (string, error) {
	if path == "" {
		return "", errors.New("a private --ticket-file is required before enrollment")
	}
	reader := stdin
	if path != "-" {
		file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return "", errors.New("cannot open private join credential file")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > identityenrollment.MaxJoinTicketSize+2 {
			return "", errors.New("join credential must be a nonempty private regular file (0600 or stricter), at most 32 KiB plus newline")
		}
		reader = file
	}
	if reader == nil {
		return "", errors.New("join credential stdin is unavailable")
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, identityenrollment.MaxJoinTicketSize+3))
	if err != nil {
		return "", errors.New("cannot read private join credential")
	}
	if len(encoded) > identityenrollment.MaxJoinTicketSize+2 {
		return "", errors.New("join credential input exceeds 32 KiB limit")
	}
	text := strings.TrimSpace(string(encoded))
	if len(text) == 0 || len(text) > identityenrollment.MaxJoinTicketSize {
		return "", errors.New("join credential input is empty or too large")
	}
	return text, nil
}
