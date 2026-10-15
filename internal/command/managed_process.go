package command

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dubi-project/dubi/internal/server"
)

// Serving remains possible when HOME is unavailable (for example a service
// account), but the operator sees that zero-argument local discovery was not
// saved. The Store itself remains the only authoritative state.
func runManagedProcess(ctx context.Context, process *server.Server, stderr io.Writer) error {
	if ctx == nil || process == nil {
		return errors.New("managed Node needs context and process")
	}
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	select {
	case <-process.Ready():
		endpoint, err := localEndpoint(process)
		var advertise string
		if err == nil {
			value, found, readErr := process.Runtime.Root.Node(process.Runtime.Identity.RaftID)
			if readErr != nil {
				err = readErr
			} else if !found {
				err = errors.New("ready Node has no Root network registration")
			} else {
				advertise = value.Address
			}
		}
		if err == nil {
			err = saveLocalContext(process.StoreRoot(), process.Runtime.Identity, process.LocalAddress(), endpoint, advertise)
		}
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "dubi: warning: Node is running, but ~/.dubi/current.json was not updated: %v\n", err)
		}
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	return <-done
}
