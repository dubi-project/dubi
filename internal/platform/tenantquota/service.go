// Package tenantquota checks soft limits locally and reports durable logical
// sizes asynchronously. It never reserves cluster-wide capacity on the write path.
package tenantquota

import (
	"context"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

var ErrExceeded = errors.New("Tenant storage quota exceeded")
var ErrBusy = errors.New("Tenant key has an unresolved write")
var ErrClosed = errors.New("Tenant write reservation is closed")
var ErrReused = errors.New("quota request ID reused with different content")

type Service struct {
	local  localCache
	Root   *rootstate.State
	Commit func(context.Context, rootstate.Command) error
}
