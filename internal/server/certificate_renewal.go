package server

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertrpc"
)

type certificateRenewal struct {
	store                        string
	id                           identity.Identity
	host                         nodecertrpc.RootReader
	root                         *rootstate.State
	signer                       *nodepki.Signer
	report                       groupmembership.ErrorReporter
	now                          func() time.Time
	interval, retryMin, retryMax time.Duration
}

func newCertificateRenewal(store string, id identity.Identity, host nodecertrpc.RootReader, root *rootstate.State, signer *nodepki.Signer, report groupmembership.ErrorReporter) *certificateRenewal {
	if report == nil {
		report = func(error) {}
	}
	return &certificateRenewal{store: store, id: id, host: host, root: root, signer: signer, report: report, now: time.Now,
		interval: 6 * time.Hour, retryMin: time.Minute, retryMax: time.Hour}
}

// run belongs to Server.workers and uses its cancellation/Store lifetime. It
// does not bootstrap, join, change membership, or acquire another Store lease.
func (r *certificateRenewal) run(ctx context.Context) {
	retry := r.retryMin
	for {
		if ctx.Err() != nil {
			return
		}
		attempt, stop := context.WithTimeout(ctx, time.Minute)
		err := r.once(attempt)
		stop()
		if ctx.Err() != nil {
			return
		}
		delay := r.interval
		if err != nil {
			r.report(fmt.Errorf("Node certificate renewal: %w", err))
			delay = retry
			retry = min(retry*2, r.retryMax)
		} else {
			retry = r.retryMin
		}
		// Spread retries after a fleet restart; admission on the issuer is still
		// strictly bounded. These are internal defaults, never operator settings.
		jitter := delay / 10
		if jitter > 0 {
			delay = delay - jitter + time.Duration(rand.Int64N(int64(2*jitter)+1))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *certificateRenewal) once(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := nodepki.Load(r.store, r.id, nodepki.DefaultServerName, r.now())
	if err != nil {
		return err
	}
	if !nodepki.NeedsRenewal(current.Certificate, r.now()) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A prior attempt may have renamed the leaf but failed directory Sync.
		// Visibility and an extended expiry are not a durability acknowledgement.
		// Reaffirm the existing leaf even after restart, without signing again.
		leaf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: current.Certificate.Raw})
		_, err := nodepki.InstallRenewedLeaf(r.store, r.id, nodepki.DefaultServerName, leaf, r.now())
		return err
	}
	if err := r.confirm(ctx); err != nil {
		return err
	}
	var leaf []byte
	if r.signer != nil {
		ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: current.CACertificate.Raw})
		if !bytes.Equal(ca, r.signer.CACertificatePEM()) {
			return errors.New("local renewal authority differs from managed CA")
		}
		old := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: current.Certificate.Raw})
		leaf, err = r.signer.RenewNode(r.id, nodepki.DefaultServerName, old, r.now())
	} else {
		// The immutable receipt pins the original issuer, not an arbitrary user
		// endpoint. Signer relocation/HA requires its own confirmed directory.
		receipt, loadErr := enrollment.LoadReceipt(r.store)
		if loadErr != nil {
			return loadErr
		}
		if receipt.Bundle.ClusterID != r.id.ClusterID {
			return errors.New("renewal receipt belongs to another Cluster")
		}
		leaf, err = nodecertrpc.RequestRenewal(ctx, r.store, receipt.IssuerAddress)
	}
	if err != nil {
		return err
	}
	if err := r.confirm(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = nodepki.InstallRenewedLeaf(r.store, r.id, nodepki.DefaultServerName, leaf, r.now())
	return err
}

func (r *certificateRenewal) confirm(ctx context.Context) error {
	if _, err := r.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	node, member, err := r.root.RootMember(r.id.RaftID)
	if err != nil {
		return err
	}
	if !member || node.NodeID != r.id.NodeID {
		return errors.New("local Node is not a current Root member for renewal")
	}
	return ctx.Err()
}
