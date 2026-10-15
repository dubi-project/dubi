package server

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodeadminrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

// enrollmentAuthorityTransfer is the only component that moves sealed signer
// material. It never sends a CA private key; the target first exposes its
// node-local X25519 public key over authenticated NodeAdmin RPC.
type enrollmentAuthorityTransfer struct {
	runtime *node.Runtime
	client  *raftrpc.GroupClient
	signer  *nodepki.Signer
	store   string
}

func (transfer enrollmentAuthorityTransfer) Transfer(ctx context.Context, target rootstate.Node, epoch uint64) error {
	if transfer.runtime == nil || transfer.client == nil {
		return errors.New("local Node has no enrollment authority signer")
	}
	signer := transfer.signer
	if transfer.store != "" {
		loaded, err := nodepki.OpenSigner(transfer.store, transfer.runtime.Identity, time.Now().UTC())
		if err == nil {
			signer = loaded
		}
	}
	if signer == nil {
		return errors.New("local Node has no enrollment authority signer")
	}
	conn, err := transfer.client.NodeAdminConnection(target)
	if err != nil {
		return err
	}
	public, err := nodeadminrpc.SignerWrappingKey(ctx, conn, transfer.runtime.Identity.ClusterID, transfer.runtime.Identity.RaftID)
	if err != nil {
		return err
	}
	targetIdentity := identity.Identity{Version: 1, ClusterID: transfer.runtime.Identity.ClusterID, NodeID: target.NodeID, RaftID: target.RaftID}
	envelope, err := signer.SealFor(targetIdentity, public, epoch)
	if err != nil {
		return err
	}
	return nodeadminrpc.TransferSigner(ctx, conn, transfer.runtime.Identity.ClusterID, transfer.runtime.Identity.RaftID, epoch, envelope)
}
