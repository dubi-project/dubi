package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// EnsureLocalNode publishes only the first bootstrap Node's own descriptor.
// Joined Nodes must already have an exact enrollment-authorized registration.
// Restart validates existing metadata; it never silently changes an address,
// Region, NodeID or RaftID. There are no caller-supplied identity fields.
func (runtime *Runtime) EnsureLocalNode(ctx context.Context, region, address string) error {
	return runtime.EnsureLocalNodeWithName(ctx, region, address, "")
}

// EnsureLocalNodeWithName is the bootstrap registration path with an optional
// operator-facing label. Existing registrations keep their stored label when
// name is empty; changing a label is handled by a separate Root command.
func (runtime *Runtime) EnsureLocalNodeWithName(ctx context.Context, region, address, name string) error {
	if runtime == nil || runtime.Host == nil || runtime.Root == nil || ctx == nil {
		return errors.New("local registration requires a Node runtime and context")
	}
	if err := identityenrollment.ValidateNodeAddress(address); err != nil {
		return err
	}
	descriptor := rootstate.Node{NodeID: runtime.Identity.NodeID, RaftID: runtime.Identity.RaftID, Name: name, RegionID: region, Address: address}
	encodedNode, err := json.Marshal(descriptor)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encodedNode)
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.RegisterNode,
		RequestID: "bootstrap-node-" + hex.EncodeToString(digest[:]), Node: &descriptor})
	if err != nil {
		return err
	}
	runtime.recoverMu.Lock()
	defer runtime.recoverMu.Unlock()
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return errors.New("Node runtime is closed")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if runtime.recoveryContext != nil {
		stop := context.AfterFunc(runtime.recoveryContext, cancel)
		defer stop()
	}
	// An enrolled Node has already completed the local activation fence before
	// this registration confirmation runs. Its Root descriptor is therefore
	// locally authenticated and does not need another learner-local ReadIndex
	// (which may be unavailable until promotion). Bootstrap still takes the
	// normal linearizable barrier below.
	if runtime.RootJoin != nil {
		local, err := runtime.RootJoin.Load()
		if err != nil {
			return err
		}
		if local.Phase == rootjoin.LocalActive {
			current, found, err := runtime.Root.Node(runtime.Identity.RaftID)
			if err != nil {
				return err
			}
			if _, fenced, err := runtime.Root.NodeFence(runtime.Identity.RaftID); err != nil {
				return err
			} else if fenced {
				return errors.New("local Node identity is fenced")
			}
			if !found || current.NodeID != descriptor.NodeID || current.RegionID != descriptor.RegionID || current.Address != descriptor.Address {
				return errors.New("local network metadata differs from registered Node; controlled address/Region change required")
			}
			if name == "" || current.Name == name {
				return nil
			}
		}
	}
	barrier := func() error { return runtime.confirmRoot(ctx) }
	if err := barrier(); err != nil {
		return err
	}
	current, found, err := runtime.Root.Node(runtime.Identity.RaftID)
	if err != nil {
		return err
	}
	if _, fenced, err := runtime.Root.NodeFence(runtime.Identity.RaftID); err != nil {
		return err
	} else if fenced {
		return errors.New("local Node identity is fenced")
	}
	if found {
		if name == "" {
			descriptor.Name = current.Name
		}
		if current != descriptor {
			return errors.New("local network metadata differs from registered Node; controlled address/Region change required")
		}
		return nil
	}
	conf, err := runtime.Root.RootMembership()
	if err != nil {
		return err
	}
	if runtime.Identity.Origin != identity.OriginBootstrap || len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != runtime.Identity.RaftID ||
		len(conf.GetLearners()) != 0 || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return errors.New("only the original single-node bootstrap may self-register; enrollment registration is missing")
	}
	submitErr := runtime.Host.Submit(raft.RootGroupID, command)
	if err := barrier(); err != nil {
		return errors.Join(submitErr, err)
	}
	result, found, err := runtime.Root.ResultForCommand(command)
	if err != nil || !found || result.Error != "" {
		return errors.Join(submitErr, err, fmt.Errorf("Root did not confirm exact bootstrap registration: found=%v rejection=%q", found, result.Error))
	}
	current, found, err = runtime.Root.Node(runtime.Identity.RaftID)
	if err != nil || !found || current != descriptor {
		return errors.Join(err, errors.New("confirmed bootstrap registration differs from Node descriptor"))
	}
	return nil
}
