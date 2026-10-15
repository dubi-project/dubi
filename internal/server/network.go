package server

import (
	"context"
	"errors"
	"net"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/node"
)

// NetworkOptions express deployment intent only. Empty values reuse immutable
// registration/enrollment metadata, or derive the first Node's address and use
// the default Region. They cannot override any generated identity.
type NetworkOptions struct {
	Advertise string
	Region    string
	// Name is an optional operator-facing label. It is applied after the Node
	// has started and may be changed without changing its identity.
	Name string
}

func resolveLocalNetwork(runtime *node.Runtime, bound string, options NetworkOptions) (rootstate.Node, error) {
	known, found, err := runtime.Root.Node(runtime.Identity.RaftID)
	if err != nil {
		return rootstate.Node{}, err
	}
	if _, fenced, err := runtime.Root.NodeFence(runtime.Identity.RaftID); err != nil {
		return rootstate.Node{}, err
	} else if fenced {
		return rootstate.Node{}, errors.New("local Node identity is fenced")
	}
	if !found {
		view, hasJoin, err := runtime.RootJoinView()
		if err != nil {
			return rootstate.Node{}, err
		}
		if hasJoin {
			for _, peer := range view.Peers {
				if peer.RaftID == runtime.Identity.RaftID {
					known, found = peer, true
					break
				}
			}
		}
	}
	if found && known.NodeID != runtime.Identity.NodeID {
		return rootstate.Node{}, errors.New("local Root network record belongs to another Node")
	}
	if found {
		if err := identityenrollment.ValidateNodeAddress(known.Address); err != nil {
			return rootstate.Node{}, err
		}
		if options.Advertise != "" && options.Advertise != known.Address || options.Region != "" && options.Region != known.RegionID {
			return rootstate.Node{}, errors.New("network flags differ from registered Node; controlled address/Region change required")
		}
		_, knownPort, _ := net.SplitHostPort(known.Address)
		_, boundPort, err := net.SplitHostPort(bound)
		if err != nil || options.Advertise == "" && knownPort != boundPort {
			return rootstate.Node{}, errors.New("listen port differs from the registered route; reuse its port or explicitly confirm --advertise for an intentional port mapping")
		}
		// Preserve the published route; never replace it with today's default
		// interface. A different NAT port requires an explicit network override.
		return known, nil
	}
	address, err := config.DiscoverAdvertise(context.Background(), bound, options.Advertise, "")
	if err != nil {
		return rootstate.Node{}, err
	}
	region := options.Region
	if region == "" {
		region = "default"
	}
	return rootstate.Node{NodeID: runtime.Identity.NodeID, RaftID: runtime.Identity.RaftID, Name: options.Name, RegionID: region, Address: address}, nil
}
