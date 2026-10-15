package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
)

// DiscoverAdvertise uses the actual bound listener address (after port-zero
// allocation), never an identity or TLS server name. A seed selects the local
// route toward an existing cluster. With no seed, documentation-only addresses
// query the system's default routes. UDP connect chooses a route but sends NO
// datagram; this is not a reachability probe or proof of inbound connectivity.
// NAT, tunnels and unusual routing may require the network-only override.
func DiscoverAdvertise(ctx context.Context, bound, explicit, seed string) (string, error) {
	return discoverAdvertise(ctx, bound, explicit, seed, localRouteIP)
}

type routeIP func(context.Context, string, string) (net.IP, error)

func discoverAdvertise(ctx context.Context, bound, explicit, seed string, route routeIP) (string, error) {
	if ctx == nil {
		return "", errors.New("network discovery context required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(bound)
	if err != nil || ValidateListen(bound) != nil || identityenrollment.ValidateNodeAddress(net.JoinHostPort("127.0.0.1", port)) != nil {
		return "", errors.New("advertise discovery requires an already bound host:port")
	}
	if explicit != "" {
		if err := identityenrollment.ValidateNodeAddress(explicit); err != nil {
			return "", err
		}
		return explicit, nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsUnspecified() {
		if err := identityenrollment.ValidateNodeAddress(bound); err != nil {
			return "", err
		}
		return bound, nil
	}
	if seed != "" {
		if err := identityenrollment.ValidateNodeAddress(seed); err != nil {
			return "", err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	type candidate struct{ network, address string }
	var candidates []candidate
	if seed != "" {
		network := "udp"
		if ip.To4() != nil {
			network = "udp4"
		}
		candidates = append(candidates, candidate{network, seed})
	} else {
		candidates = append(candidates, candidate{"udp4", "192.0.2.1:9"})
		if ip.To4() == nil {
			candidates = append(candidates, candidate{"udp6", "[2001:db8::1]:9"})
		}
	}
	var failures []error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		local, err := route(ctx, candidate.network, candidate.address)
		if err == nil && (local == nil || local.IsUnspecified() || local.IsMulticast() || local.IsLinkLocalUnicast() ||
			local.IsLinkLocalMulticast() || seed == "" && local.IsLoopback() || ip.To4() != nil && local.To4() == nil) {
			err = errors.New("route has no usable listener-compatible local address")
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		address := net.JoinHostPort(local.String(), port)
		if err := identityenrollment.ValidateNodeAddress(address); err != nil {
			failures = append(failures, err)
			continue
		}
		return address, nil
	}
	return "", fmt.Errorf("cannot determine the Node's network address; specify --advertise host:port: %w", errors.Join(failures...))
}

func localRouteIP(ctx context.Context, network, destination string) (net.IP, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	closeErr := conn.Close() // No Read/Write: do not send discovery traffic.
	if closeErr != nil {
		return nil, closeErr
	}
	if !ok || local == nil || local.Zone != "" {
		return nil, errors.New("route has no unscoped UDP local address")
	}
	return append(net.IP(nil), local.IP...), nil
}
