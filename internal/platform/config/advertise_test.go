package config

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestAdvertiseUsesBoundAddressOrExplicitNetworkOverrideWithoutDiscovery(t *testing.T) {
	for _, tc := range []struct{ bound, explicit, want string }{
		{"127.0.0.1:17343", "", "127.0.0.1:17343"},
		{"[::1]:7443", "", "[::1]:7443"},
		{"0.0.0.0:7443", "public.example:8443", "public.example:8443"},
	} {
		got, err := discoverAdvertise(context.Background(), tc.bound, tc.explicit, "", func(context.Context, string, string) (net.IP, error) {
			t.Fatal("unnecessary route discovery")
			return nil, nil
		})
		if err != nil || got != tc.want {
			t.Fatal(tc, got, err)
		}
	}
}

func TestAdvertiseSelectsSeedRouteAndIPv6Fallback(t *testing.T) {
	for _, tc := range []struct {
		bound, seed, want string
		networks, targets []string
		ips               []net.IP
	}{
		{"0.0.0.0:7443", "seed.example:7443", "10.1.2.3:7443", []string{"udp4"}, []string{"seed.example:7443"}, []net.IP{net.ParseIP("10.1.2.3")}},
		{"[::]:7443", "[2001:db8::2]:7443", "[2001:db8::3]:7443", []string{"udp"}, []string{"[2001:db8::2]:7443"}, []net.IP{net.ParseIP("2001:db8::3")}},
		{"[::]:7443", "", "[2001:db8::3]:7443", []string{"udp4", "udp6"}, []string{"192.0.2.1:9", "[2001:db8::1]:9"}, []net.IP{nil, net.ParseIP("2001:db8::3")}},
	} {
		calls := 0
		got, err := discoverAdvertise(context.Background(), tc.bound, "", tc.seed, func(ctx context.Context, network, target string) (net.IP, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("unbounded network discovery")
			}
			if calls >= len(tc.targets) || network != tc.networks[calls] || target != tc.targets[calls] {
				t.Fatal("unexpected route", network, target)
			}
			ip := tc.ips[calls]
			calls++
			if ip == nil {
				return nil, errors.New("no route")
			}
			return ip, nil
		})
		if err != nil || got != tc.want || calls != len(tc.targets) {
			t.Fatal(got, calls, err)
		}
	}
}

func TestAdvertiseRejectsUnusableInputsAndRoutes(t *testing.T) {
	for _, tc := range [][3]string{{"0.0.0.0:0", "", ""}, {"0.0.0.0:0000", "", ""}, {"0.0.0.0:07443", "", ""}, {"0.0.0.0:7443", "0.0.0.0:7443", ""}, {"0.0.0.0:7443", "", "seed:0"}} {
		if _, err := discoverAdvertise(context.Background(), tc[0], tc[1], tc[2], func(context.Context, string, string) (net.IP, error) {
			t.Fatal("invalid input reached discovery")
			return nil, nil
		}); err == nil {
			t.Fatal("invalid network input accepted", tc)
		}
	}
	for _, bad := range []string{"0.0.0.0", "127.0.0.1", "224.0.0.1", "169.254.1.1", "2001:db8::2", ""} {
		_, err := discoverAdvertise(context.Background(), "0.0.0.0:7443", "", "", func(context.Context, string, string) (net.IP, error) { return net.ParseIP(bad), nil })
		if err == nil || !strings.Contains(err.Error(), "--advertise") {
			t.Fatal("unusable route accepted", bad, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DiscoverAdvertise(ctx, "127.0.0.1:7443", "", ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAdvertiseUDPRouteDoesNotSendDatagrams(t *testing.T) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	got, err := DiscoverAdvertise(context.Background(), "0.0.0.0:7443", "", listener.LocalAddr().String())
	if err != nil || got != "127.0.0.1:7443" {
		t.Fatal(got, err)
	}
	// Closing a UDP socket with no writes must not require any responder.
	if err := listener.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := listener.ReadFromUDP(make([]byte, 1)); err == nil {
		t.Fatal("discovery sent a packet")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("listener failed instead of proving no packet", err)
	}
}
