package tlscredentials

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"google.golang.org/grpc/credentials"
)

// These fixtures represent already-verified TLS output, isolating transport
// lifetime behavior. The integration tests separately perform real mTLS.
func verifiedState(now, leafExpiry, caExpiry time.Time) tls.ConnectionState {
	leaf := &x509.Certificate{Raw: []byte{1}, NotBefore: now.Add(-time.Hour), NotAfter: leafExpiry}
	ca := &x509.Certificate{Raw: []byte{2}, NotBefore: now.Add(-time.Hour), NotAfter: caExpiry}
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf, ca}}}
}

type countedConn struct {
	net.Conn
	closes atomic.Int64
}

func (c *countedConn) Close() error { c.closes.Add(1); return c.Conn.Close() }

func TestTLSDeadlineClosesIdleConnectionAndBlockedRead(t *testing.T) {
	for _, expires := range []string{"leaf", "ca"} {
		t.Run(expires, func(t *testing.T) {
			now := time.Now()
			leaf, ca := now.Add(time.Hour), now.Add(time.Hour)
			if expires == "leaf" {
				leaf = now.Add(100 * time.Millisecond)
			} else {
				ca = now.Add(100 * time.Millisecond)
			}
			a, b := net.Pipe()
			defer b.Close()
			raw := &countedConn{Conn: a}
			c := &boundedCredentials{now: time.Now}
			conn, _, err := c.bind(raw, raw, credentials.TLSInfo{State: verifiedState(now, leaf, ca)})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			done := make(chan error, 1)
			go func() { _, err := conn.Read(make([]byte, 1)); done <- err }()
			select {
			case err := <-done:
				if !errors.Is(err, ErrPeerValidity) {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("expired idle connection remained open")
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if raw.closes.Load() != 1 {
				t.Fatal("raw connection closed repeatedly", raw.closes.Load())
			}
			if _, err := b.Read(make([]byte, 1)); err != io.EOF {
				t.Fatal("peer not closed", err)
			}
		})
	}
}

func TestTLSDeadlineRejectsClockJumpBeforeIO(t *testing.T) {
	for _, direction := range []string{"forward", "backward"} {
		t.Run(direction, func(t *testing.T) {
			now := time.Now()
			var clock atomic.Int64
			clock.Store(now.UnixNano())
			a, b := net.Pipe()
			defer b.Close()
			c := &boundedCredentials{now: func() time.Time { return time.Unix(0, clock.Load()) }}
			conn, _, err := c.bind(a, a, credentials.TLSInfo{State: verifiedState(now, now.Add(time.Hour), now.Add(time.Hour))})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if direction == "forward" {
				clock.Store(now.Add(2 * time.Hour).UnixNano())
			} else {
				clock.Store(now.Add(-2 * time.Hour).UnixNano())
			}
			if n, err := conn.Write([]byte("must not send")); n != 0 || !errors.Is(err, ErrPeerValidity) {
				t.Fatal(n, err)
			}
			if _, err := b.Read(make([]byte, 1)); err != io.EOF {
				t.Fatal("expired writer did not close transport", err)
			}
		})
	}
}

type afterReadConn struct {
	net.Conn
	after func()
}

func (c *afterReadConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.after()
	return n, err
}

func TestTLSDeadlineDiscardsBytesExpiringDuringRead(t *testing.T) {
	now := time.Now()
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	a, b := net.Pipe()
	defer b.Close()
	read := &afterReadConn{Conn: a, after: func() { clock.Store(now.Add(2 * time.Hour).UnixNano()) }}
	c := &boundedCredentials{now: func() time.Time { return time.Unix(0, clock.Load()) }}
	conn, _, err := c.bind(a, read, credentials.TLSInfo{State: verifiedState(now, now.Add(time.Hour), now.Add(time.Hour))})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done := make(chan error, 1)
	go func() { _, err := b.Write([]byte("data")); done <- err }()
	buffer := make([]byte, 4)
	if n, err := conn.Read(buffer); n != 0 || !errors.Is(err, ErrPeerValidity) || !bytes.Equal(buffer, make([]byte, 4)) {
		t.Fatal("expired plaintext escaped", n, err, buffer)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTLSDeadlineSelectsLongestVerifiedPathAndRejectsUnverified(t *testing.T) {
	now := time.Now()
	state := verifiedState(now, now.Add(time.Hour), now.Add(time.Minute))
	alternate := *state.VerifiedChains[0][1]
	alternate.Raw = []byte{3}
	alternate.NotAfter = now.Add(30 * time.Minute)
	state.VerifiedChains = append(state.VerifiedChains, []*x509.Certificate{state.PeerCertificates[0], &alternate})
	_, deadline, err := identity.VerifiedTLSCertificate(state, now)
	if err != nil || !deadline.Equal(alternate.NotAfter) {
		t.Fatal("short path capped valid alternative", deadline, err)
	}
	for _, bad := range []tls.ConnectionState{{PeerCertificates: state.PeerCertificates}, {VerifiedChains: state.VerifiedChains}} {
		a, b := net.Pipe()
		defer b.Close()
		if _, _, err := (&boundedCredentials{now: time.Now}).bind(a, a, credentials.TLSInfo{State: bad}); err == nil {
			t.Fatal("unverified TLS accepted")
		}
		if _, err := b.Read(make([]byte, 1)); err != io.EOF {
			t.Fatal("rejected handshake leaked connection", err)
		}
	}
}

func TestTLSDeadlineClonePreservesPolicyAndNormalClose(t *testing.T) {
	now := time.Now()
	c := NewTLS(&tls.Config{ServerName: "dubi.internal"}).(*boundedCredentials)
	c.now = func() time.Time { return now }
	clone := c.Clone().(*boundedCredentials)
	if clone.Info() != c.Info() || !clone.now().Equal(now) {
		t.Fatal("clone changed policy")
	}
	if err := clone.OverrideServerName("other.internal"); err != nil || c.Info().ServerName != "dubi.internal" {
		t.Fatal("clone mutated original", err)
	}
	a, b := net.Pipe()
	defer b.Close()
	conn, _, err := clone.bind(a, a, credentials.TLSInfo{State: verifiedState(now, now.Add(time.Hour), now.Add(time.Hour))})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	bound := conn.(*boundedConn)
	bound.expire() // An already queued timer callback must not rearm after Close.
	bound.mu.Lock()
	defer bound.mu.Unlock()
	if !bound.closed || bound.timer.Stop() {
		t.Fatal("timer remained active after close")
	}
}
