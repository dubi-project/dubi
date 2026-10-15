// Package tlscredentials bounds the lifetime of authenticated TLS transports.
// RPC handlers still enforce Node/enrollment roles and current membership.
package tlscredentials

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"google.golang.org/grpc/credentials"
)

const validityCheckInterval = time.Minute

var ErrPeerValidity = errors.New("TLS peer certificate chain is no longer valid")

// NewTLS preserves standard gRPC TLS verification and AuthInfo, and closes the
// underlying transport when no historically verified path is currently valid.
// It never extends expiry, trusts an unverified chain, or bypasses TLS roles.
func NewTLS(config *tls.Config) credentials.TransportCredentials {
	return &boundedCredentials{base: credentials.NewTLS(config), now: time.Now}
}

type boundedCredentials struct {
	base credentials.TransportCredentials
	now  func() time.Time
}

func (c *boundedCredentials) Info() credentials.ProtocolInfo { return c.base.Info() }
func (c *boundedCredentials) Clone() credentials.TransportCredentials {
	return &boundedCredentials{base: c.base.Clone(), now: c.now}
}
func (c *boundedCredentials) OverrideServerName(name string) error {
	return c.base.OverrideServerName(name)
}

func (c *boundedCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.base.ClientHandshake(ctx, authority, raw)
	if err != nil {
		return nil, nil, err
	}
	return c.bind(raw, conn, info)
}

func (c *boundedCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.base.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	return c.bind(raw, conn, info)
}

func (c *boundedCredentials) bind(raw, conn net.Conn, info credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	tlsInfo, ok := info.(credentials.TLSInfo)
	if !ok {
		_ = raw.Close()
		return nil, nil, errors.New("verified TLS transport required")
	}
	now := c.now()
	_, deadline, err := identity.VerifiedTLSCertificate(tlsInfo.State, now)
	if err != nil {
		_ = raw.Close()
		return nil, nil, errors.Join(ErrPeerValidity, err)
	}
	bounded := &boundedConn{Conn: conn, raw: raw, state: tlsInfo.State, now: c.now, done: make(chan struct{})}
	bounded.mu.Lock()
	bounded.scheduleLocked(now, deadline)
	bounded.mu.Unlock()
	return bounded, info, nil
}

type boundedConn struct {
	net.Conn
	raw      net.Conn
	state    tls.ConnectionState
	now      func() time.Time
	mu       sync.Mutex
	timer    *time.Timer
	closed   bool
	done     chan struct{}
	closeErr error
}

func (c *boundedConn) scheduleLocked(now, deadline time.Time) {
	// Periodic wall-clock checks also cover a forward clock adjustment while
	// idle. Normal expiry has its own exact deadline; IO rechecks immediately.
	delay := min(deadline.Sub(now), validityCheckInterval)
	if delay <= 0 {
		delay = time.Nanosecond
	}
	c.timer = time.AfterFunc(delay, c.expire)
}

func (c *boundedConn) expire() {
	now := c.now()
	_, deadline, err := identity.VerifiedTLSCertificate(c.state, now)
	if err != nil {
		_ = c.Close()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.scheduleLocked(now, deadline)
	}
}

func (c *boundedConn) valid() error {
	if _, _, err := identity.VerifiedTLSCertificate(c.state, c.now()); err != nil {
		_ = c.Close()
		return errors.Join(ErrPeerValidity, err)
	}
	select {
	case <-c.done:
		return net.ErrClosed
	default:
	}
	return nil
}

func (c *boundedConn) Read(p []byte) (int, error) {
	if err := c.valid(); err != nil {
		return 0, err
	}
	n, err := c.Conn.Read(p)
	if validityErr := c.valid(); validityErr != nil {
		// Do not hand newly read plaintext to HTTP/2 after its peer expired.
		clear(p[:n])
		return 0, validityErr
	}
	return n, err
}

func (c *boundedConn) Write(p []byte) (int, error) {
	if err := c.valid(); err != nil {
		return 0, err
	}
	n, err := c.Conn.Write(p)
	// Bytes already written cannot be recalled. Preserve their count and
	// report uncertainty; higher-level Raft/request idempotency still applies.
	if validityErr := c.valid(); validityErr != nil {
		return n, errors.Join(err, validityErr)
	}
	return n, err
}

func (c *boundedConn) Close() error {
	c.mu.Lock()
	if c.closed {
		done := c.done
		c.mu.Unlock()
		<-done
		return c.closeErr
	}
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
	// Closing the raw socket immediately interrupts blocked TLS IO. Sending a
	// TLS close-notify first can itself block beyond the credential deadline.
	err := c.raw.Close()
	c.mu.Lock()
	c.closeErr = err
	close(c.done)
	c.mu.Unlock()
	return err
}

// Preserve the syscall capability that gRPC's TLS wrapper exposes for socket
// diagnostics. Pipe/test transports may not support it.
func (c *boundedConn) SyscallConn() (syscall.RawConn, error) {
	if conn, ok := c.raw.(syscall.Conn); ok {
		return conn.SyscallConn()
	}
	return nil, syscall.ENOTSUP
}
