package server

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"
)

const internalConnectionLimit = 128
const handshakeLimit = 8
const handshakeTimeout = 10 * time.Second

// Reject excess sockets without returning an Accept error that would stop
// gRPC serving. Slots include TLS/HTTP2 setup and last until socket Close.
type connectionListener struct {
	net.Listener
	active   atomic.Int64
	rejected atomic.Uint64
	limit    int64
}

func limitListener(listener net.Listener, limit int64) *connectionListener {
	return &connectionListener{Listener: listener, limit: limit}
}
func (l *connectionListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.active.Add(1) > l.limit {
			l.active.Add(-1)
			l.rejected.Add(1)
			_ = conn.Close()
			continue
		}
		return &countedConn{Conn: conn, release: func() { l.active.Add(-1) }}, nil
	}
}

type countedConn struct {
	net.Conn
	once    sync.Once
	release func()
	err     error
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.release() })
	return c.err
}

type handshakeCredentials struct {
	credentials.TransportCredentials
	slots   chan struct{}
	timeout time.Duration
}

func boundedHandshakes(base credentials.TransportCredentials) *handshakeCredentials {
	return &handshakeCredentials{TransportCredentials: base, slots: make(chan struct{}, handshakeLimit), timeout: handshakeTimeout}
}
func (c *handshakeCredentials) Clone() credentials.TransportCredentials {
	return &handshakeCredentials{TransportCredentials: c.TransportCredentials.Clone(), slots: c.slots, timeout: c.timeout}
}
func (c *handshakeCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		_ = raw.Close()
		return nil, nil, errors.New("Node TLS handshake capacity exhausted")
	}
	if err := raw.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		_ = raw.Close()
		return nil, nil, err
	}
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		_ = raw.Close()
		return nil, nil, err
	}
	if err := raw.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, info, nil
}

func limitedPublicListener(listener net.Listener) net.Listener {
	if listener == nil {
		return nil
	}
	return limitListener(listener, 32)
}
