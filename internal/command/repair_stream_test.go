package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/server"
)

// Test-only TLS passthrough: a partial server-to-client record stream is lost
// at the TCP layer. No production RPC or repair hook is involved.
type interruptedRecordListener struct {
	net.Listener
	armed atomic.Bool
	fired chan struct{}
}

type interruptedRecordConn struct {
	net.Conn
	listener *interruptedRecordListener
	eligible bool
	mu       sync.Mutex
	written  int
}

func (l *interruptedRecordListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &interruptedRecordConn{Conn: conn, listener: l, eligible: l.armed.Load()}, nil
}

func (c *interruptedRecordConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.Conn.Write(p)
	if c.eligible && c.listener.armed.Load() {
		c.written += n
		if c.written >= 2<<20 && c.listener.armed.CompareAndSwap(true, false) {
			_ = c.Conn.Close()
			close(c.listener.fired)
		}
	}
	return n, err
}

func TestManagedRepairRecoversAfterMidStreamSourceDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	mux := referenceTestMux(t)
	stores := []string{filepath.Join(t.TempDir(), "n1"), filepath.Join(t.TempDir(), "n2"), filepath.Join(t.TempDir(), "n3")}
	if err := runInit([]string{"--store", stores[0]}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	interrupted := &interruptedRecordListener{Listener: listener, fired: make(chan struct{})}
	first, err := openManagedServerWithMux(stores[0], interrupted,
		server.NetworkOptions{Advertise: listener.Addr().String(), Region: "default"}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	stopFirst, ready := startJoinTestServer(t, first)
	ready()
	nodes := []*server.Server{first}
	stops := []func(){stopFirst}
	reports := &repairReportBuffer{}
	for i := 1; i < 3; i++ {
		_, ticket := joinTestTicket(t, stores[0], listener.Addr().String())
		output := io.Writer(io.Discard)
		if i == 2 {
			output = reports
		}
		joined, err := openJoiningServerWithMux(ctx, []string{"--store", stores[i], "--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, output, mux)
		if err != nil {
			t.Fatal(err)
		}
		stop, wait := startJoinTestServer(t, joined)
		wait()
		nodes, stops = append(nodes, joined), append(stops, stop)
	}
	group := regiongroups.GroupID("default")
	for {
		progress, found, err := first.Runtime.Root.RegionProgress(group)
		if err == nil && found && progress.Eligible && progress.Pending == nil && len(progress.Current.Nodes) == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("three-Node Region did not form", err, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	space := rootstate.Keyspace{TenantID: "stream-repair", CapabilityID: "data-alpha", DatasetID: "shared"}
	tenant, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "stream-repair-tenant", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: space.TenantID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, tenant); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		result, found, err := first.Runtime.Root.ResultForCommand(tenant)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			break
		}
	}
	activateCapabilityTestDataset(t, ctx, first, group, space)
	routes, err := router.New(first.Runtime.Host, first.Runtime.Root)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("mid-stream-repair")
	target, err := routes.ResolveTarget(ctx, space, key)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("repair-frame-payload"), (8<<20)/20+1)
	d, err := records.Describe(space, group, key, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Runtime.Records.Put(ctx, d, body); err != nil {
		t.Fatal("persist initial source bytes", err)
	}
	if err := nodes[1].Runtime.Records.Close(); err != nil {
		t.Fatal("make alternate Store temporarily unavailable", err)
	}
	if _, err := nodes[2].Runtime.Records.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("target already holds unpublished record", err)
	}
	interrupted.armed.Store(true)
	payload, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	const requestID = "publish-mid-stream-repair"
	mutation, err := routedstate.EncodeMutation(requestID, target, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(group, mutation); err != nil {
		t.Fatal(err)
	}
	owner, found := first.Runtime.Capabilities.Resolve(group)
	if !found {
		t.Fatal("initial Owner Group is not open")
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, group); err != nil {
			t.Fatal(err)
		}
		result, found, err := owner.ResultForMutation(requestID, target, payload)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if !result.Applied || result.Error != "" {
				t.Fatal("reference publication rejected", result)
			}
			break
		}
	}
	select {
	case <-interrupted.fired:
	case <-ctx.Done():
		t.Fatal("source did not disconnect during the record stream", ctx.Err())
	}
	stopFirst()
	// The second source is still unavailable. A partial TLS stream must not
	// become a holding, and the managed worker must retain this exact debt.
	if _, err := nodes[2].Runtime.Records.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("partial stream published a destination record", err)
	}
	for !strings.Contains(reports.String(), requestID) {
		select {
		case <-ctx.Done():
			t.Fatal("managed repair did not report the interrupted record debt", reports.String(), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	// Restore the alternate source under its original Node/Store identity.
	stops[1]()
	secondNode, found, err := nodes[2].Runtime.Root.Node(nodes[1].Runtime.Identity.RaftID)
	if err != nil || !found {
		t.Fatal("second Node registration missing", err)
	}
	restarted, err := openServerWithMux([]string{"--store", stores[1], "--listen", secondNode.Address}, io.Discard, mux)
	if err != nil {
		t.Fatal("restart alternate source", err)
	}
	nodes[1] = restarted
	stops[1], ready = startJoinTestServer(t, restarted)
	ready()
	if _, err := restarted.Runtime.Records.Put(ctx, d, body); err != nil {
		t.Fatal("restore alternate physical source", err)
	}
	for {
		if _, err := nodes[2].Runtime.Records.Verify(ctx, d); err == nil {
			break
		} else if !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("target repair verification failed", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed target did not recover from another source", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	readerOwner, found := nodes[2].Runtime.Capabilities.Resolve(group)
	if !found {
		t.Fatal("target Owner Group is not open")
	}
	reader := recordwrite.Reader{Barrier: nodes[2].Runtime.Host, Root: nodes[2].Runtime.Root,
		Owner: readerOwner, LocalID: nodes[2].Runtime.Identity.RaftID,
		LocalNodeID: nodes[2].Runtime.Identity.NodeID, Store: nodes[2].Runtime.Records,
		Observer: nodes[2].GroupClient, Fetcher: nodes[2].GroupClient}
	for {
		read, err := reader.ReadLocal(ctx, target, key)
		if err == nil {
			if !bytes.Equal(read.Body, body) {
				t.Fatal("recovered majority returned different bytes")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("recovered physical majority was not confirmed", err, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
