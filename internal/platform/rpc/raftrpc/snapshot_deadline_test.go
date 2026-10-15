package raftrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestSnapshotDeadlinePolicyBoundedAndSizeAware(t *testing.T) {
	previous := snapshotDeadlines{}
	for _, size := range []uint64{0, 1, 64 << 20, 1 << 30, 100 << 30, snapshot.MaxStreamBytes, math.MaxUint64} {
		got := deadlinesForSnapshot(size)
		if got.idle != 2*time.Minute || got.install < 15*time.Minute || got.install > 6*time.Hour ||
			got.total < got.install || got.total > 24*time.Hour || got.install < previous.install || got.total < previous.total {
			t.Fatalf("size=%d limits=%+v previous=%+v", size, got, previous)
		}
		previous = got
	}
	if deadlinesForSnapshot(1<<30).install <= deadlinesForSnapshot(64<<20).install {
		t.Fatal("size did not extend installation grace")
	}
}

func TestSnapshotDeadlinePhaseFences(t *testing.T) {
	for _, phase := range []string{"upload", "install", "renewed-callback", "parent", "total", "close", "tiny-timer"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			limits := snapshotDeadlines{idle: time.Hour, install: 2 * time.Hour, total: 3 * time.Hour}
			if phase == "total" {
				limits.total = 20 * time.Millisecond
			}
			if phase == "tiny-timer" {
				limits.idle = time.Nanosecond
			}
			p := newSnapshotProgress(parent, limits)
			defer p.Close()
			switch phase {
			case "upload", "install":
				if phase == "install" {
					if err := p.renew(true); err != nil {
						t.Fatal(err)
					}
				}
				p.mu.Lock()
				p.deadline = time.Now().Add(-time.Second)
				p.mu.Unlock()
				if err := p.renew(false); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("late progress revived deadline", err)
				}
				want := "transfer"
				if phase == "install" {
					want = "installation"
				}
				if !strings.Contains(context.Cause(p.ctx).Error(), want) {
					t.Fatal(context.Cause(p.ctx))
				}
			case "renewed-callback":
				if err := p.renew(false); err != nil {
					t.Fatal(err)
				}
				p.expire() // Model an old callback that was queued before renewal.
				if p.ctx.Err() != nil {
					t.Fatal("old callback canceled renewed transfer")
				}
				if err := p.renew(true); err != nil {
					t.Fatal(err)
				}
				if err := p.renew(false); err == nil {
					t.Fatal("upload resumed during installation")
				}
				if err := p.renew(true); err == nil {
					t.Fatal("installation grace renewed")
				}
			case "parent":
				cancel()
				if err := p.renew(false); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "total", "tiny-timer":
				select {
				case <-p.ctx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("deadline not enforced")
				}
				if !errors.Is(context.Cause(p.ctx), context.DeadlineExceeded) {
					t.Fatal(context.Cause(p.ctx))
				}
			case "close":
				p.Close()
				p.Close()
				p.expire()
				if err := p.renew(false); err == nil {
					t.Fatal("closed tracker renewed")
				}
			}
		})
	}
}

func deadlineWireTransport(t *testing.T, source *snapshot.Store, backend *fileSnapshotBackend) (*Transport, *Server, <-chan bool) {
	t.Helper()
	pool, certificates := testCertificates(t, []uint64{1, 2})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	server, err := NewServer("cluster", 2, backend, func(context.Context, uint64) error { return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	service := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certificates[2]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
	t.Cleanup(service.Stop)
	raftwire.RegisterRaftServiceServer(service, server)
	go service.Serve(listener)
	directory := NewStaticDirectory()
	if err := directory.Update(2, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	result := make(chan bool, 1)
	transport, err := NewTransport(TransportOptions{ClusterID: "cluster", RaftID: 1, Directory: directory,
		Credentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: pool,
			Certificates: []tls.Certificate{certificates[1]}}),
		OpenSnapshot: func(_ string, ref snapshot.Reference) (io.ReadCloser, error) { return source.Open(ref) },
		OnSnapshot:   func(_ string, _ uint64, success bool) { result <- success }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.Close() })
	return transport, server, result
}

func TestSnapshotDeadlineWireTransferStallAndInstallGrace(t *testing.T) {
	for _, scenario := range []string{"slow-install", "stalled-upload", "stalled-install"} {
		t.Run(scenario, func(t *testing.T) {
			source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
			size := 32 << 10
			if scenario == "stalled-upload" {
				size = 24 << 20
			} // Larger than gRPC flow-control buffering.
			ref := wireArtifact(t, source, 1, size)
			imported, exited := make(chan struct{}), make(chan struct{})
			backend := &fileSnapshotBackend{importSnapshot: func(ctx context.Context, _ string, _ *raftpb.Message, reader io.Reader) (uint64, error) {
				defer close(exited)
				if scenario == "stalled-upload" {
					<-ctx.Done()
					return 0, ctx.Err()
				}
				if err := destination.Import(ctx, ref, reader); err != nil {
					return 0, err
				}
				close(imported)
				if scenario == "stalled-install" {
					<-ctx.Done()
					return 0, ctx.Err()
				}
				// Exceed upload idle, but remain inside the independent install grace.
				timer := time.NewTimer(750 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return ref.Index, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}}
			transport, server, result := deadlineWireTransport(t, source, backend)
			transport.snapshotLimits = func(uint64) snapshotDeadlines {
				install := 3 * time.Second
				if scenario == "stalled-install" {
					install = 700 * time.Millisecond
				}
				return snapshotDeadlines{idle: 500 * time.Millisecond, install: install, total: 10 * time.Second}
			}
			if err := transport.Send("data", fileMessage(t, ref, 1, 2)); err != nil {
				t.Fatal(err)
			}
			if scenario != "stalled-upload" {
				select {
				case <-imported:
				case success := <-result:
					t.Fatal("ended before import", success)
				case <-time.After(5 * time.Second):
					t.Fatal("import did not finish")
				}
				select {
				case <-result:
					t.Fatal("ACK before install completed")
				default:
				}
			}
			select {
			case success := <-result:
				if success != (scenario == "slow-install") {
					t.Fatal("unexpected snapshot result", scenario, success)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("deadline did not release sender")
			}
			if len(transport.snapshotSlots) != 0 || transport.ResourceStatus().Snapshots.UsedBytes != 0 {
				t.Fatal("sender deadline leaked lease")
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("receiver did not see cancellation/completion")
			}
			until := time.NewTimer(5 * time.Second)
			defer until.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for server.SnapshotResources().UsedBytes != 0 {
				select {
				case <-tick.C:
				case <-until.C:
					t.Fatal("receiver leaked lease")
				}
			}
		})
	}
}
