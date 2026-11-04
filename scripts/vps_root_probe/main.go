// Read-only VPS diagnostic: separate remote quorum RPC from local Root apply wait.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func run() error {
	store := flag.String("store", "", "existing enrolled Node Store")
	count := flag.Int("count", 32, "finite number of samples")
	workers := flag.Int("workers", 4, "bounded concurrent requests")
	duration := flag.Duration("duration", 0, "optional finite observation window, at most 60 minutes")
	flag.Parse()
	if flag.NArg() != 0 || *store == "" || *count < 1 || *count > 256 || *workers < 1 || *workers > 8 || *duration < 0 || *duration > 60*time.Minute {
		return errors.New("invalid diagnostic bounds")
	}
	var in struct {
		Status struct {
			Root rootjoin.View `json:"root"`
		} `json:"status"`
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 128<<10))
	if err := decoder.Decode(&in); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing or oversized input")
	}
	id, err := identity.Load(*store)
	if err != nil {
		return err
	}
	view := in.Status.Root
	if err := view.Validate(id.ClusterID); err != nil {
		return err
	}
	if len(view.Peers) > 64 {
		return errors.New("diagnostic peer bound exceeded")
	}
	_, clientTLS, err := config.LoadTLS(*store, id)
	if err != nil {
		return err
	}
	targets := make(map[uint64]identity.CertificateIdentity)
	voters := make(map[uint64]bool)
	for _, ids := range [][]uint64{view.Voters, view.VotersOutgoing} {
		for _, rid := range ids {
			voters[rid] = true
		}
	}
	clients := make(map[uint64]raftwire.GroupServiceClient)
	for _, n := range view.Peers {
		if !voters[n.RaftID] {
			continue
		}
		targets[n.RaftID] = identity.CertificateIdentity{ClusterID: id.ClusterID, NodeID: n.NodeID, RaftID: n.RaftID}
		conn, err := grpc.NewClient("passthrough:///"+n.Address, grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)))
		if err != nil {
			return err
		}
		defer conn.Close()
		clients[n.RaftID] = raftwire.NewGroupServiceClient(conn)
	}
	if _, ok := targets[view.LeaderID]; !ok {
		return errors.New("no known voter leader hint")
	}
	probe := rootProbe{targets: targets}
	probe.preferred.Store(view.LeaderID)
	probe.query = func(ctx context.Context, target uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error) {
		var actual peer.Peer
		reply, err := clients[target].ReadIndex(ctx, &raftwire.GroupReadIndexRequest{ClusterId: id.ClusterID, GroupId: raft.RootGroupID, FromRaftId: id.RaftID, ToRaftId: target}, grpc.Peer(&actual))
		if err != nil {
			return identity.CertificateIdentity{}, nil, err
		}
		cert, err := identity.VerifiedPeerCertificate(peer.NewContext(ctx, &actual), time.Now())
		if err != nil {
			return identity.CertificateIdentity{}, nil, fmt.Errorf("peer TLS verification: %w", err)
		}
		remote, err := identity.ParseNodeCertificate(cert)
		if err != nil {
			return identity.CertificateIdentity{}, nil, fmt.Errorf("peer certificate identity: %w", err)
		}
		return remote, reply, nil
	}
	transport := &http.Transport{TLSClientConfig: clientTLS, MaxConnsPerHost: *workers, MaxIdleConnsPerHost: *workers}
	defer transport.CloseIdleConnections()
	metrics := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	applied := func(ctx context.Context) (uint64, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://127.0.0.1:12444/metrics", nil)
		if err != nil {
			return 0, err
		}
		resp, err := metrics.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("local metrics HTTP %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return 0, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "dubi_root_applied_index ") {
				value, err := strconv.ParseUint(strings.TrimPrefix(line, "dubi_root_applied_index "), 10, 64)
				if value >= 1<<53 {
					return 0, errors.New("Root metric exceeds exact integer precision")
				}
				return value, err
			}
		}
		return 0, errors.New("local Root applied metric missing")
	}
	type sample struct {
		Number    int       `json:"number"`
		UTC       string    `json:"utc"`
		From      uint64    `json:"from"`
		To        uint64    `json:"to"`
		Before    uint64    `json:"local_before"`
		Confirmed uint64    `json:"confirmed_index"`
		After     uint64    `json:"local_after"`
		RPCMS     float64   `json:"rpc_ms"`
		ApplyMS   float64   `json:"apply_ms"`
		Phase     string    `json:"phase"`
		Error     string    `json:"error,omitempty"`
		Attempts  []attempt `json:"attempts"`
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	var output sync.Mutex
	var failures atomic.Int32
	encoder := json.NewEncoder(os.Stdout)
	for worker := 0; worker < *workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for number := range jobs {
				s := sample{Number: number, UTC: time.Now().UTC().Format(time.RFC3339Nano), From: id.RaftID, Phase: "local_metrics"}
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				func() {
					before, err := applied(ctx)
					if err != nil {
						s.Error = err.Error()
						return
					}
					s.Before = before
					s.Phase = "remote_quorum_rpc"
					start := time.Now()
					index, attempts, err := probe.confirm(ctx)
					s.Attempts = attempts
					if len(attempts) > 0 {
						s.To = attempts[len(attempts)-1].To
					}
					s.RPCMS = float64(time.Since(start).Microseconds()) / 1000
					if err != nil {
						s.Error = err.Error()
						return
					}
					s.Confirmed = index
					s.Phase = "local_apply_wait"
					start = time.Now()
					defer func() { s.ApplyMS = float64(time.Since(start).Microseconds()) / 1000 }()
					for {
						s.After, err = applied(ctx)
						if err != nil {
							s.Error = err.Error()
							return
						}
						if s.After >= s.Confirmed {
							s.Phase = "passed"
							return
						}
						select {
						case <-ctx.Done():
							s.Error = ctx.Err().Error()
							return
						case <-time.After(50 * time.Millisecond):
						}
					}
				}()
				cancel()
				if s.Error != "" {
					failures.Add(1)
				}
				output.Lock()
				if err := encoder.Encode(s); err != nil {
					failures.Add(1)
				}
				output.Unlock()
			}
		}()
	}
	// Stop scheduling on collection, but drain accepted samples so a timeout
	// in flight is not silently lost when the scenario finishes.
	stop, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	send := func(n int) bool {
		select {
		case <-stop.Done():
			return false
		default:
		}
		select {
		case jobs <- n:
			return true
		case <-stop.Done():
			return false
		}
	}
	if *duration == 0 {
		for n := 1; n <= *count; n++ {
			if !send(n) {
				break
			}
		}
	} else {
		// At most two scheduled requests per second per observed Node, even when
		// its quorum path is fast. Worker count and total observation are bounded.
		end := time.Now().Add(*duration)
		timer := time.NewTicker(500 * time.Millisecond)
	loop:
		for n := 1; n <= 7200 && time.Now().Before(end); n++ {
			select {
			case <-timer.C:
			case <-stop.Done():
				break loop
			}
			if !send(n) {
				break
			}
		}
		timer.Stop()
	}
	close(jobs)
	wg.Wait()
	if failures.Load() != 0 {
		return fmt.Errorf("%d diagnostic samples failed", failures.Load())
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
