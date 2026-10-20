package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func runDataset(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "create" {
		return errors.New("usage: dubi dataset create [--store path] [--server host:port] [--request-id ID] <tenant> <dataset>")
	}
	flags := flag.NewFlagSet("dataset create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	requestID := flags.String("request-id", "", "stable ID for retrying an uncertain request")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	endpoint, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = endpoint
	if flags.NArg() != 2 || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		return errors.New("usage: dubi dataset create [--store path] [--server host:port] [--request-id ID] <tenant> <dataset>")
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).CreateDataset(call, &datawire.CreateDatasetRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID,
		TenantId: flags.Arg(0), DatasetId: flags.Arg(1)})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}

func runRecord(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "put" && args[0] != "get") {
		return errors.New("usage: dubi record put|get [--store path] [--server host:port] <tenant> <dataset> <key> [value]")
	}
	operation := args[0]
	flags := flag.NewFlagSet("record "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	requestID := flags.String("request-id", "", "stable ID for retrying an uncertain put")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	endpoint, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = endpoint
	want := 3
	if operation == "put" {
		want = 4
	}
	if flags.NArg() != want || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil ||
		(operation == "get" && *requestID != "") {
		return errors.New("usage: dubi record put|get [--store path] [--server host:port] <tenant> <dataset> <key> [value]")
	}
	if operation == "put" {
		if err := ensureRequestID(requestID); err != nil {
			return err
		}
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := datawire.NewDataServiceClient(conn)
	call, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if operation == "put" {
		_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
		response, err := client.PutRecord(call, &datawire.PutRecordRequest{ClusterId: id.ClusterID,
			FromRaftId: id.RaftID, RequestId: *requestID, TenantId: flags.Arg(0), DatasetId: flags.Arg(1),
			Key: []byte(flags.Arg(2)), Value: []byte(flags.Arg(3))})
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(response)
	}
	response, err := client.GetRecord(call, &datawire.GetRecordRequest{ClusterId: id.ClusterID,
		FromRaftId: id.RaftID, TenantId: flags.Arg(0), DatasetId: flags.Arg(1), Key: []byte(flags.Arg(2))})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}

func ensureRequestID(value *string) error {
	if *value == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		*value = "cli-" + hex.EncodeToString(random[:])
	}
	if len(*value) > 100 || !validCLIRequestID(*value) {
		return errors.New("request-id must be 1-100 letters, digits, periods, underscores or dashes")
	}
	return nil
}

func validCLIRequestID(value string) bool {
	if value == "" {
		return false
	}
	for i, c := range value {
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '.' || c == '_' || c == '-') {
			continue
		}
		return false
	}
	return true
}

func connectDataNode(ctx context.Context, store, address string) (*grpc.ClientConn, identity.Identity, error) {
	if ctx == nil {
		return nil, identity.Identity{}, errors.New("context required")
	}
	id, err := identity.Load(store)
	if err != nil {
		return nil, identity.Identity{}, err
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		return nil, identity.Identity{}, err
	}
	conn, err := grpc.NewClient("passthrough:///"+address,
		grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(rootjoinrpc.MaxResponseSize), grpc.MaxCallSendMsgSize(9<<20)))
	if err != nil {
		return nil, identity.Identity{}, err
	}
	// Root discovery includes a linearizable ReadIndex. On a low-cost,
	// cross-region cluster the control-plane barrier can legitimately take
	// longer than a single WAN RTT while followers apply the committed tail.
	// Keep the probe bounded, but do not fail before the server's own
	// 30-second request budget can complete.
	probe, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var actual peer.Peer
	response, err := rootjoinwire.NewRootJoinServiceClient(conn).DiscoverRoot(probe,
		&rootjoinwire.DiscoverRootRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}, grpc.Peer(&actual))
	if err != nil {
		conn.Close()
		return nil, identity.Identity{}, err
	}
	if err := (protocol.Advertisement{ProtocolMin: int(response.GetProtocolMin()), ProtocolMax: int(response.GetProtocolMax()),
		StateFormat: int(response.GetStateFormat()), PackFormat: int(response.GetPackFormat()), RecordIndex: int(response.GetRecordIndex())}).Validate(); err != nil {
		conn.Close()
		return nil, identity.Identity{}, fmt.Errorf("incompatible Root protocol or durable format: %w", err)
	}
	view, err := rootjoinrpc.DecodeView(response.GetView(), id.ClusterID)
	if err != nil || !view.Contains(id.RaftID) {
		conn.Close()
		return nil, identity.Identity{}, errors.New("Node is not in confirmed Root membership")
	}
	info, ok := actual.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		conn.Close()
		return nil, identity.Identity{}, errors.New("data server has no verified Node identity")
	}
	remote, err := identity.ParseNodeCertificate(info.State.PeerCertificates[0])
	if err != nil || remote.ClusterID != id.ClusterID || !view.Contains(remote.RaftID) {
		conn.Close()
		return nil, identity.Identity{}, errors.New("data server is not a current Root member")
	}
	for _, node := range view.Peers {
		if node.RaftID == remote.RaftID && node.NodeID == remote.NodeID {
			return conn, id, nil
		}
	}
	conn.Close()
	return nil, identity.Identity{}, errors.New("data server certificate differs from Root registration")
}
