package command

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

const (
	// Peer repair status is an operator diagnostic, not a quorum path.  Keep
	// enough time for a fresh mutual-TLS connection on a small, cross-region
	// VPS; the old two-second limit produced transient false "unreachable"
	// observations while the Node was otherwise healthy.
	peerRepairProbeWindow  = 8 * time.Second
	peerRepairProbeTimeout = 5 * time.Second
	rootQuorumProbeWindow  = 30 * time.Second
	rootQuorumProbeTimeout = 25 * time.Second
)

// status is an operator view of one quorum-confirmed Root cut. It uses the
// Node's managed TLS identity and does not claim that listed data is ready.
type localRepairView struct {
	WorkerEnabled     bool                   `json:"worker_enabled"`
	LastCheckUnix     int64                  `json:"last_check_unix"`
	Issues            []localRepairIssue     `json:"issues"`
	TotalIssues       uint32                 `json:"total_issues"`
	RepairDebt        uint32                 `json:"repair_debt"`
	VolumeMigration   *volumeMigrationView   `json:"volume_migration,omitempty"`
	VolumeWriteErrors []volumeWriteIssueView `json:"volume_write_errors,omitempty"`
	MaintenanceBudget *resourceBudgetView    `json:"maintenance_budget,omitempty"`
	RequestBudget     *resourceBudgetView    `json:"request_budget,omitempty"`
	GeoMetadataDebt   uint64                 `json:"geo_metadata_debt"`
	GeoRecordDebt     uint64                 `json:"geo_record_debt"`
}

type volumeMigrationView struct {
	Active         bool   `json:"active"`
	SourceVolumeID string `json:"source_volume_id,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
	Migrated       uint64 `json:"migrated"`
	StartedUnix    int64  `json:"started_unix,omitempty"`
	LastError      string `json:"last_error,omitempty"`
}

type volumeWriteIssueView struct {
	VolumeID string `json:"volume_id"`
	Error    string `json:"error"`
}

type resourceBudgetView struct {
	LimitBytes uint64 `json:"limit_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
	PeakBytes  uint64 `json:"peak_bytes"`
	Rejected   uint64 `json:"rejected"`
}

type localRepairIssue struct {
	GroupID string `json:"group_id"`
	Error   string `json:"error"`
}

// localHealthView is deliberately scoped to observations made by this Node.
// It never turns a reachable Root endpoint into a claim that user data has a
// physical majority elsewhere in the Cluster.
type localHealthView struct {
	Scope   string   `json:"scope"`
	State   string   `json:"state"`
	Reasons []string `json:"reasons,omitempty"`
}

// clusterHealthView reports what the status request can prove from one
// current Root cut. Peer process liveness is intentionally not guessed from
// membership; the Root read confirmation is the majority signal.
type clusterHealthView struct {
	State               string             `json:"state"`
	RootQuorumConfirmed bool               `json:"root_quorum_confirmed"`
	RootVoters          int                `json:"root_voters"`
	RootQuorum          int                `json:"root_quorum"`
	Regions             []regionHealthView `json:"regions"`
	Repairs             []peerRepairView   `json:"repair_observations,omitempty"`
	Reasons             []string           `json:"reasons,omitempty"`
}

type regionHealthView struct {
	Region     string `json:"region"`
	Nodes      int    `json:"nodes"`
	RootVoters int    `json:"root_voters"`
}

type peerRepairView struct {
	RaftID           uint64           `json:"raft_id"`
	NodeID           string           `json:"node_id"`
	Reachable        bool             `json:"reachable"`
	Error            string           `json:"error,omitempty"`
	RepairDebt       uint32           `json:"repair_debt,omitempty"`
	GeoDebt          uint64           `json:"geo_debt,omitempty"`
	WriteQuarantines uint32           `json:"write_quarantines,omitempty"`
	Volumes          []peerVolumeView `json:"volumes,omitempty"`
}

type peerVolumeView struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Health string `json:"health"`
	Error  string `json:"error,omitempty"`
}

// statusReport is the shared wire shape for status and doctor. Keeping the
// report named lets doctor consume exactly the same authenticated observations
// without implementing a second cluster probe with different guarantees.
type statusReport struct {
	ObservedNodeID    string            `json:"observed_node_id"`
	Root              rootjoin.View     `json:"root"`
	Cluster           clusterHealthView `json:"cluster"`
	Health            localHealthView   `json:"health"`
	LocalStorage      *volume.Report    `json:"local_storage,omitempty"`
	LocalStorageError string            `json:"local_storage_error,omitempty"`
	LocalRepair       *localRepairView  `json:"local_repair,omitempty"`
	LocalRepairError  string            `json:"local_repair_error,omitempty"`
}

func deriveClusterHealth(view rootjoin.View, local localHealthView, rootQuorumConfirmed bool) clusterHealthView {
	result := clusterHealthView{State: "degraded", RootQuorumConfirmed: rootQuorumConfirmed,
		RootVoters: len(view.Voters), RootQuorum: (len(view.Voters) / 2) + 1}
	regions := make(map[string]*regionHealthView)
	for _, peer := range view.Peers {
		item := regions[peer.RegionID]
		if item == nil {
			item = &regionHealthView{Region: peer.RegionID}
			regions[peer.RegionID] = item
		}
		item.Nodes++
	}
	voters := make(map[uint64]struct{}, len(view.Voters))
	for _, id := range view.Voters {
		voters[id] = struct{}{}
	}
	for _, peer := range view.Peers {
		if _, ok := voters[peer.RaftID]; ok {
			regions[peer.RegionID].RootVoters++
		}
	}
	for _, item := range regions {
		result.Regions = append(result.Regions, *item)
	}
	slices.SortFunc(result.Regions, func(a, b regionHealthView) int { return strings.Compare(a.Region, b.Region) })
	if rootQuorumConfirmed {
		result.State = "available"
	} else {
		result.Reasons = append(result.Reasons, "Root majority was not confirmed")
	}
	if local.State == "degraded" {
		result.State = "degraded"
		result.Reasons = append(result.Reasons, local.Reasons...)
	}
	return result
}

func applyPeerRepairHealth(cluster clusterHealthView, observations []peerRepairView) clusterHealthView {
	cluster.Repairs = observations
	for _, observation := range observations {
		if !observation.Reachable {
			cluster.State = "degraded"
			cluster.Reasons = append(cluster.Reasons, fmt.Sprintf("Node %s repair status is unreachable", observation.NodeID))
			continue
		}
		if observation.Error != "" {
			cluster.State = "degraded"
			cluster.Reasons = append(cluster.Reasons, fmt.Sprintf("Node %s: %s", observation.NodeID, observation.Error))
		}
		if debt := uint64(observation.RepairDebt) + observation.GeoDebt + uint64(observation.WriteQuarantines); debt != 0 {
			cluster.State = "degraded"
			cluster.Reasons = append(cluster.Reasons, fmt.Sprintf("Node %s has %d repair/maintenance debt item(s)", observation.NodeID, debt))
		}
	}
	return cluster
}

func probePeerRepairs(ctx context.Context, id identity.Identity, clientTLS *tls.Config, view rootjoin.View) []peerRepairView {
	observations := make([]peerRepairView, 0, len(view.Peers))
	if len(view.Peers) == 0 {
		return observations
	}
	// The main status RPC has a short discovery/read deadline. Do not let a
	// slow local repair probe consume that deadline before peer health can be
	// observed; peer probes have their own bounded diagnostic window.
	probeParent := ctx
	if probeParent == nil || probeParent.Err() != nil {
		probeParent = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(probeParent, peerRepairProbeWindow)
	defer cancel()
	// Probe peers concurrently. A failed or slow peer must not consume the
	// shared status deadline and turn healthy peers later in the list into
	// false "unreachable" observations.
	results := make([]peerRepairView, len(view.Peers))
	var wg sync.WaitGroup
	for index, peerNode := range view.Peers {
		wg.Add(1)
		go func(index int, peerNode rootstate.Node) {
			defer wg.Done()
			results[index] = probePeerRepair(probeCtx, id, clientTLS, peerNode)
		}(index, peerNode)
	}
	wg.Wait()
	return results
}

func probePeerRepair(ctx context.Context, id identity.Identity, clientTLS *tls.Config, peerNode rootstate.Node) peerRepairView {
	observation := peerRepairView{RaftID: peerNode.RaftID, NodeID: peerNode.NodeID}
	if identityenrollment.ValidateNodeAddress(peerNode.Address) != nil {
		observation.Error = "Root member has no valid address"
		return observation
	}
	conn, err := grpc.NewClient("passthrough:///"+peerNode.Address,
		grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(rootjoinrpc.MaxResponseSize), grpc.MaxCallSendMsgSize(rootjoinrpc.MaxRequestSize)))
	if err != nil {
		observation.Error = err.Error()
		return observation
	}
	probe, cancel := context.WithTimeout(ctx, peerRepairProbeTimeout)
	var actual peer.Peer
	response, callErr := datawire.NewDataServiceClient(conn).ClusterRepairStatus(probe,
		&datawire.LocalRepairStatusRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}, grpc.Peer(&actual))
	cancel()
	_ = conn.Close()
	if callErr != nil {
		observation.Error = callErr.Error()
		return observation
	}
	info, ok := actual.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		observation.Error = "peer response has no verified Node TLS identity"
		return observation
	}
	remote, parseErr := identity.ParseNodeCertificate(info.State.PeerCertificates[0])
	if parseErr != nil || remote.ClusterID != id.ClusterID || remote.RaftID != peerNode.RaftID || remote.NodeID != peerNode.NodeID {
		observation.Error = "peer response certificate differs from Root membership"
		return observation
	}
	if response == nil || response.GetNodeId() != peerNode.NodeID || response.GetRaftId() != peerNode.RaftID ||
		len(response.GetIssues()) > 64 || len(response.GetVolumeWriteErrors()) > 64 {
		observation.Error = "peer returned invalid repair status"
		return observation
	}
	observation.Reachable = true
	observation.RepairDebt = response.GetTotalIssues()
	observation.GeoDebt = response.GetGeoMetadataDebt() + response.GetGeoRecordDebt()
	observation.WriteQuarantines = uint32(len(response.GetVolumeWriteErrors()))
	for _, volume := range response.GetVolumes() {
		if volume == nil {
			observation.Error = "peer returned an invalid Volume status"
			observation.Reachable = false
			return observation
		}
		observation.Volumes = append(observation.Volumes, peerVolumeView{ID: volume.GetVolumeId(), State: volume.GetState(), Health: volume.GetHealth(), Error: volume.GetError()})
		if volume.GetHealth() != "healthy" {
			observation.Error = "peer has an unhealthy Volume"
		}
	}
	return observation
}

func deriveLocalHealth(storage *volume.Report, storageErr string, repair *localRepairView, repairErr string) localHealthView {
	health := localHealthView{Scope: "local", State: "unknown"}
	if storageErr != "" {
		health.State = "degraded"
		health.Reasons = append(health.Reasons, "local storage: "+storageErr)
	} else if storage == nil {
		health.Reasons = append(health.Reasons, "local storage was not observed")
	} else {
		for _, item := range storage.Volumes {
			if item.Health != "healthy" {
				health.State = "degraded"
				health.Reasons = append(health.Reasons, fmt.Sprintf("volume %s is %s", item.ID, item.Health))
			}
		}
	}
	if repairErr != "" {
		health.State = "degraded"
		health.Reasons = append(health.Reasons, "repair status: "+repairErr)
	} else if repair == nil || !repair.WorkerEnabled {
		health.Reasons = append(health.Reasons, "local repair worker is not observable")
	} else if repair.TotalIssues != 0 {
		health.State = "degraded"
		health.Reasons = append(health.Reasons, fmt.Sprintf("local repair has %d issue(s)", repair.TotalIssues))
	} else if repair.LastCheckUnix == 0 && health.State == "unknown" {
		health.Reasons = append(health.Reasons, "local repair has not completed an observation")
	}
	if repair != nil && repair.GeoMetadataDebt+repair.GeoRecordDebt != 0 {
		health.State = "degraded"
		health.Reasons = append(health.Reasons, fmt.Sprintf("Geo repair has %d pending item(s)", repair.GeoMetadataDebt+repair.GeoRecordDebt))
	}
	if repair != nil && repair.VolumeMigration != nil && repair.VolumeMigration.Active {
		health.State = "degraded"
		reason := "Volume migration is in progress"
		if repair.VolumeMigration.LastError != "" {
			reason = "Volume migration: " + repair.VolumeMigration.LastError
		}
		health.Reasons = append(health.Reasons, reason)
	}
	if repair != nil && len(repair.VolumeWriteErrors) != 0 {
		health.State = "degraded"
		health.Reasons = append(health.Reasons, fmt.Sprintf("%d Volume write error(s) are quarantined", len(repair.VolumeWriteErrors)))
	}
	if health.State == "unknown" && len(health.Reasons) == 0 {
		health.State = "healthy"
	}
	return health
}

// confirmRootQuorum performs a real authenticated Root read barrier. A Root
// discovery response is only a membership snapshot; it can be served by a
// follower while the quorum is unavailable. Status must not call that
// snapshot a confirmed majority.
func confirmRootQuorum(ctx context.Context, id identity.Identity, clientTLS *tls.Config, view rootjoin.View) (bool, error) {
	if ctx == nil || clientTLS == nil || len(view.Voters) < 1 {
		return false, errors.New("Root quorum has no usable voters")
	}
	directory := raftrpc.NewStaticDirectory()
	for _, peerNode := range view.Peers {
		if err := directory.Update(peerNode.RaftID, peerNode.Address); err != nil {
			return false, err
		}
	}
	client, err := raftrpc.NewGroupClient(raftrpc.GroupClientOptions{
		ClusterID: id.ClusterID, RaftID: id.RaftID, Directory: directory,
		Credentials: tlscredentials.NewTLS(clientTLS), MaxSize: rootjoinrpc.MaxRequestSize,
	})
	if err != nil {
		return false, err
	}
	defer client.Close()
	targets := make([]uint64, 0, len(view.Voters))
	add := func(target uint64) {
		if target == 0 {
			return
		}
		for _, existing := range targets {
			if existing == target {
				return
			}
		}
		targets = append(targets, target)
	}
	add(view.LeaderID)
	for _, voter := range view.Voters {
		add(voter)
	}
	if len(targets) == 0 {
		return false, errors.New("Root quorum has no remote voter to probe")
	}
	probeCtx, probeCancel := context.WithCancel(ctx)
	defer probeCancel()
	type result struct {
		target uint64
		err    error
	}
	results := make(chan result, len(targets))
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(target uint64) {
			defer wg.Done()
			attempt, cancel := context.WithTimeout(probeCtx, rootQuorumProbeTimeout)
			defer cancel()
			var probeErr error
			if target == id.RaftID {
				address, found := directory.Resolve(target)
				if !found {
					probeErr = errors.New("local Root voter has no address")
				} else {
					conn, dialErr := grpc.NewClient(address, grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)))
					if dialErr != nil {
						probeErr = dialErr
					} else {
						_, probeErr = raftwire.NewGroupServiceClient(conn).ReadIndex(attempt,
							&raftwire.GroupReadIndexRequest{ClusterId: id.ClusterID, GroupId: raft.RootGroupID,
								FromRaftId: id.RaftID, ToRaftId: id.RaftID})
						_ = conn.Close()
					}
				}
			} else {
				_, probeErr = client.ReadIndex(attempt, target, raft.RootGroupID)
			}
			results <- result{target: target, err: probeErr}
		}(target)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	var failures []error
	for item := range results {
		if item.err == nil {
			probeCancel()
			return true, nil
		}
		failures = append(failures, fmt.Errorf("voter %d: %w", item.target, item.err))
	}
	return false, errors.Join(failures...)
}

func runStatus(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing managed Node store")
	address := flags.String("server", "", "reachable Node RPC address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	endpoint, err := resolveServer(flags, *address, *store)
	if err != nil {
		return err
	}
	*address = endpoint
	if ctx == nil || flags.NArg() != 0 || *store == "" || identityenrollment.ValidateNodeAddress(*address) != nil {
		return errors.New("usage: dubi status [--store <path>] [--server <host:port>]")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	requestCtx := ctx
	id, err := identity.Load(*store)
	if err != nil {
		return fmt.Errorf("load existing Node identity: %w", err)
	}
	_, clientTLS, err := config.LoadTLS(*store, id)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient("passthrough:///"+*address,
		grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(rootjoinrpc.MaxResponseSize), grpc.MaxCallSendMsgSize(rootjoinrpc.MaxRequestSize)))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var actual peer.Peer
	response, err := rootjoinwire.NewRootJoinServiceClient(conn).DiscoverRoot(ctx,
		&rootjoinwire.DiscoverRootRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}, grpc.Peer(&actual))
	if err != nil {
		return err
	}
	if err := (protocol.Advertisement{ProtocolMin: int(response.GetProtocolMin()), ProtocolMax: int(response.GetProtocolMax()),
		StateFormat: int(response.GetStateFormat()), PackFormat: int(response.GetPackFormat()), RecordIndex: int(response.GetRecordIndex())}).Validate(); err != nil {
		return fmt.Errorf("incompatible Root protocol or durable format: %w", err)
	}
	view, err := rootjoinrpc.DecodeView(response.GetView(), id.ClusterID)
	if err != nil {
		return fmt.Errorf("invalid live Root view: %w", err)
	}
	if !view.Contains(id.RaftID) {
		return errors.New("local Node is not in the confirmed Root membership")
	}
	info, ok := actual.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return errors.New("status server has no verified Node TLS identity")
	}
	remote, err := identity.ParseNodeCertificate(info.State.PeerCertificates[0])
	if err != nil || remote.ClusterID != id.ClusterID || !view.Contains(remote.RaftID) {
		return errors.New("status server identity is not a current Root member")
	}
	matched := false
	for _, node := range view.Peers {
		if node.RaftID == remote.RaftID && node.NodeID == remote.NodeID {
			matched = true
			break
		}
	}
	if !matched {
		return errors.New("status server certificate differs from its Root registration")
	}
	// The Root view describes cluster membership, not this machine's disks.
	// Inspect the managed local Store separately and keep the confirmed Root
	// view visible even when local storage is damaged or unavailable.
	localStorage, storageErr := volume.Inspect(*store)
	var storage *volume.Report
	var storageError string
	if storageErr != nil {
		storageError = storageErr.Error()
	} else {
		storage = &localStorage
	}
	var repair *localRepairView
	var repairError string
	if remote.RaftID == id.RaftID && remote.NodeID == id.NodeID {
		var repairPeer peer.Peer
		response, err := datawire.NewDataServiceClient(conn).LocalRepairStatus(ctx,
			&datawire.LocalRepairStatusRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}, grpc.Peer(&repairPeer))
		if err != nil {
			repairError = err.Error()
		} else if info, ok := repairPeer.AuthInfo.(credentials.TLSInfo); !ok ||
			len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
			repairError = "local repair server has no verified Node TLS identity"
		} else if observed, err := identity.ParseNodeCertificate(info.State.PeerCertificates[0]); err != nil ||
			observed.ClusterID != id.ClusterID || observed.NodeID != id.NodeID || observed.RaftID != id.RaftID {
			repairError = "local repair server identity differs from this Store"
		} else if response == nil || len(response.GetIssues()) > 64 || len(response.GetVolumeWriteErrors()) > 64 || response.GetTotalIssues() < uint32(len(response.GetIssues())) {
			repairError = "invalid local repair status response"
		} else {
			repair = &localRepairView{WorkerEnabled: response.GetWorkerEnabled(),
				LastCheckUnix: response.GetLastCheckUnix(), TotalIssues: response.GetTotalIssues(),
				RepairDebt: response.GetTotalIssues(),
				Issues:     make([]localRepairIssue, 0, len(response.GetIssues()))}
			if migration := response.GetVolumeMigration(); migration != nil {
				repair.VolumeMigration = &volumeMigrationView{Active: migration.GetActive(),
					SourceVolumeID: migration.GetSourceVolumeId(), Cursor: migration.GetCursor(),
					Migrated: migration.GetMigrated(), StartedUnix: migration.GetStartedUnix(),
					LastError: migration.GetLastError()}
			}
			for _, issue := range response.GetVolumeWriteErrors() {
				if issue != nil {
					repair.VolumeWriteErrors = append(repair.VolumeWriteErrors, volumeWriteIssueView{VolumeID: issue.GetVolumeId(), Error: issue.GetError()})
				}
			}
			if budget := response.GetMaintenanceBudget(); budget != nil {
				repair.MaintenanceBudget = &resourceBudgetView{LimitBytes: budget.GetLimitBytes(), UsedBytes: budget.GetUsedBytes(), PeakBytes: budget.GetPeakBytes(), Rejected: budget.GetRejected()}
			}
			if budget := response.GetRequestBudget(); budget != nil {
				repair.RequestBudget = &resourceBudgetView{LimitBytes: budget.GetLimitBytes(), UsedBytes: budget.GetUsedBytes(), PeakBytes: budget.GetPeakBytes(), Rejected: budget.GetRejected()}
			}
			repair.GeoMetadataDebt = response.GetGeoMetadataDebt()
			repair.GeoRecordDebt = response.GetGeoRecordDebt()
			for _, issue := range response.GetIssues() {
				if issue == nil {
					repairError = "invalid local repair status issue"
					repair = nil
					break
				}
				repair.Issues = append(repair.Issues, localRepairIssue{GroupID: issue.GetGroupId(), Error: issue.GetError()})
			}
		}
	}
	// Local repair inspection is deliberately bounded, but it must not consume
	// the only deadline available to the Root majority probe. A status command
	// is useful precisely when one local diagnostic is slow; keep the quorum
	// proof on its own bounded window while still honoring caller cancellation.
	rootProbeCtx, rootProbeCancel := context.WithTimeout(requestCtx, rootQuorumProbeWindow)
	rootQuorumConfirmed, rootQuorumErr := confirmRootQuorum(rootProbeCtx, id, clientTLS, view)
	rootProbeCancel()
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	health := deriveLocalHealth(storage, storageError, repair, repairError)
	cluster := deriveClusterHealth(view, health, rootQuorumConfirmed)
	if rootQuorumErr != nil && !rootQuorumConfirmed {
		cluster.Reasons = append(cluster.Reasons, "Root quorum probe failed: "+rootQuorumErr.Error())
	}
	cluster = applyPeerRepairHealth(cluster, probePeerRepairs(ctx, id, clientTLS, view))
	return encoder.Encode(statusReport{ObservedNodeID: remote.NodeID, Root: view, Cluster: cluster, Health: health, LocalStorage: storage, LocalStorageError: storageError,
		LocalRepair: repair, LocalRepairError: repairError})
}
