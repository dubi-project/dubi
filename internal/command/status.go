package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
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
	Reasons             []string           `json:"reasons,omitempty"`
}

type regionHealthView struct {
	Region     string `json:"region"`
	Nodes      int    `json:"nodes"`
	RootVoters int    `json:"root_voters"`
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
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	health := deriveLocalHealth(storage, storageError, repair, repairError)
	cluster := deriveClusterHealth(view, health, true)
	return encoder.Encode(struct {
		ObservedNodeID    string            `json:"observed_node_id"`
		Root              rootjoin.View     `json:"root"`
		Cluster           clusterHealthView `json:"cluster"`
		Health            localHealthView   `json:"health"`
		LocalStorage      *volume.Report    `json:"local_storage,omitempty"`
		LocalStorageError string            `json:"local_storage_error,omitempty"`
		LocalRepair       *localRepairView  `json:"local_repair,omitempty"`
		LocalRepairError  string            `json:"local_repair_error,omitempty"`
	}{ObservedNodeID: remote.NodeID, Root: view, Cluster: cluster, Health: health, LocalStorage: storage, LocalStorageError: storageError,
		LocalRepair: repair, LocalRepairError: repairError})
}
