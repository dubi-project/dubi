package server

import (
	"context"
	"errors"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc"
)

type managementSource struct{ data *dataService }

func (s managementSource) Directory(ctx context.Context) (management.Directory, error) {
	if _, err := s.data.runtime.ReadRootIndex(ctx); err != nil {
		return management.Directory{}, err
	}
	cut, err := s.data.runtime.Root.RootRoutingView(4096)
	if err != nil {
		return management.Directory{}, err
	}
	d := management.Directory{ClusterID: s.data.runtime.Identity.ClusterID, Members: make([]management.Member, 0, len(cut.Nodes))}
	regionIndices := map[string]uint64{}
	for _, n := range cut.Nodes {
		if _, found := regionIndices[n.RegionID]; !found {
			cohort, _, err := s.data.runtime.Root.RegionCohort(n.RegionID, 1)
			if err != nil {
				return management.Directory{}, err
			}
			regionIndices[n.RegionID] = cohort.PlannedIndex
		}
		d.Members = append(d.Members, management.Member{CreatedIndex: n.CreatedIndex, RegionCreatedIndex: regionIndices[n.RegionID], NodeID: n.NodeID, Name: n.Name, RegionID: n.RegionID, RaftID: n.RaftID, Address: n.Address})
	}
	return d, nil
}
func (s managementSource) Observe(ctx context.Context, m management.Member) (management.Observation, error) {
	var response *datawire.LocalRepairStatusResponse
	var err error
	if m.NodeID == s.data.runtime.Identity.NodeID {
		if err = ctx.Err(); err == nil {
			response, err = s.data.localStatusResponse(true)
		}
	} else {
		if s.data.client == nil {
			return management.Observation{}, management.ErrUnavailable
		}
		conn, e := s.data.client.NodeAdminConnection(rootstate.Node{NodeID: m.NodeID, RaftID: m.RaftID, RegionID: m.RegionID, Name: m.Name, Address: m.Address})
		if e != nil {
			return management.Observation{}, e
		}
		response, err = datawire.NewDataServiceClient(conn).ClusterRepairStatus(ctx, &datawire.LocalRepairStatusRequest{
			ClusterId: s.data.runtime.Identity.ClusterID, FromRaftId: s.data.runtime.Identity.RaftID, IncludeVolumeDetails: true}, grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(4096))
	}
	if err != nil {
		return management.Observation{}, err
	}
	return managementObservation(m, response)
}
func unixObservation(n int64) *int64 {
	if n <= 0 {
		return nil
	}
	return &n
}
func managementObservation(m management.Member, r *datawire.LocalRepairStatusResponse) (management.Observation, error) {
	if r == nil || r.NodeId != m.NodeID || r.RaftId != m.RaftID || !r.VolumeDetailsSupported || r.ObservedUnix <= 0 || r.ObservedUnix > time.Now().Add(time.Minute).Unix() || len(r.Volumes) > 1024 || int(r.TotalVolumes) != len(r.Volumes) || len(r.Issues) > 64 || len(r.ScanProgress) > 64 || r.TotalIssues < uint32(len(r.Issues)) || r.TotalScanGroups < uint32(len(r.ScanProgress)) || r.FilesystemAvailableBytes > r.FilesystemTotalBytes {
		return management.Observation{}, errors.New("invalid or unsupported management observation")
	}
	o := management.Observation{Health: v1.OperationalHealth{Status: "healthy", Reasons: []v1.ObservationError{}}, ObservedUnix: r.ObservedUnix,
		Complete: r.TotalIssues == uint32(len(r.Issues)) && r.TotalScanGroups == uint32(len(r.ScanProgress)), Volumes: []v1.ManagedVolume{}}
	reason := func(code, message string) {
		o.Health.Status = "degraded"
		if len(o.Health.Reasons) < 64 {
			o.Health.Reasons = append(o.Health.Reasons, v1.ObservationError{NodeID: m.NodeID, Code: code, Message: message})
		}
	}
	if r.CapacitySupported && r.FilesystemTotalBytes > 0 {
		writable := r.WritableBytes
		o.Capacity = &v1.FilesystemCapacity{FilesystemTotalBytes: r.FilesystemTotalBytes, FilesystemAvailableBytes: r.FilesystemAvailableBytes, WritableBytes: &writable}
	}
	repair := v1.MaintenanceActivity{Kind: "repair", Supported: r.ScanProgressSupported, Phase: "unknown", Errors: []string{}}
	var scanned, pending uint64
	needsScan := false
	if r.ScanProgressSupported && r.WorkerEnabled && r.LastCheckUnix > 0 {
		repair.Phase = "observed"
		repair.Unit = "records_scanned"
		for _, p := range r.ScanProgress {
			if p == nil {
				return management.Observation{}, errors.New("invalid scan observation")
			}
			if p.Phase != "" {
				if repair.Phase == "observed" {
					repair.Phase = p.Phase
				} else if repair.Phase != p.Phase {
					repair.Phase = "multiple"
				}
			}
			scanned += p.Scanned
			pending += p.Unresolved
			needsScan = needsScan || p.NeedsCleanSweep
			if p.LastProgressUnix > 0 && (repair.LastProgressUnix == nil || p.LastProgressUnix > *repair.LastProgressUnix) {
				repair.LastProgressUnix = unixObservation(p.LastProgressUnix)
			}
		}
		repair.Processed = &scanned
		repair.Pending = &pending
		repair.NeedsFullScan = &needsScan
	}
	if !r.WorkerEnabled {
		repair.Phase = "disabled"
	}
	for _, issue := range r.Issues {
		if issue == nil {
			return management.Observation{}, errors.New("invalid issue observation")
		}
		repair.Errors = append(repair.Errors, issue.Error)
	}
	if r.TotalIssues > 0 || pending > 0 {
		reason("repair_pending", "Repair reports issues or unresolved records")
	}
	if !o.Complete {
		reason("observation_truncated", "Repair observation exceeds diagnostic detail limit")
	}
	if r.LastCheckUnix == 0 || !r.ScanProgressSupported {
		reason("repair_not_observed", "No completed repair observation is available")
	}
	geoPending := r.GeoMetadataDebt + r.GeoRecordDebt
	geo := v1.MaintenanceActivity{Kind: "geo_sync", Supported: true, Phase: "observed", Pending: &geoPending, Errors: []string{}}
	if geoPending > 0 {
		reason("geo_sync_pending", "Cross-region synchronization has pending records or metadata")
	}
	rebalance := v1.MaintenanceActivity{Kind: "rebalance", Supported: r.RebalanceSupported, Phase: "unknown", Errors: []string{}}
	if r.RebalanceSupported {
		rebalance.Phase = r.RebalancePhase
	}
	if r.MaintenanceIo != nil && r.MaintenanceIo.Active == "capacity-balance" && rebalance.Phase == "no_active_handoff" {
		rebalance.Phase = "checking"
	}
	migration := v1.MaintenanceActivity{Kind: "volume_migration", Supported: r.VolumeMigration != nil, Phase: "unknown", Errors: []string{}}
	if p := r.VolumeMigration; p != nil {
		migration.Phase = "idle"
		if p.Active {
			migration.Phase = "running"
		}
		migration.Processed = &p.Migrated
		migration.Unit = "records"
		migration.SourceVolumeID = p.SourceVolumeId
		migration.TargetVolumeID = p.TargetVolumeId
		if p.LastError != "" {
			migration.Errors = append(migration.Errors, p.LastError)
			reason("volume_migration_error", "Volume migration reports an error")
		}
	}
	seen := map[string]bool{}
	for _, v := range r.Volumes {
		if v == nil || v.VolumeId == "" || seen[v.VolumeId] || v.FilesystemAvailableBytes > v.FilesystemTotalBytes {
			return management.Observation{}, errors.New("invalid volume observation")
		}
		seen[v.VolumeId] = true
		item := v1.ManagedVolume{CreatedUnixNano: v.CreatedUnixNano, VolumeID: v.VolumeId, Path: v.Path, State: v.State, Health: v.Health, Error: v.Error, WriteIsolated: v.WriteIsolated, WriteError: v.WriteError}
		if v.CapacityKnown {
			item.Capacity = &v1.FilesystemCapacity{FilesystemTotalBytes: v.FilesystemTotalBytes, FilesystemAvailableBytes: v.FilesystemAvailableBytes}
		}
		if v.VolumeId == migration.SourceVolumeID || v.VolumeId == migration.TargetVolumeID {
			copy := migration
			item.Migration = &copy
		}
		if v.Health == "missing" || v.Health == "invalid" {
			o.Complete = false
		}
		if v.Health != "healthy" && v.Health != "removed" {
			reason("volume_unhealthy", "A registered volume is not healthy")
		}
		if v.WriteIsolated {
			reason("volume_write_isolated", "A volume is isolated from writes")
		}
		o.Volumes = append(o.Volumes, item)
	}
	if len(r.VolumeWriteErrors) > 0 {
		reason("volume_write_errors", "Storage reports write errors")
	}
	o.Maintenance = []v1.MaintenanceActivity{repair, rebalance, geo, migration}
	return o, nil
}
