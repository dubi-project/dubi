package recordwrite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GeoMetadataReplicationError identifies the exact Region mirror that still
// needs durable delivery. A retryable error means no voter accepted the
// deterministic mirror request; a non-retryable error means the destination
// applied a durable rejection and retrying another voter cannot help.
type GeoMetadataReplicationError struct {
	RegionID  string
	Mirror    routedstate.GeoMetadataMirror
	Retryable bool
	Cause     error
}

// ReplicateGeoMetadataWithRetryStore is the durable form of metadata mirror
// replication. A mirror is recorded before delivery, removed only after a
// durable destination Result, and retained when every voter is temporarily
// unavailable. Durable destination rejection also remains in the ledger until
// a fresh placement/publication retry explicitly supersedes it.
func ReplicateGeoMetadataWithRetryStore(ctx context.Context, transport GeoMetadataMirrorTransport,
	plan placement.GeoPlan, mirrors []routedstate.GeoMetadataMirror, retry *GeoMetadataRetryStore) ([]enginestate.Result, error) {
	return replicateGeoMetadataWithCompletion(ctx, transport, plan, mirrors, retry, nil)
}

// A background page may defer deletion of a successful mirror until its
// workers join. Debt stays durable meanwhile; an interrupted page can replay
// it safely. Normal callers retain immediate per-mirror deletion.
func replicateGeoMetadataWithCompletion(ctx context.Context, transport GeoMetadataMirrorTransport,
	plan placement.GeoPlan, mirrors []routedstate.GeoMetadataMirror, retry *GeoMetadataRetryStore,
	completed func(routedstate.GeoMetadataMirror) error) ([]enginestate.Result, error) {
	if retry == nil {
		return ReplicateGeoMetadata(ctx, transport, plan, mirrors)
	}
	if ctx == nil || transport == nil || len(mirrors) == 0 {
		return nil, errors.New("Geo metadata replication requires a transport and mirrors")
	}
	known := make(map[string]placement.LocalPlan)
	for _, region := range plan.Regions() {
		known[region.RegionID] = region
	}
	seen := make(map[string]bool)
	for _, mirror := range mirrors {
		region, found := known[mirror.RegionID]
		if !found || seen[mirror.RegionID] || !mirrorMatchesRegion(mirror, region) {
			return nil, errors.New("Geo metadata mirrors differ from the current plan")
		}
		seen[mirror.RegionID] = true
	}
	started := time.Now()
	err := retry.saveMirrors(mirrors)
	Trace(ctx, "repair.mirror_save", started)
	if err != nil {
		return nil, fmt.Errorf("save Geo metadata retry: %w", err)
	}
	var results []enginestate.Result
	var failures []error
	for _, mirror := range mirrors {
		result, err := RetryGeoMetadata(ctx, transport, known[mirror.RegionID], mirror)
		if err != nil {
			// Keep only the unfinished item as durable debt and move the fair
			// queue cursor past it. Rejection/exhaustion is never completion.
			if markErr := retry.MarkAttempt(mirror); markErr != nil {
				return nil, errors.Join(err, markErr)
			}
			failures = append(failures, err)
			if ctx.Err() != nil {
				return nil, errors.Join(failures...)
			}
			continue
		}
		results = append(results, result)
		if completed != nil {
			err = completed(mirror)
		} else {
			started := time.Now()
			err = retry.Delete(mirror)
			Trace(ctx, "repair.mirror_delete", started)
		}
		if err != nil {
			return nil, fmt.Errorf("delete completed Geo metadata retry: %w", err)
		}
	}
	if len(failures) != 0 {
		return nil, errors.Join(failures...)
	}
	return results, nil
}

func (e *GeoMetadataReplicationError) Error() string {
	if e == nil {
		return "geo metadata replication failed"
	}
	return fmt.Sprintf("Region %q geo metadata replication failed: %v", e.RegionID, e.Cause)
}

func (e *GeoMetadataReplicationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// GeoMetadataMirrors builds one destination-Region metadata checkpoint per
// verified Region. The caller/coordinator must submit each mirror to that
// Region Group and wait for its durable Result before treating the covered
// Region set as recoverable GeoReady metadata. Regions absent from evidence
// remain repair debt and are intentionally not required for the majority.
func GeoMetadataMirrors(target router.Target, descriptor records.Descriptor,
	publication placement.PublicationCut, plan placement.GeoPlan, evidence []placement.RegionHoldings) ([]routedstate.GeoMetadataMirror, error) {
	if target.Validate() != nil || descriptor.Validate() != nil || target.Keyspace != descriptor.Keyspace ||
		!bytes.Equal(target.RoutingKey, publication.RoutingKey) || publication.Validate() != nil {
		return nil, errors.New("geo metadata mirror differs from Owner route or publication")
	}
	recordID, err := descriptor.ID()
	if err != nil || publication.RecordID != recordID || publication.RangeID != target.RangeID || publication.RangeEpoch != target.Epoch {
		return nil, errors.New("geo metadata mirror differs from Range publication")
	}
	coverage, err := plan.AssessPublication(publication, evidence)
	if err != nil || !coverage.HasMajority() {
		return nil, errors.New("geo metadata mirrors require verified Region majority")
	}
	covered := make(map[string]bool, len(coverage.CoveredRegions))
	for _, regionID := range coverage.CoveredRegions {
		covered[regionID] = true
	}
	regions := plan.Regions()
	mirrors := make([]routedstate.GeoMetadataMirror, 0, len(covered))
	for _, region := range regions {
		if !covered[region.RegionID] {
			continue
		}
		mirrors = append(mirrors, routedstate.GeoMetadataMirror{Target: target, Key: slices.Clone(descriptor.Key),
			DestinationGroupID: region.GroupID, RegionID: region.RegionID, Generation: region.Generation,
			GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex, RecordID: recordID,
			ReferenceRequestID: publication.RequestID, ReferenceIndex: publication.Index,
			ReferenceCommandHash: publication.CommandHash, Descriptor: descriptor})
	}
	return mirrors, nil
}

// GeoMetadataMirrorTransport is the authenticated node-to-node path used to
// submit a MirrorGeo command to a destination engine Group. Implementations
// must return the destination's durable Result, not just a Raft proposal index.
type GeoMetadataMirrorTransport interface {
	MirrorGeo(context.Context, uint64, string, []byte) (enginestate.Result, error)
}

// ReplicateGeoMetadata submits each supplied covered-Region mirror to any
// current voter in its destination Group. A failed node is skipped; a durable
// rejection is returned immediately because retrying another voter cannot
// change the local fence. The supplied list may be a strict Region-majority
// subset of the full plan; omitted Regions remain repair debt.
// The request ID is deterministic per mirror, making retries safe after a lost
// response and avoiding a second metadata record.
func ReplicateGeoMetadata(ctx context.Context, transport GeoMetadataMirrorTransport,
	plan placement.GeoPlan, mirrors []routedstate.GeoMetadataMirror,
) ([]enginestate.Result, error) {
	if ctx == nil || transport == nil || len(mirrors) == 0 {
		return nil, errors.New("Geo metadata replication requires a transport and mirrors")
	}
	regions := plan.Regions()
	known := make(map[string]placement.LocalPlan, len(regions))
	for _, region := range regions {
		known[region.RegionID] = region
	}
	byRegion := make(map[string]routedstate.GeoMetadataMirror, len(mirrors))
	for _, mirror := range mirrors {
		if _, exists := byRegion[mirror.RegionID]; exists {
			return nil, errors.New("Geo metadata mirrors repeat a Region")
		}
		region, found := known[mirror.RegionID]
		if !found || !mirrorMatchesRegion(mirror, region) {
			return nil, fmt.Errorf("Geo metadata mirror %q differs from placement plan", mirror.RegionID)
		}
		byRegion[mirror.RegionID] = mirror
	}
	results := make([]enginestate.Result, 0, len(mirrors))
	for _, region := range regions {
		mirror, found := byRegion[region.RegionID]
		if !found {
			continue
		}
		applied, err := RetryGeoMetadata(ctx, transport, region, mirror)
		if err != nil {
			return nil, err
		}
		results = append(results, applied)
	}
	return results, nil
}

func mirrorMatchesRegion(m routedstate.GeoMetadataMirror, r placement.LocalPlan) bool {
	return m.Validate() == nil && m.RegionID == r.RegionID && m.DestinationGroupID == r.GroupID &&
		m.Generation == r.Generation && m.GroupEpoch == r.GroupEpoch && m.FenceIndex == r.FenceIndex
}

// RetryGeoMetadata delivers ONE mirror against a freshly authorized Region
// plan. Callers must also verify the exact current Owner publication first.
func RetryGeoMetadata(ctx context.Context, transport GeoMetadataMirrorTransport, region placement.LocalPlan,
	mirror routedstate.GeoMetadataMirror) (enginestate.Result, error) {
	if ctx == nil || transport == nil || !mirrorMatchesRegion(mirror, region) {
		return enginestate.Result{}, errors.New("Geo metadata retry differs from current Region plan")
	}
	requestID, err := mirrorRequestID(mirror)
	if err != nil {
		return enginestate.Result{}, err
	}
	encoded, err := routedstate.EncodeGeoMetadataMirror(requestID, mirror)
	if err != nil {
		return enginestate.Result{}, err
	}
	var failures []error
	var applied enginestate.Result
	var submitted bool
	nodes := region.Nodes()
	// A locally hosted Owner can submit its own checkpoint. Prefer it only if
	// it is in this unchanged confirmed cohort; never add a candidate or reduce
	// a majority. Other transports keep their original order and RPC behavior.
	if local, ok := transport.(interface{ preferredGeoMirrorNode(string) uint64 }); ok {
		preferred := local.preferredGeoMirrorNode(region.GroupID)
		for i, node := range nodes {
			if preferred != 0 && node.RaftID == preferred {
				nodes[0], nodes[i] = nodes[i], nodes[0]
				break
			}
		}
	}
	for i, node := range nodes {
		if err := ctx.Err(); err != nil {
			return enginestate.Result{}, err
		}
		attempt, cancel := remoteCandidateContext(ctx, len(nodes)-i)
		result, submitErr := transport.MirrorGeo(attempt, node.RaftID, region.GroupID, encoded)
		cancel()
		if submitErr != nil {
			if ctx.Err() != nil {
				return enginestate.Result{}, ctx.Err()
			}
			if status.Code(submitErr) != codes.Unknown && !retryable(submitErr) && !errors.Is(submitErr, context.DeadlineExceeded) {
				return enginestate.Result{}, &GeoMetadataReplicationError{RegionID: region.RegionID, Mirror: mirror, Cause: submitErr}
			}
			failures = append(failures, fmt.Errorf("Node %d: %w", node.RaftID, submitErr))
			continue
		}
		if result.RequestID != requestID || result.Index == 0 {
			return enginestate.Result{}, &GeoMetadataReplicationError{RegionID: region.RegionID, Mirror: mirror,
				Cause: errors.New("destination returned an invalid Geo metadata Result")}
		}
		if result.Error != "" || !result.Applied {
			return enginestate.Result{}, &GeoMetadataReplicationError{RegionID: region.RegionID, Mirror: mirror,
				Cause: fmt.Errorf("destination rejected Geo metadata mirror: %s", result.Error)}
		}
		applied, submitted = result, true
		break
	}
	if !submitted {
		return enginestate.Result{}, &GeoMetadataReplicationError{RegionID: region.RegionID, Mirror: mirror,
			Retryable: true, Cause: fmt.Errorf("no reachable voter: %w", errors.Join(failures...))}
	}
	return applied, nil
}

func mirrorRequestID(mirror routedstate.GeoMetadataMirror) (string, error) {
	encoded, err := json.Marshal(mirror)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	requestID := "geo-mirror-" + hex.EncodeToString(digest[:])
	if rootstate.ValidateRequestID(requestID) != nil {
		return "", errors.New("generated Geo metadata mirror RequestID is invalid")
	}
	return requestID, nil
}
