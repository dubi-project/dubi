package capstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func sealedReferenceFixture(t *testing.T, empty bool) (*State, regionfence.Handoff, []RecordReference) {
	return sealedReferenceFixtureWithOutcome(t, empty, 0)
}

func sealedReferenceFixtureWithOutcome(t *testing.T, empty bool, outcomeBytes int) (*State, regionfence.Handoff, []RecordReference) {
	t.Helper()
	s, intent, _ := handoffFixture(t, t.TempDir())
	t.Cleanup(func() { s.Close() })
	handler := referenceHandler()
	apply := handler.action
	handler.action = func(tx *Transaction, body []byte) (Outcome, error) {
		out, err := apply(tx, body)
		out.Value = bytes.Repeat([]byte("x"), outcomeBytes)
		return out, err
	}
	s.handler = handler
	var expected []RecordReference
	if !empty {
		for i, capability := range []string{"file", "cache", "message", "search"} {
			target := packedTarget(t, s, capability, fmt.Sprintf("tenant-%d", i))
			if err := s.ApplyRouted(s.AppliedIndex()+1, "reference-"+capability, target, []byte("payload/"+capability)); err != nil {
				t.Fatal(err)
			}
			r, found, err := readRecordReference(t, s, target)
			if err != nil || !found {
				t.Fatal(r, found, err)
			}
			expected = append(expected, r)
		}
	}
	handoffApply(t, s, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}, true)
	seal, found, err := s.RegionHandoff()
	if err != nil || !found {
		t.Fatal(seal, found, err)
	}
	return s, seal, expected
}

func buildRecordCut(t *testing.T, s *State, seal regionfence.Handoff, artifacts *snapshot.Store) RecordCut {
	t.Helper()
	p, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cut, err := p.Build(context.Background(), artifacts)
	if err != nil {
		t.Fatal(err)
	}
	return cut
}

func TestRecordCutFixedViewCanonicalDigestAndReopen(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			s, seal, expected := sealedReferenceFixture(t, empty)
			artifacts, path := fileArtifacts(t)
			p, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			captured := s.AppliedIndex()
			if _, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: captured}, seal); !errors.Is(err, ErrRecordCutBusy) {
				t.Fatal("unbounded preparations", err)
			}
			// A Raft entry after capture must not move this artifact's cut. An
			// activated live Group also must not rewrite the captured seal.
			candidate := handoffCandidate(t, s, seal.Intent)
			handoffApply(t, s, "activate", regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &candidate}, true)
			if len(expected) > 0 {
				r := expected[0]
				target := router.Target{Keyspace: r.Descriptor.Keyspace, RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: r.RangeEpoch, RoutingKey: r.Descriptor.Key}
				if err := s.ApplyRouted(s.AppliedIndex()+1, "after-activation", target, []byte("changed-after-capture")); err != nil {
					t.Fatal(err)
				}
				current, found, err := readRecordReference(t, s, target)
				if err != nil || !found || current.RequestID != "after-activation" {
					t.Fatal("live reference did not change", current, err)
				}
			}
			cut, err := p.Build(context.Background(), artifacts)
			if err != nil {
				t.Fatal(err)
			}
			if cut.Artifact.Index != captured || cut.Count != uint64(len(expected)) {
				t.Fatal(cut)
			}
			if _, err := p.Build(context.Background(), artifacts); err == nil {
				t.Fatal("build reused")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal); err == nil {
				t.Fatal("old seal was accepted after activation")
			}
			seen := map[string]RecordReference{}
			if err := WalkRecordCut(context.Background(), artifacts, cut, func(r RecordReference) error { seen[r.RequestID] = r; return nil }); err != nil {
				t.Fatal(err)
			}
			var total uint64
			for _, r := range expected {
				if !reflect.DeepEqual(seen[r.RequestID], r) {
					t.Fatal("cut changed", r, seen)
				}
				total += r.Descriptor.Size
			}
			if cut.PayloadBytes != total {
				t.Fatal("payload total", cut)
			}
			if err := artifacts.Close(); err != nil {
				t.Fatal(err)
			}
			artifacts, err = snapshot.OpenStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.Close()
			if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err != nil {
				t.Fatal("reopened cut", err)
			}
			// This artifact deliberately is not a capstate Raft state image.
			encoded, _ := cut.Artifact.Encode()
			target, err := Open(t.TempDir(), "data-1", referenceHandler())
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			if err := target.RestoreFrom(context.Background(), artifacts, cut.Artifact.Index, encoded, s.ConfState()); err == nil {
				t.Fatal("reference manifest installed as full Raft state")
			}
		})
	}
}

func TestRecordCutSameSealSetSurvivesLaterAppliedPositions(t *testing.T) {
	s, seal, _ := sealedReferenceFixture(t, false)
	artifacts, _ := fileArtifacts(t)
	first := buildRecordCut(t, s, seal, artifacts)
	// Replaying the same seal only advances applied, not the logical set.
	handoffApply(t, s, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &seal.Intent}, true)
	second := buildRecordCut(t, s, seal, artifacts)
	if first.Artifact.Index == second.Artifact.Index || first.Artifact.SHA256 == second.Artifact.SHA256 || first.RecordCutSummary != second.RecordCutSummary {
		t.Fatal("set digest depends on unrelated applied position", first, second)
	}
	if err := WalkRecordCut(context.Background(), artifacts, first, nil); err != nil {
		t.Fatal(err)
	}
	if err := WalkRecordCut(context.Background(), artifacts, second, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRecordCutRejectsBadBacklinksWithoutPublishing(t *testing.T) {
	for _, damage := range []string{"range", "keyspace", "result", "scope", "post-seal", "oversized"} {
		t.Run(damage, func(t *testing.T) {
			s, seal, refs := sealedReferenceFixture(t, false)
			artifacts, path := fileArtifacts(t)
			r := refs[0]
			switch damage {
			case "range":
				if err := s.db.Delete(fenceRangeKey(r.RangeID), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "keyspace":
				if err := s.db.Delete(fenceKeyspaceKey(r.Descriptor.Keyspace), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "result":
				if err := s.db.Delete(resultKey(r.RequestID), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "scope":
				key := recordReferenceKey(r.Descriptor.Keyspace, r.Descriptor.Key)
				r.Descriptor.Keyspace.TenantID = "other"
				b, _ := json.Marshal(r)
				if err := s.db.Set(key, b, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "post-seal":
				r.Index = seal.SealIndex
				b, _ := json.Marshal(r)
				if err := s.db.Set(recordReferenceKey(r.Descriptor.Keyspace, r.Descriptor.Key), b, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := s.db.Set(recordReferenceKey(r.Descriptor.Keyspace, r.Descriptor.Key), bytes.Repeat([]byte(" "), maxRecordReferenceBytes+1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			p, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if cut, err := p.Build(context.Background(), artifacts); err == nil || cut.Artifact.Size != 0 {
				t.Fatal("corrupt references published", cut, err)
			}
			files, err := filepath.Glob(filepath.Join(path, "*.snap"))
			if err != nil || len(files) != 0 {
				t.Fatal("partial artifact published", files, err)
			}
		})
	}
}

func TestRecordCutBarrierCancellationAndCloseOwnership(t *testing.T) {
	s, seal, _ := sealedReferenceFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PrepareRecordCut(ctx, &fakeReadBarrier{index: s.AppliedIndex()}, seal); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex() + 1}, seal); !errors.Is(err, ErrStateBehind) {
		t.Fatal(err)
	}
	p, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
	if err != nil {
		t.Fatal("failed prepare leaked admission", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		t.Fatal("DB closed with live preparation", err)
	case <-time.After(20 * time.Millisecond):
	}
	artifacts, _ := fileArtifacts(t)
	if _, err := p.Build(context.Background(), artifacts); err != nil {
		t.Fatal("captured view lost during Close", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("State.Close did not join")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: 1}, seal); err == nil {
		t.Fatal("closed State prepared cut")
	}
}

func TestRecordCutCancellationAndInputOwnership(t *testing.T) {
	s, seal, _ := sealedReferenceFixture(t, false)
	artifacts, path := fileArtifacts(t)
	barrier := &fakeReadBarrier{index: s.AppliedIndex()}
	failed := errors.New("read barrier unavailable")
	if _, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{err: failed}, seal); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	wrong := cloneCutSeal(seal)
	wrong.SealRequestID = "another-seal"
	if _, err := s.PrepareRecordCut(context.Background(), barrier, wrong); err == nil {
		t.Fatal("accepted another seal")
	}
	p, err := s.PrepareRecordCut(context.Background(), barrier, seal)
	if err != nil {
		t.Fatal("failed prepare leaked admission", err)
	}
	defer p.Close()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if cut, err := p.Build(canceled, artifacts); !errors.Is(err, context.Canceled) || cut.Artifact.Size != 0 {
		t.Fatal("canceled build published cut", cut, err)
	}
	if _, err := p.Build(context.Background(), artifacts); err == nil {
		t.Fatal("reused failed preparation")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(path, "*.snap"))
	if err != nil || len(files) != 0 {
		t.Fatal("canceled build left published artifact", files, err)
	}
	input := cloneCutSeal(seal)
	p, err = s.PrepareRecordCut(context.Background(), barrier, input)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	input.Intent.Previous.Fence.Members[0].RaftID = 999
	input.Intent.Next.Nodes[0] = 999
	cut, err := p.Build(context.Background(), artifacts)
	if err != nil || !reflect.DeepEqual(cut.Seal, seal) {
		t.Fatal("preparation retained input slices", cut, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	visits := 0
	visit := func(RecordReference) error { visits++; return nil }
	if err := WalkRecordCut(canceled, artifacts, cut, visit); !errors.Is(err, context.Canceled) || visits != 0 {
		t.Fatal("canceled walk visited records", visits, err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	err = WalkRecordCut(ctx, artifacts, cut, func(RecordReference) error { visits++; stop(); return nil })
	if !errors.Is(err, context.Canceled) || visits != 1 {
		t.Fatal("walk ignored cancellation", visits, err)
	}
	if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err != nil {
		t.Fatal("canceled walk damaged reusable artifact", err)
	}
}

func TestRecordCutWalkRejectsDescriptorMismatchAndDamagedArtifact(t *testing.T) {
	s, seal, _ := sealedReferenceFixture(t, false)
	artifacts, path := fileArtifacts(t)
	cut := buildRecordCut(t, s, seal, artifacts)
	for _, mode := range []string{"count", "bytes", "digest", "seal", "artifact-index"} {
		bad := cut
		bad.Seal = cloneCutSeal(cut.Seal)
		switch mode {
		case "count":
			bad.Count++
		case "bytes":
			bad.PayloadBytes++
		case "digest":
			bad.SHA256 = fmt.Sprintf("%064x", 1)
		case "seal":
			bad.Seal.SealRequestID = "other"
		case "artifact-index":
			bad.Artifact.Index++
		}
		if err := WalkRecordCut(context.Background(), artifacts, bad, nil); err == nil {
			t.Fatal("mismatched descriptor accepted", mode)
		}
	}
	stop := errors.New("visitor stopped")
	if err := WalkRecordCut(context.Background(), artifacts, cut, func(RecordReference) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err != nil {
		t.Fatal("visitor error poisoned artifact", err)
	}
	filename := filepath.Join(path, cut.Artifact.SHA256+".snap")
	file, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(cut.Artifact.Size)-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err == nil {
		t.Fatal("damaged footer accepted")
	}
}

func TestRecordCutBacklinksAreFromCapturedView(t *testing.T) {
	s, seal, refs := sealedReferenceFixture(t, false)
	artifacts, _ := fileArtifacts(t)
	p, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// Corrupt the LIVE backlink after capture. The old consistent view must
	// succeed, while a new capture/build must reject the broken latest state.
	if err := s.db.Delete(resultKey(refs[0].RequestID), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	cut, err := p.Build(context.Background(), artifacts)
	if err != nil {
		t.Fatal("mixed live and captured backlinks", err)
	}
	if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err != nil {
		t.Fatal(err)
	}
	p.Close()
	newer, err := s.PrepareRecordCut(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, seal)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	if _, err := newer.Build(context.Background(), artifacts); err == nil {
		t.Fatal("fresh cut accepted missing backlink")
	}
}

type cutTestRow struct{ key, value []byte }

func readCutRows(t *testing.T, artifacts *snapshot.Store, cut RecordCut) (snapshot.Header, []cutTestRow) {
	t.Helper()
	r, err := artifacts.Open(cut.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	d, err := snapshot.NewDecoder(r, cut.Artifact.Size)
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	var rows []cutTestRow
	for {
		key, value, err := d.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			r.Close()
			t.Fatal(err)
		}
		rows = append(rows, cutTestRow{key, value})
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return d.Header(), rows
}

func TestRecordCutWalkRejectsWellFramedMalformedManifests(t *testing.T) {
	for _, damage := range []string{"missing-header", "wrong-format", "raft-conf", "future-read", "unknown-header", "missing-summary", "extra-record", "unknown-reference", "noncanonical-reference", "post-seal-reference"} {
		t.Run(damage, func(t *testing.T) {
			s, seal, _ := sealedReferenceFixture(t, false)
			artifacts, _ := fileArtifacts(t)
			cut := buildRecordCut(t, s, seal, artifacts)
			header, rows := readCutRows(t, artifacts, cut)
			switch damage {
			case "missing-header":
				rows = rows[1:]
			case "wrong-format":
				header.Format = "capstate-v7"
			case "raft-conf":
				header.Conf = []byte{1}
			case "future-read":
				var meta recordCutMetadata
				json.Unmarshal(rows[0].value, &meta)
				meta.ReadIndex = header.Index + 1
				rows[0].value, _ = json.Marshal(meta)
			case "unknown-header":
				var meta map[string]any
				json.Unmarshal(rows[0].value, &meta)
				meta["local_path"] = "forbidden"
				rows[0].value, _ = json.Marshal(meta)
			case "missing-summary":
				rows = rows[:len(rows)-1]
			case "extra-record":
				rows = append(rows, cutTestRow{[]byte("trailing"), []byte("x")})
			case "unknown-reference":
				var r map[string]any
				json.Unmarshal(rows[1].value, &r)
				r["offset"] = 42
				rows[1].value, _ = json.Marshal(r)
			case "noncanonical-reference":
				rows[1].value = append(rows[1].value, ' ')
			case "post-seal-reference":
				var r RecordReference
				json.Unmarshal(rows[1].value, &r)
				r.Index = seal.SealIndex
				rows[1].value, _ = json.Marshal(r)
				// Recompute a consistent set hash, so publication chronology is
				// independently enforced instead of merely failing the digest.
				h := cutSetHash(seal)
				summary := RecordCutSummary{}
				for _, row := range rows[1 : len(rows)-1] {
					json.Unmarshal(row.value, &r)
					if err := addCutReference(&summary, h, row.key, row.value, r); err != nil {
						t.Fatal(err)
					}
				}
				summary.SHA256 = fmt.Sprintf("%x", h.Sum(nil))
				cut.RecordCutSummary = summary
				rows[len(rows)-1].value, _ = json.Marshal(summary)
			}
			ref, err := artifacts.Create(context.Background(), header, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, row := range rows {
					if err := e.Add(row.key, row.value); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			cut.Artifact = ref
			if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err == nil {
				t.Fatal("well-framed bad manifest accepted", damage)
			}
		})
	}
}

func TestRecordCutWalkDoesNotLoseFinalArtifactHashFailure(t *testing.T) {
	s, seal, _ := sealedReferenceFixture(t, false)
	artifacts, path := fileArtifacts(t)
	cut := buildRecordCut(t, s, seal, artifacts)
	verifiedVisits := 0
	if err := WalkVerifiedRecordCut(context.Background(), artifacts, cut, func(RecordReference) error {
		verifiedVisits++
		return nil
	}); err != nil || verifiedVisits != int(cut.Count) {
		t.Fatal("valid cut was not fully visited", verifiedVisits, err)
	}
	header, rows := readCutRows(t, artifacts, cut)
	var meta recordCutMetadata
	if err := json.Unmarshal(rows[0].value, &meta); err != nil {
		t.Fatal(err)
	}
	meta.ReadIndex-- // Still semantically valid, same digit width, different SHA.
	rows[0].value, _ = json.Marshal(meta)
	var replacement bytes.Buffer
	encoder, err := snapshot.NewEncoder(&replacement, header, snapshot.MaxStreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := encoder.Add(row.key, row.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Finish(); err != nil {
		t.Fatal(err)
	}
	if uint64(replacement.Len()) != cut.Artifact.Size {
		t.Fatal("fixture changed size before SHA check")
	}
	file, err := os.OpenFile(filepath.Join(path, cut.Artifact.SHA256+".snap"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(replacement.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	visited := 0
	err = WalkRecordCut(context.Background(), artifacts, cut, func(RecordReference) error { visited++; return nil })
	if err == nil || visited != int(cut.Count) || !bytes.Contains([]byte(err.Error()), []byte("SHA-256")) {
		t.Fatal("late hash failure lost or fixture failed early", visited, err)
	}
	visited = 0
	if err := WalkVerifiedRecordCut(context.Background(), artifacts, cut, func(RecordReference) error {
		visited++
		return nil
	}); err == nil || visited != 0 {
		t.Fatal("invalid cut invoked repair callback", visited, err)
	}
}

func TestRecordCutLargeResultBacklinksDoNotEnterManifest(t *testing.T) {
	s, seal, _ := sealedReferenceFixtureWithOutcome(t, false, 2<<20)
	artifacts, _ := fileArtifacts(t)
	cut := buildRecordCut(t, s, seal, artifacts)
	if cut.Count != 4 || cut.Artifact.Size > 64<<10 {
		t.Fatal("result bodies copied into reference manifest", cut)
	}
	if err := WalkRecordCut(context.Background(), artifacts, cut, nil); err != nil {
		t.Fatal(err)
	}
}

type cutRaftTransport struct{ messages chan *raftpb.Message }

func (n *cutRaftTransport) Send(_ string, m *raftpb.Message) error {
	select {
	case n.messages <- proto.Clone(m).(*raftpb.Message):
		return nil
	default:
		return errors.New("test Raft transport full")
	}
}

type cutGroupBarrier struct{ group *raft.Group }

func (b cutGroupBarrier) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if group != "data-1" {
		return 0, errors.New("wrong read Group")
	}
	return b.group.ReadIndex(ctx)
}

func TestRecordCutRealThreeVoterBarrierAndColdRestart(t *testing.T) {
	// Real etcd Raft, three separately persisted Groups, queued peer messages.
	// This is local temporary storage, not mTLS/WAN/independent disk evidence.
	root := t.TempDir()
	var groups map[uint64]*raft.Group
	var states map[uint64]*State
	var stopPump func()
	var pumpDone chan struct{}
	pumpErrors := make(chan error, 1)
	closeCluster := func() {
		if stopPump != nil {
			stopPump()
			<-pumpDone
			stopPump = nil
		}
		for _, g := range groups {
			if err := g.Close(); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(closeCluster)
	openCluster := func() {
		network := &cutRaftTransport{messages: make(chan *raftpb.Message, 4096)}
		groups = make(map[uint64]*raft.Group)
		states = make(map[uint64]*State)
		for id := uint64(1); id <= 3; id++ {
			store := filepath.Join(root, fmt.Sprint(id))
			s, err := Open(filepath.Join(store, "state"), "data-1", referenceHandler())
			if err != nil {
				t.Fatal(err)
			}
			a, err := routedstate.New(s)
			if err != nil {
				s.Close()
				t.Fatal(err)
			}
			g, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: store, GroupID: "data-1", RaftID: id, BootstrapPeers: []uint64{1, 2, 3}, State: a, Transport: network})
			if err != nil {
				s.Close()
				t.Fatal(err)
			}
			states[id], groups[id] = s, g
		}
		pumpCtx, cancel := context.WithCancel(context.Background())
		stopPump = cancel
		pumpDone = make(chan struct{})
		currentGroups, done := groups, pumpDone
		go func() {
			defer close(done)
			for {
				select {
				case <-pumpCtx.Done():
					return
				case message := <-network.messages:
					if err := currentGroups[message.GetTo()].Step(message); err != nil {
						select {
						case pumpErrors <- err:
						default:
						}
					}
				}
			}
		}()
		if err := groups[1].Campaign(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			status, err := groups[1].Status()
			if err == nil && status.LeaderID == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("real Group failed to elect", err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	openCluster()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	submit := func(command []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if err := groups[1].Submit(command); err != nil {
			t.Fatal(err)
		}
		if _, err := groups[1].ReadIndex(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := groups[1].ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	membership, err := states[1].MembershipCut()
	if err != nil {
		t.Fatal(err)
	}
	d := regionalDescriptor()
	d.MembershipIndex = membership.Index
	submit(routedstate.EncodeRegionFence("initial", d))
	r := rootstate.Range{RangeID: "r", TenantID: "tenant", CapabilityID: "search", DatasetID: "index", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	submit(routedstate.EncodeActivateRange("range", r))
	target := router.Target{Keyspace: r.Keyspace(), RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: 1, RoutingKey: []byte("item")}
	submit(routedstate.EncodeMutation("publish", target, []byte("real payload")))
	v, err := states[1].RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	intent := regionfence.HandoffIntent{Previous: v.Record, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: 11000, Nodes: []uint64{1, 2, 3, 4}, State: rootstate.RegionCohortPlanned}}
	submit(routedstate.EncodeRegionHandoff("seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}))
	seal, found, err := states[1].RegionHandoff()
	if err != nil || !found {
		t.Fatal(seal, found, err)
	}
	artifacts, _ := fileArtifacts(t)
	build := func(id uint64) RecordCut {
		t.Helper()
		p, err := states[id].PrepareRecordCut(ctx, cutGroupBarrier{groups[id]}, seal)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		cut, err := p.Build(ctx, artifacts)
		if err != nil {
			t.Fatal(err)
		}
		if err := WalkRecordCut(ctx, artifacts, cut, nil); err != nil {
			t.Fatal(err)
		}
		return cut
	}
	first, follower := build(1), build(2)
	if first.Count != 1 || first.RecordCutSummary != follower.RecordCutSummary {
		t.Fatal("replicated cuts differ", first, follower)
	}
	closeCluster()
	openCluster()
	restarted := build(2)
	if restarted.RecordCutSummary != first.RecordCutSummary || !reflect.DeepEqual(restarted.Seal, first.Seal) {
		t.Fatal("cold restart changed reference set", first, restarted)
	}
	select {
	case err := <-pumpErrors:
		t.Fatal("Raft message processing failed", err)
	default:
	}
}
