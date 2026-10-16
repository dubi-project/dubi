#!/usr/bin/env bash
# Local DEVELOPMENT evidence, never a production release approval. All test
# state is temporary. Preserve reports; never remove user stores or old runs.
set -euo pipefail

gate_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$gate_root"
gate_output=$(mktemp -d "${TMPDIR:-/tmp}/dubi-foundation-gate.XXXXXX")
printf 'DUBI local foundation gate; reports: %s\n' "$gate_output"

gate_step=preflight
gate_summary="$gate_output/summary.txt"
trap 'gate_exit=$?; if (( gate_exit != 0 )); then printf "FAIL step=%s exit=%s\n" "$gate_step" "$gate_exit" | tee -a "$gate_summary"; fi' EXIT

{
  printf 'purpose=LOCAL_DEVELOPMENT_EVIDENCE_NOT_PRODUCTION_APPROVAL\n'
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'commit=%s\n' "$(git rev-parse HEAD)"
  printf 'source_root=%s\n' "$gate_root"
  go version
  go env GOOS GOARCH
  git status --porcelain=v1
} > "$gate_output/source.txt"
git diff --binary HEAD > "$gate_output/tracked.patch"

# Include untracked implementation files and distinguish deletions. Reports
# live outside the checkout so running the gate cannot change its fingerprint.
source_manifest() {
  while IFS= read -r -d '' gate_file; do
    if [[ -f "$gate_file" ]]; then
      shasum -a 256 -- "$gate_file"
    else
      printf 'ABSENT_OR_NONREGULAR %q\n' "$gate_file"
    fi
  done < <(git ls-files -z --cached --others --exclude-standard)
}
source_manifest > "$gate_output/source-before.sha256"

run_step() {
  gate_step=$1
  shift
  printf 'RUN %s\n' "$gate_step" | tee -a "$gate_summary"
  printf '%q ' "$@" > "$gate_output/$gate_step.command"
  printf '\n' >> "$gate_output/$gate_step.command"
  if "$@" > "$gate_output/$gate_step.log" 2>&1; then
    :
  else
    gate_command_exit=$?
    tail -60 "$gate_output/$gate_step.log" >&2
    return "$gate_command_exit"
  fi
  printf 'PASS %s\n' "$gate_step" | tee -a "$gate_summary"
}

run_step whitespace git diff --check
run_step tests go test -json ./... -count=1
run_step race go test -race -json ./... -count=1
run_step vet go vet ./...
run_step build go build ./...
run_step linux-build env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
run_step immutable-record-repetition go test -race -json ./internal/platform/storage/records ./internal/platform/storage/volume ./internal/platform/node ./internal/platform/cluster/enginestate ./internal/platform/rpc/raftrpc -run 'TestDescriptor|TestRecord|TestNodeRecordStore|TestStoreResource|TestRuntimeRecords|TestMutationOutcomeLimit|TestHandlerResultEncoding|TestBoundedStateRead|TestOversizedPersistedResult' -count=3
run_step fault-repetition go test -json ./internal/platform/storage/pack ./internal/platform/cluster/enginestate ./internal/platform/raft/... -run 'TestProcessCrash|TestCollectionCrashRecovery|TestPackedCrashRecoveryPublicationBoundaries|TestHostAdmissionRejectsNewWorkButPreservesReads|TestPressuredFollowerStillAppliesCommittedReplication|TestGroupPromotionRejectsCaughtUpButInactiveLearner|TestGroupLearnerJoinsPromotesAndMemberIsRemoved|TestLogRegistration|TestGroupFormation|TestGroupRegistry|TestGroupLostLogAndState' -count=3
run_step isolation-repetition go test -race -json ./internal/platform/cluster/enginestate ./internal/platform/rpc/raftrpc -run 'TestKeyspace|TestLegacyAndUnknownStateFormats|TestSnapshotRejectsCrossKeyspace|TestTransport.*(Budget|Lane)|TestSnapshot(Receive|Lease|Authorization)|TestFileSnapshot(Authorization|Receive)' -count=3
run_step snapshot-publication-repetition go test -json ./internal/platform/storage/snapshot -run 'TestStore|TestSnapshotStoreProcessCrashPublication|TestNodeSnapshotStore|TestStaging|TestStreamNarrowAllocationBounds' -count=3
run_step root-uncertainty-repetition go test -race -json ./internal/platform/cluster/rootstate -run 'TestRootUncertainCommit|TestRootRestoreRejectsOversized|TestRegionCohort|TestRegionFormation|TestRegionGrowth' -count=3
run_step regional-fence-repetition go test -race -json ./internal/platform/cluster/regionfence ./internal/platform/cluster/enginestate ./internal/platform/cluster/routedstate ./internal/platform/rpc/raftrpc -run 'TestRegion.*(Fence|Handoff)|TestRegionObservation|TestAdapterRegion|TestLegacyAndUnknown' -count=3
run_step membership-fence-repetition go test -race -json ./internal/platform/raft ./internal/platform/cluster/enginestate ./internal/platform/cluster/rootstate ./internal/platform/cluster/groupmembership ./internal/platform/rpc/raftrpc -run 'TestMembershipCut|TestGroupMutation|TestJoinManager|TestMembershipFence|TestRoutedMembership|TestReconcile' -count=3
run_step prepared-snapshot-repetition go test -race -json ./internal/platform/raft/... -run 'TestPreparedSnapshot|TestLocalSnapshotPreserves' -count=3
run_step file-snapshot-repetition go test -race -json ./internal/platform/cluster/enginestate -run 'TestFileSnapshot' -count=3
run_step root-file-snapshot-repetition go test -race -json ./internal/platform/cluster/rootstate -run 'TestRootFileSnapshot' -count=3
run_step snapshot-wire-repetition go test -race -json ./internal/platform/rpc/raftrpc -run 'TestFileSnapshotWire|TestSnapshotDeadline' -count=3
run_step host-isolation-repetition go test -race -json ./internal/platform/raft -run 'TestHostBackgroundTicksIsolateBlockedGroup|TestHostOpeningRecoveryDoesNotHoldHostLockAndCloseWaits' -count=5
run_step snapshot-runtime-repetition go test -json ./internal/server -run '^TestThreeNodeEngineFileSnapshotCatchupAndRestart$' -count=2
run_step startup-lifecycle-repetition go test -race -json ./internal/command ./internal/platform/config ./internal/platform/node ./internal/server -run 'TestRootJoinStartup|TestServerCloseJoins|TestServerListenerFailure|TestInternalConnectionLifetime|TestAdvertise|TestBootstrapNode|TestManagedCLI|TestJoinCredentialCLI|TestJoinConnect|TestJoinCredentialInput|TestLocalNetwork|TestManagedServerInitRestart' -count=3
run_step enrollment-repetition go test -race -json ./internal/platform/identity/... ./internal/platform/cluster/rootstate ./internal/platform/cluster/enrollment ./internal/platform/cluster/rootjoin ./internal/platform/raft ./internal/platform/rpc/enrollmentrpc ./internal/platform/rpc/rootjoinrpc ./internal/platform/rpc/nodecertrpc ./internal/platform/rpc/tlscredentials ./internal/platform/rpc/raftrpc ./internal/server -run 'TestJoinGrant|TestJoinTicket|TestJoinReceipt|TestRootJoin|TestRootRouting|TestRootMember|TestEnrollmentCertificate|TestEnrollmentNetwork|TestEnrollmentInstallRetry|TestLeafRenewal|TestLeafReload|TestTLSDeadline|TestNodeRenewal|TestNodePKIRead|TestManagedCertificateRenewal|TestManagedEnrollmentServer|TestNodeCertificateRejectsNoncanonical|TestNodePeerAuthorization|TestVerifiedPeerCertificate|TestGroupRecoveryReplaysMembership|TestRootDirectoryUsesAppliedRoot' -count=3
run_step topology-repetition go test -race -json ./internal/platform/cluster/roottopology ./internal/platform/cluster/regiongroups ./internal/platform/node ./internal/platform/raft ./internal/platform/rpc/enrollmentrpc ./internal/server ./internal/command -run 'TestRootTopology|TestGroupTopology|TestGroupPromotionRejectsCaughtUpButInactiveLearner|TestEnrollmentNetworkRetriesEnrollment|TestEnrollmentRetry|TestRootControlTasks|TestRegionalFormation|TestRegionalConfirmation|TestRegionalGrowth|TestRuntimeRegionFormation|TestManagedRootTopologyAutomaticQuorumFailoverAndColdRestart|TestManagedRegionalGrowth' -count=3
run_step fuzz-record-header go test ./internal/platform/storage/pack -run '^$' -fuzz '^FuzzRecordHeader$' -fuzztime=10s -parallel=2
run_step fuzz-keyspace go test ./internal/platform/cluster/enginestate -run '^$' -fuzz '^FuzzKeyspaceValueKey$' -fuzztime=10s -parallel=2
run_step fuzz-snapshot-stream go test ./internal/platform/storage/snapshot -run '^$' -fuzz '^FuzzSnapshotStream$' -fuzztime=10s -parallel=2
run_step fuzz-join-ticket go test ./internal/platform/identity/enrollment -run '^$' -fuzz '^FuzzJoinTicketDecode$' -fuzztime=10s -parallel=2
run_step fuzz-root-join-view go test ./internal/platform/rpc/rootjoinrpc -run '^$' -fuzz '^FuzzRootJoinView$' -fuzztime=10s -parallel=2

gate_step=source-stability
source_manifest > "$gate_output/source-after.sha256"
cmp "$gate_output/source-before.sha256" "$gate_output/source-after.sha256"

# A green package that skipped a required integration test is not a green
# development gate. Keep skip evidence rather than silently granting a pass.
gate_step=required-test-coverage
if awk '/"Action":"skip"/ && /"Test":/ { print; skipped=1 } END { exit !skipped }' "$gate_output/tests.log" "$gate_output/race.log" "$gate_output/immutable-record-repetition.log" "$gate_output/fault-repetition.log" "$gate_output/isolation-repetition.log" "$gate_output/snapshot-publication-repetition.log" "$gate_output/root-uncertainty-repetition.log" "$gate_output/prepared-snapshot-repetition.log" "$gate_output/file-snapshot-repetition.log" "$gate_output/root-file-snapshot-repetition.log" "$gate_output/snapshot-wire-repetition.log" "$gate_output/host-isolation-repetition.log" "$gate_output/snapshot-runtime-repetition.log" "$gate_output/startup-lifecycle-repetition.log" "$gate_output/enrollment-repetition.log" "$gate_output/topology-repetition.log" "$gate_output/regional-fence-repetition.log" "$gate_output/membership-fence-repetition.log" > "$gate_output/skipped-tests.txt"; then
  printf 'Required tests were skipped; see %s\n' "$gate_output/skipped-tests.txt" >&2
  exit 1
fi
printf 'LOCAL_DEVELOPMENT_GATE_PASS\nproduction_decision=NO_GO\ncompleted_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$gate_summary"
printf 'Reports retained at %s\n' "$gate_output"
