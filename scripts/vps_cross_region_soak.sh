#!/usr/bin/env bash
set -uo pipefail

# Low-rate VPS acceptance workload. Run on a test Node so traffic originates
# from the VPS network and uses that Node's real identity.
binary=${DUBI_BINARY:-/usr/local/bin/dubi}
store=${DUBI_STORE:-/var/lib/dubi-lab6-ready}
tenant=${DUBI_SOAK_TENANT:-rootread}
duration=${DUBI_SOAK_DURATION_SECONDS:-21600}
interval=${DUBI_SOAK_INTERVAL_SECONDS:-30}
max_iterations=${DUBI_SOAK_MAX_ITERATIONS:-0}
status_interval=${DUBI_SOAK_STATUS_INTERVAL_SECONDS:-90}
allow_geo_degraded=${DUBI_SOAK_ALLOW_GEO_DEGRADED:-0}
geo_settle_seconds=${DUBI_SOAK_GEO_SETTLE_SECONDS:-0}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
diagnostics=${DUBI_SOAK_DIAGNOSTICS:-$script_dir/vps_soak_diagnostics.py}

if [[ ! -x "$binary" || ! -d "$store" ]]; then
	printf 'dubi binary or local Node store is unavailable\n' >&2
	exit 2
fi
if [[ ! -f "$diagnostics" ]]; then
	printf 'copy vps_soak_diagnostics.py beside this script before starting the workload\n' >&2
	exit 2
fi
for value in "$duration" "$interval" "$max_iterations" "$status_interval" "$geo_settle_seconds"; do
	if [[ ! "$value" =~ ^[0-9]+$ ]]; then
		printf 'duration and iteration values must be integers\n' >&2
		exit 2
	fi
done
if (( duration <= 0 || interval <= 0 || status_interval <= 0 )); then
	printf 'duration, interval, and status interval must be positive\n' >&2
	exit 2
fi

if (( geo_settle_seconds > 300 )) || [[ "$allow_geo_degraded" != 0 && "$geo_settle_seconds" != 0 ]]; then
    printf 'bounded Geo wait requires strict mode and at most 300 seconds\n' >&2
    exit 2
fi

epoch=$(date -u +%Y%m%dT%H%M%SZ)
run_dir=${DUBI_SOAK_DIR:-/var/tmp/dubi-vps-soak-$epoch}
mkdir -p "$(dirname -- "$run_dir")"
# Evidence from an earlier run must never be overwritten.
mkdir -- "$run_dir" || exit 2
metrics="$run_dir/metrics.tsv"
failures="$run_dir/failures.log"
printf 'utc\tevent\titeration\ttarget\telapsed_ms\tresult\n' >"$metrics"
printf 'started_utc=%s\nkey_epoch=%s\nstore=%s\ntenant=%s\nduration_seconds=%s\ninterval_seconds=%s\n' \
	"$(date -u +%FT%TZ)" "$epoch" "$store" "$tenant" "$duration" "$interval" >"$run_dir/run.info"
printf 'DUBI cross-Region soak started: %s\n' "$run_dir"
printf 'pid=%d\nallow_geo_degraded=%s\n' "$$" "$allow_geo_degraded" >>"$run_dir/run.info"
printf 'geo_settle_seconds=%s\n' "$geo_settle_seconds" >>"$run_dir/run.info"
iteration=0
incident_count=0
first_request_failure=1
had_request_failures=0
termination_reason=baseline
last_status="$run_dir/baseline.json"
sha256sum "$binary" "$0" "$diagnostics" >"$run_dir/binary-start-sha256.txt" 2>&1 || true
timeout 10 "$binary" version >"$run_dir/binary-version.txt" 2>&1 || true

capture_incident() {
	local reason=$1 stamp trigger=${2:-$last_status}
	incident_count=$((incident_count + 1))
	stamp="$(date -u +%Y%m%dT%H%M%SZ)-$incident_count"
	printf '%s incident: %s\n' "$(date -u +%FT%TZ)" "$reason" >>"$failures"
	# Preserve the exact status that triggered the guard, including peer issues.
	if [[ -f "$trigger" ]]; then
		cp -- "$trigger" "$run_dir/incident-$stamp-status.json"
	fi
	timeout 15 systemctl status dubi --no-pager >"$run_dir/incident-$stamp-systemd.txt" 2>&1 || true
	timeout 15 journalctl -u dubi --since '10 minutes ago' --no-pager -o short-iso -n 1500 >"$run_dir/incident-$stamp-journal.txt" 2>&1 || true
	sha256sum "$binary" >"$run_dir/incident-$stamp-binary-sha256.txt" 2>&1 || true
}

finalize() {
	local result=$1
	trap - EXIT
	if (( result != 0 && incident_count == 0 )); then
		capture_incident "workload exited with code $result"
	fi
	sha256sum "$binary" >"$run_dir/binary-finish-sha256.txt" 2>&1 || true
	if ! python3 - "$run_dir" <<'PY'
from pathlib import Path
import re, sys
p = Path(sys.argv[1])
before = (p / "binary-start-sha256.txt").read_text().split()
after = (p / "binary-finish-sha256.txt").read_text().split()
if not before or not after or not re.fullmatch(r"[a-f0-9]{64}", before[0]) or before[0] != after[0]:
    raise SystemExit("workload binary hash changed or could not be verified")
PY
	then
		capture_incident "workload binary hash changed or could not be verified"
		(( result != 0 )) || result=1
	fi
	printf 'finished_utc=%s\niterations=%d\nexit_code=%d\ntermination_reason=%s\n' "$(date -u +%FT%TZ)" "$iteration" "$result" "$termination_reason" >>"$run_dir/run.info"
	if ! python3 "$diagnostics" report "$run_dir" >"$run_dir/summary.log" 2>"$run_dir/summary.err"; then
		printf 'report generation failed; see %s\n' "$run_dir/summary.err" >&2
		(( result != 0 )) || result=2
		printf 'exit_code=%d\nreport_error=summary.err\n' "$result" >>"$run_dir/run.info"
	fi
	printf 'DUBI cross-Region soak finished: iterations=%d exit=%d logs=%s\n' "$iteration" "$result" "$run_dir"
	exit "$result"
}
trap 'finalize "$?"' EXIT
trap 'termination_reason=signal_interrupt; exit 130' INT
trap 'termination_reason=signal_terminate; exit 143' TERM

validate_status() {
	python3 - "$1" "$allow_geo_degraded" <<'PY'
import json, re, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    status = json.load(stream)
cluster = status.get("cluster", {})
if not cluster.get("root_quorum_confirmed"):
    raise SystemExit("Root quorum is not confirmed")
if status.get("local_repair", {}).get("total_issues", 0) != 0:
    raise SystemExit("local Owner repair has unresolved issues")
regions = cluster.get("regions", [])
if len(regions) < 2 or any(region.get("nodes", 0) < 3 for region in regions):
    raise SystemExit("cross-Region soak requires at least two three-Node Regions")
observations = cluster.get("repair_observations", [])
if not observations or any(not node.get("reachable") for node in observations):
    raise SystemExit("a cluster Node is unreachable")
for node in observations:
    if not node.get("worker_enabled") or not node.get("last_check_unix", 0):
        raise SystemExit(f"Node {node.get('name') or node.get('node_id')} repair worker has not completed a check; "
                         "require live worker evidence from the current status CLI")
    if node.get("repair_debt", 0) or node.get("write_quarantines", 0):
        details = [(issue.get("group_id"), issue.get("error")) for issue in node.get("issues", [])]
        raise SystemExit(f"Node {node.get('name') or node.get('node_id')} has repair debt={node.get('repair_debt', 0)}, "
                         f"write quarantines={node.get('write_quarantines', 0)}; issues={details}")
    for volume in node.get("volumes", []):
        if volume.get("state") != "removed" and volume.get("health") != "healthy":
            raise SystemExit("a Node has an unhealthy Volume")
if cluster.get("state") == "available":
    raise SystemExit(0)
if cluster.get("state") != "degraded" or sys.argv[2] != "1":
    raise SystemExit("cluster is not available")
reasons = cluster.get("reasons", [])
if not reasons or any(not (reason.startswith("Geo repair has ") or
                           re.fullmatch(r"Node .+ has \d+ repair/maintenance debt item\(s\)", reason))
                      for reason in reasons):
    raise SystemExit("degraded state includes a non-Geo repair issue")
PY
}

if ! "$binary" status --store "$store" >"$run_dir/baseline.json" 2>"$run_dir/baseline.err"; then
	printf 'baseline status failed: %s\n' "$(<"$run_dir/baseline.err")" >&2
	exit 2
fi
if ! validate_status "$run_dir/baseline.json" >"$run_dir/baseline-check.err" 2>&1; then
	printf 'baseline cluster is not available; see %s\n' "$run_dir/baseline.json" >&2
	cat "$run_dir/baseline-check.err" >&2
	exit 2
fi

started=$SECONDS
deadline=$((started + duration))
next_status=$((started + status_interval))
consecutive_write_failures=0
consecutive_read_failures=0
exit_code=0
read_targets=("local" "146.103.40.3:12443" "209.200.246.159:12443")
termination_reason=duration

while (( SECONDS < deadline )); do
	iteration=$((iteration + 1))
	cycle_started=$(date +%s%3N)
	key="soak-$epoch-$iteration"
	value="dubi-cross-region-soak:$epoch:$iteration"
	request_id="soak-$epoch-$iteration"
	put_out="$run_dir/current-put.json"
	put_err="$run_dir/current-put.err"
	put_started=$(date +%s%3N)
	if timeout 90 "$binary" record put --store "$store" --request-id "$request_id" \
		"$tenant" "$key" "$value" >"$put_out" 2>"$put_err"; then
		put_elapsed=$(( $(date +%s%3N) - put_started ))
		printf '%s\tput\t%d\tlocal\t%d\tok\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" >>"$metrics"
		consecutive_write_failures=0
	else
		put_elapsed=$(( $(date +%s%3N) - put_started ))
		printf '%s\tput\t%d\tlocal\t%d\tfail\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" >>"$metrics"
		printf '%s iteration=%d put failed after %dms: %s\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" "$(<"$put_err")" >>"$failures"
		consecutive_write_failures=$((consecutive_write_failures + 1))
		had_request_failures=1
		cp -- "$put_out" "$run_dir/failure-put-$iteration.json"
		cp -- "$put_err" "$run_dir/failure-put-$iteration.err"
		if (( first_request_failure )); then
			capture_incident "first request failure: put iteration=$iteration"
			first_request_failure=0
		fi
		if (( consecutive_write_failures >= 5 )); then
			termination_reason=consecutive_write_failures
			printf 'stopping after 5 consecutive write failures\n' >>"$failures"
			exit_code=1
			break
		fi
	fi

	if (( consecutive_write_failures == 0 )); then
		for target in "${read_targets[@]}"; do
			get_out="$run_dir/current-get.json"
			get_err="$run_dir/current-get.err"
			get_started=$(date +%s%3N)
			if [[ "$target" == "local" ]]; then
				get_cmd=("$binary" record get --store "$store" "$tenant" "$key")
			else
				get_cmd=("$binary" record get --store "$store" --server "$target" "$tenant" "$key")
			fi
			if timeout 90 "${get_cmd[@]}" >"$get_out" 2>"$get_err" && \
				python3 - "$get_out" "$value" 2>>"$get_err" <<'PY'
import base64, json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    result = json.load(stream)
expected = base64.b64encode(sys.argv[2].encode()).decode()
if result.get("found") is not True or result.get("value") != expected:
    raise SystemExit("record read did not return the value just written")
PY
			then
				get_elapsed=$(( $(date +%s%3N) - get_started ))
				printf '%s\tget\t%d\t%s\t%d\tok\n' "$(date -u +%FT%TZ)" "$iteration" "$target" "$get_elapsed" >>"$metrics"
				consecutive_read_failures=0
			else
				get_elapsed=$(( $(date +%s%3N) - get_started ))
				printf '%s\tget\t%d\t%s\t%d\tfail\n' "$(date -u +%FT%TZ)" "$iteration" "$target" "$get_elapsed" >>"$metrics"
				printf '%s iteration=%d target=%s read failed after %dms: %s\n' \
					"$(date -u +%FT%TZ)" "$iteration" "$target" "$get_elapsed" "$(<"$get_err")" >>"$failures"
				consecutive_read_failures=$((consecutive_read_failures + 1))
				had_request_failures=1
				safe_target=${target//[^a-zA-Z0-9_.-]/_}
				cp -- "$get_out" "$run_dir/failure-get-$iteration-$safe_target.json"
				cp -- "$get_err" "$run_dir/failure-get-$iteration-$safe_target.err"
				if (( first_request_failure )); then
					capture_incident "first request failure: get iteration=$iteration target=$target"
					first_request_failure=0
				fi
				if (( consecutive_read_failures >= 10 )); then
					termination_reason=consecutive_read_failures
					printf 'stopping after 10 consecutive read failures\n' >>"$failures"
					exit_code=1
					break 2
				fi
			fi
		done
	fi

	if (( SECONDS >= next_status )); then
		status_out="$run_dir/status-$(date -u +%Y%m%dT%H%M%SZ).json"
		status_err="${status_out%.json}.err"
		last_status="$status_out"
		status_started=$(date +%s%3N)
		if timeout 90 "$binary" status --store "$store" >"$status_out.tmp" 2>"$status_err"; then
			mv -- "$status_out.tmp" "$status_out"
			status_elapsed=$(( $(date +%s%3N) - status_started ))
			if ! status_summary=$(python3 - "$status_out" <<'PY'
import datetime, json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    status = json.load(stream)
cluster = status.get("cluster", {})
root = status.get("root", {})
print(f"{cluster.get('state', 'unknown')}\t{root.get('leader_id', 0)}")
PY
			); then
				termination_reason=invalid_status
				capture_incident "status response is invalid JSON"
				exit_code=1
				break
			fi
			IFS=$'\t' read -r cluster_state leader_id <<<"$status_summary"
			printf '%s\tstatus\t0\tcluster\t%d\t%s:%s\n' "$(date -u +%FT%TZ)" "$status_elapsed" "$cluster_state" "$leader_id" >>"$metrics"
			if ! validate_status "$status_out" >"${status_out%.json}.check.err" 2>&1; then
                if (( geo_settle_seconds > 0 )) && python3 "$diagnostics" settle-geo "$status_out" \
                    --binary "$binary" --store "$store" --seconds "$geo_settle_seconds" \
                    >"${status_out%.json}.settle.log" 2>"${status_out%.json}.settle.err"; then
                    # Keep the original failed check and status; summary verifies
                    # the bounded recovery evidence before treating it as settled.
                    printf '%s\tgeo_settle\t0\tcluster\t0\tsettled\n' "$(date -u +%FT%TZ)" >>"$metrics"
                else
				termination_reason=health_guard
				capture_incident "health guard stopped soak: $(<"${status_out%.json}.check.err"); see $(basename "$status_out")"
				exit_code=1
				break
                fi
			fi
		else
			mv -- "$status_out.tmp" "$status_out"
			termination_reason=status_request_failed
			capture_incident "periodic status failed: $(<"$status_err")"
			exit_code=1
			break
		fi
		next_status=$((SECONDS + status_interval))
	fi

	if (( max_iterations > 0 && iteration >= max_iterations )); then
		termination_reason=iteration_limit
		break
	fi
	remaining=$((interval - ( $(date +%s%3N) - cycle_started ) / 1000))
	if (( remaining > 0 && SECONDS + remaining < deadline )); then
		sleep "$remaining"
	fi
done

# End with a fresh zero-debt observation, including the last write's Geo work.
if (( exit_code == 0 )); then
    last_status="$run_dir/status-final.json"
    if ! timeout 90 "$binary" status --store "$store" >"$last_status" 2>"$run_dir/status-final.err"; then
        termination_reason=final_status_failed
        exit_code=1
    elif ! validate_status "$last_status" >"$run_dir/status-final.check.err" 2>&1; then
        if ! (( geo_settle_seconds > 0 )) || ! python3 "$diagnostics" settle-geo "$last_status" \
            --binary "$binary" --store "$store" --seconds "$geo_settle_seconds" \
            >"$run_dir/status-final.settle.log" 2>"$run_dir/status-final.settle.err"; then
            termination_reason=final_health_guard
            exit_code=1
        fi
    fi
fi

if (( had_request_failures )); then
	exit_code=1
fi
exit "$exit_code"
