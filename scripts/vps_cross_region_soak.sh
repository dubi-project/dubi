#!/usr/bin/env bash
set -uo pipefail

# Low-rate VPS acceptance workload. Run on a test Node so traffic originates
# from the VPS network and uses that Node's real identity.
binary=${DUBI_BINARY:-/usr/local/bin/dubi}
store=${DUBI_STORE:-/var/lib/dubi-lab6-ready}
tenant=${DUBI_SOAK_TENANT:-rootread}
dataset=${DUBI_SOAK_DATASET:-smoke}
duration=${DUBI_SOAK_DURATION_SECONDS:-21600}
interval=${DUBI_SOAK_INTERVAL_SECONDS:-30}
max_iterations=${DUBI_SOAK_MAX_ITERATIONS:-0}
status_interval=${DUBI_SOAK_STATUS_INTERVAL_SECONDS:-900}

if [[ ! -x "$binary" || ! -d "$store" ]]; then
	printf 'dubi binary or local Node store is unavailable\n' >&2
	exit 2
fi
for value in "$duration" "$interval" "$max_iterations" "$status_interval"; do
	if [[ ! "$value" =~ ^[0-9]+$ ]]; then
		printf 'duration and iteration values must be integers\n' >&2
		exit 2
	fi
done
if (( duration <= 0 || interval <= 0 || status_interval <= 0 )); then
	printf 'duration, interval, and status interval must be positive\n' >&2
	exit 2
fi

epoch=$(date -u +%Y%m%dT%H%M%SZ)
run_dir=${DUBI_SOAK_DIR:-/var/tmp/dubi-vps-soak-$epoch}
mkdir -p "$run_dir"
metrics="$run_dir/metrics.tsv"
failures="$run_dir/failures.log"
printf 'utc\tevent\titeration\ttarget\telapsed_ms\tresult\n' >"$metrics"
printf 'started_utc=%s\nstore=%s\ntenant=%s\ndataset=%s\nduration_seconds=%s\ninterval_seconds=%s\n' \
	"$(date -u +%FT%TZ)" "$store" "$tenant" "$dataset" "$duration" "$interval" >"$run_dir/run.info"
printf 'DUBI cross-Region soak started: %s\n' "$run_dir"

if ! "$binary" status --store "$store" >"$run_dir/baseline.json" 2>"$run_dir/baseline.err"; then
	printf 'baseline status failed: %s\n' "$(<"$run_dir/baseline.err")" >&2
	exit 2
fi
if ! python3 - "$run_dir/baseline.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    status = json.load(stream)
if status.get("cluster", {}).get("state") != "available":
    raise SystemExit("cluster is not available at soak start")
PY
then
	printf 'baseline cluster is not available; see %s\n' "$run_dir/baseline.json" >&2
	exit 2
fi

started=$SECONDS
deadline=$((started + duration))
next_status=$((started + status_interval))
iteration=0
consecutive_write_failures=0
consecutive_read_failures=0
exit_code=0
read_targets=("local" "146.103.40.3:12443" "209.200.246.159:12443")

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
		"$tenant" "$dataset" "$key" "$value" >"$put_out" 2>"$put_err"; then
		put_elapsed=$(( $(date +%s%3N) - put_started ))
		printf '%s\tput\t%d\tlocal\t%d\tok\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" >>"$metrics"
		consecutive_write_failures=0
	else
		put_elapsed=$(( $(date +%s%3N) - put_started ))
		printf '%s\tput\t%d\tlocal\t%d\tfail\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" >>"$metrics"
		printf '%s iteration=%d put failed after %dms: %s\n' "$(date -u +%FT%TZ)" "$iteration" "$put_elapsed" "$(<"$put_err")" >>"$failures"
		consecutive_write_failures=$((consecutive_write_failures + 1))
		if (( consecutive_write_failures >= 5 )); then
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
				get_cmd=("$binary" record get --store "$store" "$tenant" "$dataset" "$key")
			else
				get_cmd=("$binary" record get --store "$store" --server "$target" "$tenant" "$dataset" "$key")
			fi
			if timeout 90 "${get_cmd[@]}" >"$get_out" 2>"$get_err" && \
				python3 - "$get_out" "$value" <<'PY'
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
				if (( consecutive_read_failures >= 10 )); then
					printf 'stopping after 10 consecutive read failures\n' >>"$failures"
					exit_code=1
					break 2
				fi
			fi
		done
	fi

	if (( SECONDS >= next_status )); then
		status_out="$run_dir/status-$(date -u +%Y%m%dT%H%M%SZ).json"
		status_err="$run_dir/status-current.err"
		status_started=$(date +%s%3N)
		if timeout 90 "$binary" status --store "$store" >"$status_out" 2>"$status_err"; then
			status_elapsed=$(( $(date +%s%3N) - status_started ))
			python3 - "$status_out" "$metrics" "$status_elapsed" <<'PY'
import datetime, json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    status = json.load(stream)
cluster = status.get("cluster", {})
root = status.get("root", {})
with open(sys.argv[2], "a", encoding="utf-8") as output:
    output.write(f"{datetime.datetime.now(datetime.timezone.utc).isoformat()}\tstatus\t0\tcluster\t{sys.argv[3]}\t{cluster.get('state', 'unknown')}:{root.get('leader_id', 0)}\n")
PY
		else
			printf '%s periodic status failed: %s\n' "$(date -u +%FT%TZ)" "$(<"$status_err")" >>"$failures"
		fi
		next_status=$((SECONDS + status_interval))
	fi

	if (( max_iterations > 0 && iteration >= max_iterations )); then
		break
	fi
	remaining=$((interval - ( $(date +%s%3N) - cycle_started ) / 1000))
	if (( remaining > 0 && SECONDS + remaining < deadline )); then
		sleep "$remaining"
	fi
done

printf 'finished_utc=%s\niterations=%d\nexit_code=%d\n' "$(date -u +%FT%TZ)" "$iteration" "$exit_code" >>"$run_dir/run.info"
printf 'DUBI cross-Region soak finished: iterations=%d exit=%d logs=%s\n' "$iteration" "$exit_code" "$run_dir"
exit "$exit_code"
