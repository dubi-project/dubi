#!/usr/bin/env bash
# Local DEVELOPMENT evidence only. Three Nodes share one host and one failure
# domain; this validates the bootstrap/data path, not production durability.
set -euo pipefail

lab_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$lab_root"

lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/dubi-three-node.XXXXXX")
mount_dir="$lab_dir/mount"
image="$lab_dir/dubi-three-node.sparseimage"
binary="$lab_dir/dubi"
home_dir="$lab_dir/home"
n1_store="$mount_dir/n1"
n2_store="$mount_dir/n2"
n3_store="$mount_dir/n3"
n1_addr="127.0.0.1:${DUBI_LAB_PORT_BASE:-17443}"
n2_port=$(( ${DUBI_LAB_PORT_BASE:-17443} + 1 ))
n3_port=$(( ${DUBI_LAB_PORT_BASE:-17443} + 2 ))
n2_addr="127.0.0.1:${n2_port}"
n3_addr="127.0.0.1:${n3_port}"
n1_pid=""
n2_pid=""
n3_pid=""
mounted=0
size_gib=${DUBI_LAB_SIZE_GIB:-32}

die() {
	printf 'dubi three-node lab: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	set +e
	for pid in "$n1_pid" "$n2_pid" "$n3_pid"; do
		if [[ -n "$pid" ]]; then
			kill "$pid" 2>/dev/null
			wait "$pid" 2>/dev/null
		fi
	done
	if (( mounted )); then
		case "$(uname -s)" in
		Darwin) hdiutil detach -force "$mount_dir" >/dev/null 2>&1 ;;
		Linux) umount "$mount_dir" >/dev/null 2>&1 ;;
		esac
	fi
	chmod -R u+w -- "$lab_dir" 2>/dev/null || true
	rm -rf -- "$lab_dir"
}
trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || die "Go is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
mkdir -p "$mount_dir" "$home_dir"

case "$(uname -s)" in
Darwin)
	command -v hdiutil >/dev/null 2>&1 || die "hdiutil is required on macOS"
	hdiutil create -quiet -size "${size_gib}g" -type SPARSE -fs APFS -volname DUBI-LAB "$image" ||
		die "could not create the temporary sparse image"
	hdiutil attach -quiet -nobrowse -mountpoint "$mount_dir" "$image" >/dev/null ||
		die "could not mount the temporary sparse image"
	mounted=1
	;;
Linux)
	[[ "$(id -u)" == "0" ]] || die "Linux image mounting requires root; use a dedicated test filesystem"
	command -v truncate >/dev/null 2>&1 || die "truncate is required on Linux"
	command -v mkfs.ext4 >/dev/null 2>&1 || die "mkfs.ext4 is required on Linux"
	command -v mount >/dev/null 2>&1 || die "mount is required on Linux"
	truncate -s "${size_gib}G" "$image"
	mkfs.ext4 -F -q "$image"
	mount -o loop "$image" "$mount_dir"
	mounted=1
	;;
*)
	die "unsupported host OS; use a dedicated test filesystem"
	;;
esac

go build -o "$binary" ./cmd/dubi
export HOME="$home_dir"

start_server() {
	local name=$1 store=$2 address=$3
	"$binary" server --store "$store" --listen "$address" >"$lab_dir/$name.log" 2>&1 &
	eval "${name}_pid=\$!"
}

wait_status() {
	local name=$1 store=$2 address=$3 out="$lab_dir/$1.status.json" err="$lab_dir/$1.status.err"
	for _ in $(seq 1 180); do
		if "$binary" status --store "$store" --server "$address" >"$out" 2>"$err"; then
			return 0
		fi
		sleep 0.5
	done
	tail -60 "$lab_dir/$name.log" >&2 || true
	tail -20 "$err" >&2 || true
	die "$name did not expose a confirmed Root status"
}

wait_root_voters() {
	local store=$1 address=$2 out="$lab_dir/root.status.json" err="$lab_dir/root.status.err"
	for _ in $(seq 1 180); do
		if "$binary" status --store "$store" --server "$address" >"$out" 2>"$err" &&
			python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if len(d.get("root",{}).get("voters",[])) >= 3 else 1)' "$out"; then
			return 0
		fi
		sleep 0.5
	done
	tail -60 "$lab_dir/n1.log" >&2 || true
	tail -20 "$err" >&2 || true
	die "Root did not reach three voters"
}

issue_ticket() {
	local output=$1
	if ! "$binary" join token --store "$n1_store" --server "$n1_addr" >"$output" 2>"$output.err"; then
		tail -20 "$output.err" >&2 || true
		die "could not issue a one-time join credential"
	fi
	chmod 600 "$output"
}

start_join() {
	local name=$1 store=$2 address=$3 ticket=$4
	"$binary" join --ticket-file "$ticket" --store "$store" --listen "$address" >"$lab_dir/$name.join.out" 2>"$lab_dir/$name.join.err" &
	eval "${name}_pid=\$!"
}

retry_command() {
	local label=$1 out=$2 err=$3
	shift 3
	for _ in $(seq 1 180); do
		if "$@" >"$out" 2>"$err"; then
			return 0
		fi
		sleep 0.5
	done
	tail -20 "$err" >&2 || true
	die "$label did not succeed"
}

wait_record() {
	local label=$1 store=$2 address=$3 key=$4 expected=$5
	local out="$lab_dir/$label.get.json" err="$lab_dir/$label.get.err"
	for _ in $(seq 1 180); do
		if "$binary" record get --store "$store" --server "$address" demo items "$key" >"$out" 2>"$err" &&
			python3 -c 'import base64,json,sys; d=json.load(open(sys.argv[1])); want=base64.b64encode(sys.argv[2].encode()).decode(); sys.exit(0 if d.get("found") is True and d.get("value") == want else 1)' "$out" "$expected"; then
			return 0
		fi
		sleep 0.5
	done
	tail -20 "$err" >&2 || true
	die "$label did not read the expected value"
}

compare_put_results() {
	python3 -c 'import json,sys; a=json.load(open(sys.argv[1])); b=json.load(open(sys.argv[2])); fields=("state","applied_index","confirmation_index","region_generation"); sys.exit(0 if all(a.get(k)==b.get(k) for k in fields) else 1)' "$1" "$2" || die "same RequestID retry returned a different result"
}

printf 'temporary three-Node lab: %s\n' "$lab_dir"
"$binary" init --store "$n1_store" > /dev/null 2>"$lab_dir/init.err"
start_server n1 "$n1_store" "$n1_addr"
wait_status n1 "$n1_store" "$n1_addr"

issue_ticket "$lab_dir/n2.ticket"
start_join n2 "$n2_store" "$n2_addr" "$lab_dir/n2.ticket"
wait_status n2 "$n2_store" "$n2_addr"

issue_ticket "$lab_dir/n3.ticket"
start_join n3 "$n3_store" "$n3_addr" "$lab_dir/n3.ticket"
wait_status n3 "$n3_store" "$n3_addr"
wait_root_voters "$n1_store" "$n1_addr"
printf 'bootstrap gate: three Root voters confirmed\n'

retry_command "dataset create" "$lab_dir/dataset.json" "$lab_dir/dataset.err" \
	"$binary" dataset create --store "$n1_store" --server "$n1_addr" --request-id lab-dataset demo items
retry_command "initial record put" "$lab_dir/greeting.json" "$lab_dir/greeting.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id lab-greeting demo items greeting hello
wait_record greeting-from-n2 "$n2_store" "$n2_addr" greeting hello
wait_record greeting-from-n3 "$n3_store" "$n3_addr" greeting hello
printf 'data gate: Pack-backed write/read confirmed on all three Nodes\n'

retry_command "write with one Node offline" "$lab_dir/failover.json" "$lab_dir/failover.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id lab-failover demo items failover two-online
wait_record failover-from-n3 "$n3_store" "$n3_addr" failover two-online
printf 'failure gate: one stopped Node still leaves a writable/readable majority\n'

kill "$n2_pid" 2>/dev/null || true
wait "$n2_pid" 2>/dev/null || true
n2_pid=""

retry_command "retryable record put" "$lab_dir/retry-first.json" "$lab_dir/retry-first.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id lab-retry demo items retry same-request
retry_command "same RequestID retry" "$lab_dir/retry-second.json" "$lab_dir/retry-second.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id lab-retry demo items retry same-request
compare_put_results "$lab_dir/retry-first.json" "$lab_dir/retry-second.json"
wait_record retry-from-n3 "$n3_store" "$n3_addr" retry same-request
printf 'idempotency gate: repeated RequestID returned the same committed result\n'

start_server n2 "$n2_store" "$n2_addr"
wait_status n2 "$n2_store" "$n2_addr"
wait_record recovered-greeting "$n2_store" "$n2_addr" greeting hello
wait_record recovered-failover "$n2_store" "$n2_addr" failover two-online
wait_record recovered-retry "$n2_store" "$n2_addr" retry same-request
printf 'recovery gate: restarted Node read all data written while it was offline\n'
printf 'DUBI_THREE_NODE_LAB_PASS\n'
