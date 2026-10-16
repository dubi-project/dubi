#!/usr/bin/env bash
# Local DEVELOPMENT evidence only. This models an explicit operator removal
# and a fresh Node joining the same Region; all processes share one host.
set -euo pipefail

lab_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$lab_root"

lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/dubi-node-removal.XXXXXX")
mount_dir="$lab_dir/mount"
image="$lab_dir/dubi-node-removal.sparseimage"
binary="$lab_dir/dubi"
home_dir="$lab_dir/home"
n1_store="$mount_dir/n1"
n2_store="$mount_dir/n2"
n3_store="$mount_dir/n3"
n4_store="$mount_dir/n4"
base_port=${DUBI_LAB_PORT_BASE:-21443}
n1_addr="127.0.0.1:${base_port}"
n2_addr="127.0.0.1:$((base_port + 1))"
n3_addr="127.0.0.1:$((base_port + 2))"
n4_addr="127.0.0.1:$((base_port + 3))"
size_gib=${DUBI_LAB_SIZE_GIB:-32}
n1_pid=""
n2_pid=""
n3_pid=""
n4_pid=""
mounted=0

die() {
	printf 'dubi node-removal lab: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	set +e
	for pid in "$n1_pid" "$n2_pid" "$n3_pid" "$n4_pid"; do
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
	if [[ "${DUBI_KEEP_LAB:-}" == "1" ]]; then
		printf 'keeping lab artifacts: %s\n' "$lab_dir" >&2
		return
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
	local node_name=${4:-}
	if [[ -n "$node_name" ]]; then
		"$binary" server --store "$store" --listen "$address" --name "$node_name" >"$lab_dir/$name.log" 2>&1 &
	else
		"$binary" server --store "$store" --listen "$address" >"$lab_dir/$name.log" 2>&1 &
	fi
	eval "${name}_pid=\$!"
}

start_join() {
	local name=$1 store=$2 address=$3 ticket=$4
	local node_name=${5:-$name}
	"$binary" join --ticket-file "$ticket" --store "$store" --listen "$address" --name "$node_name" >"$lab_dir/$name.join.out" 2>"$lab_dir/$name.join.err" &
	eval "${name}_pid=\$!"
}

wait_status() {
	local name=$1 store=$2 address=$3 out="$lab_dir/$1.status.json" err="$lab_dir/$1.status.err"
	for _ in $(seq 1 240); do
		if "$binary" status --store "$store" --server "$address" >"$out" 2>"$err"; then
			return 0
		fi
		sleep 0.5
	done
	tail -80 "$lab_dir/$name.log" >&2 || true
	tail -30 "$err" >&2 || true
	die "$name did not expose a confirmed Root status"
}

wait_name() {
	local expected=$1
	for _ in $(seq 1 240); do
		if "$binary" status --store "$n1_store" --server "$n1_addr" >"$lab_dir/name-$expected.status.json" 2>"$lab_dir/name-$expected.status.err" &&
			python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if any(p.get("name")==sys.argv[2] for p in d.get("root",{}).get("peers",[])) else 1)' "$lab_dir/name-$expected.status.json" "$expected"; then
			return 0
		fi
		sleep 0.5
	done
	tail -40 "$lab_dir/name-$expected.status.err" >&2 || true
	die "Root did not publish Node name $expected"
}

issue_ticket() {
	local output=$1
	if ! "$binary" join token --store "$n1_store" --server "$n1_addr" >"$output" 2>"$output.err"; then
		tail -20 "$output.err" >&2 || true
		die "could not issue a one-time join credential"
	fi
	chmod 600 "$output"
}

retry_command() {
	local label=$1 out=$2 err=$3
	shift 3
	for _ in $(seq 1 240); do
		if "$@" >"$out" 2>"$err"; then
			return 0
		fi
		sleep 0.5
	done
	tail -30 "$err" >&2 || true
	die "$label did not succeed"
}

wait_record() {
	local label=$1 store=$2 address=$3 key=$4 expected=$5
	local out="$lab_dir/$label.get.json" err="$lab_dir/$label.get.err"
	for _ in $(seq 1 240); do
		if "$binary" record get --store "$store" --server "$address" demo items "$key" >"$out" 2>"$err" &&
			python3 -c 'import base64,json,sys; d=json.load(open(sys.argv[1])); want=base64.b64encode(sys.argv[2].encode()).decode(); sys.exit(0 if d.get("found") is True and d.get("value") == want else 1)' "$out" "$expected"; then
			return 0
		fi
		sleep 0.5
	done
	tail -30 "$err" >&2 || true
	die "$label did not read the expected value"
}

assert_removed_and_added() {
	local out=$1 old_id=$2 new_name=$3
	python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); names={p.get("name"):p.get("node_id") for p in d.get("root",{}).get("peers",[])}; sys.exit(0 if sys.argv[2] not in names.values() and sys.argv[3] in names and names[sys.argv[3]] != sys.argv[2] else 1)' "$out" "$old_id" "$new_name" || die "Root view did not fence the old identity and publish the new Node"
}

printf 'temporary node-removal lab: %s\n' "$lab_dir"
"$binary" init --store "$n1_store" > /dev/null 2>"$lab_dir/init.err"
start_server n1 "$n1_store" "$n1_addr" sh-01
wait_status n1 "$n1_store" "$n1_addr"
wait_name sh-01

issue_ticket "$lab_dir/n2.ticket"
start_join n2 "$n2_store" "$n2_addr" "$lab_dir/n2.ticket" sh-02
wait_status n2 "$n2_store" "$n2_addr"
retry_command "name second Node" "$lab_dir/n2-name.json" "$lab_dir/n2-name.err" \
	"$binary" node rename --store "$n2_store" --server "$n2_addr" sh-02
wait_name sh-02
issue_ticket "$lab_dir/n3.ticket"
start_join n3 "$n3_store" "$n3_addr" "$lab_dir/n3.ticket" sh-03
wait_status n3 "$n3_store" "$n3_addr"
retry_command "name third Node" "$lab_dir/n3-name.json" "$lab_dir/n3-name.err" \
	"$binary" node rename --store "$n3_store" --server "$n3_addr" sh-03
wait_name sh-03

for _ in $(seq 1 240); do
	if "$binary" status --store "$n1_store" --server "$n1_addr" >"$lab_dir/initial.status.json" 2>"$lab_dir/initial.status.err" &&
		python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if len(d.get("root",{}).get("voters",[])) >= 3 and len(d.get("root",{}).get("peers",[])) >= 3 else 1)' "$lab_dir/initial.status.json"; then
		break
	fi
	sleep 0.5
done
if ! old_id=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); p=next((p for p in d.get("root",{}).get("peers",[]) if p.get("name")=="sh-03"),None); sys.exit(1) if p is None else print(p.get("node_id"))' "$lab_dir/initial.status.json"); then
	cat "$lab_dir/initial.status.json" >&2 || true
	for log in "$lab_dir"/*.join.err; do cat "$log" >&2 || true; done
	die "could not capture the failed Node identity"
fi
printf 'identity gate: captured sh-03 as %s\n' "$old_id"

retry_command "dataset create" "$lab_dir/dataset.json" "$lab_dir/dataset.err" \
	"$binary" dataset create --store "$n1_store" --server "$n1_addr" --request-id removal-dataset demo items
retry_command "write before removal" "$lab_dir/before.json" "$lab_dir/before.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id removal-before demo items before survives
wait_record before-from-n3 "$n3_store" "$n3_addr" before survives
printf 'data gate: old three-Node cohort holds the initial record\n'

kill "$n3_pid" 2>/dev/null || true
wait "$n3_pid" 2>/dev/null || true
n3_pid=""
retry_command "write with failed Node offline" "$lab_dir/offline.json" "$lab_dir/offline.err" \
	"$binary" record put --store "$n1_store" --server "$n1_addr" --request-id removal-offline demo items offline majority

retry_command "manual node removal" "$lab_dir/remove.json" "$lab_dir/remove.err" \
	"$binary" node remove --store "$n1_store" --server "$n1_addr" sh-03
printf 'removal gate: administrator removed the failed Node and fenced its identity\n'

issue_ticket "$lab_dir/n4.ticket"
start_join n4 "$n4_store" "$n4_addr" "$lab_dir/n4.ticket" n4
wait_status n4 "$n4_store" "$n4_addr"
retry_command "name replacement Node" "$lab_dir/n4-name.json" "$lab_dir/n4-name.err" \
	"$binary" node rename --store "$n4_store" --server "$n4_addr" n4
wait_name n4

for _ in $(seq 1 300); do
	if "$binary" status --store "$n1_store" --server "$n1_addr" >"$lab_dir/recovery.status.json" 2>"$lab_dir/recovery.status.err" &&
		python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); peers=d.get("root",{}).get("peers",[]); names={p.get("name") for p in peers}; sys.exit(0 if "sh-03" not in names and "n4" in names else 1)' "$lab_dir/recovery.status.json"; then
		break
	fi
	sleep 0.5
done
assert_removed_and_added "$lab_dir/recovery.status.json" "$old_id" n4
wait_record repaired-before "$n4_store" "$n4_addr" before survives
wait_record repaired-offline "$n4_store" "$n4_addr" offline majority
printf 'rebalance gate: new Node joined the same Region and read old Pack data\n'

if ! new_id=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); p=next((p for p in d.get("root",{}).get("peers",[]) if p.get("name")=="n4"),None); sys.exit(1) if p is None else print(p.get("node_id"))' "$lab_dir/recovery.status.json"); then
	cat "$lab_dir/recovery.status.json" >&2 || true
	die "could not read the new Node identity"
fi
[[ "$old_id" != "$new_id" ]] || die "new Node reused the removed Node identity"
printf 'DUBI_NODE_REMOVAL_REBALANCE_LAB_PASS\n'
