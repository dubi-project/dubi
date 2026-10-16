#!/usr/bin/env bash
# Local DEVELOPMENT evidence only. This script creates a private, temporary
# filesystem image and removes it on exit; it never targets /var/lib/dubi.
set -euo pipefail

lab_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$lab_root"

lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/dubi-disk-pressure.XXXXXX")
mount_dir="$lab_dir/mount"
store_dir="$mount_dir/store"
image="$lab_dir/dubi-pressure.img"
binary="$lab_dir/dubi"
server_pid=""
mounted=0

size_gib=${DUBI_LAB_SIZE_GIB:-2}
target_free_kib=${DUBI_LAB_TARGET_FREE_KIB:-900000}
max_chunks=${DUBI_LAB_MAX_CHUNKS:-32}
listen="127.0.0.1:${DUBI_LAB_PORT:-17443}"
home_dir="$lab_dir/home"

die() {
	printf 'dubi disk-pressure lab: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	set +e
	if [[ -n "$server_pid" ]]; then
		kill "$server_pid" 2>/dev/null
		wait "$server_pid" 2>/dev/null
	fi
	if (( mounted )); then
		case "$(uname -s)" in
		Darwin)
			hdiutil detach -force "$mount_dir" >/dev/null 2>&1
			;;
		Linux)
			umount "$mount_dir" >/dev/null 2>&1
			;;
		esac
	fi
	rm -rf -- "$lab_dir"
}
trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || die "Go is required"
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
	[[ "$(id -u)" == "0" ]] || die "Linux image mounting requires root; use a pre-mounted test filesystem instead"
	command -v truncate >/dev/null 2>&1 || die "truncate is required on Linux"
	command -v mkfs.ext4 >/dev/null 2>&1 || die "mkfs.ext4 is required on Linux"
	command -v mount >/dev/null 2>&1 || die "mount is required on Linux"
	truncate -s "${size_gib}G" "$image"
	mkfs.ext4 -F -q "$image"
	mount -o loop "$image" "$mount_dir"
	mounted=1
	;;
*)
	die "unsupported host OS; use a dedicated small test filesystem"
	;;
esac

mkdir -p "$store_dir"
printf 'temporary filesystem: %s\n' "$mount_dir"
printf 'temporary Store: %s\n' "$store_dir"

go build -o "$binary" ./cmd/dubi
export HOME="$home_dir"

"$binary" init --store "$store_dir" >/dev/null
"$binary" server --store "$store_dir" --listen "$listen" >"$lab_dir/server.log" 2>&1 &
server_pid=$!

ready=0
for _ in $(seq 1 100); do
	if "$binary" status --store "$store_dir" --server "$listen" >"$lab_dir/status.json" 2>"$lab_dir/status.err"; then
		ready=1
		break
	fi
	sleep 0.1
done
(( ready )) || {
	tail -60 "$lab_dir/server.log" >&2 || true
	die "DUBI server did not become ready"
}

reserve="$mount_dir/.dubi-pressure-reserve"
: >"$reserve"
for _ in $(seq 1 "$max_chunks"); do
	free_kib=$(df -Pk "$mount_dir" | awk 'NR == 2 { print $4 }')
	[[ "$free_kib" =~ ^[0-9]+$ ]] || die "could not read free space from the lab filesystem"
	if (( free_kib < target_free_kib )); then
		break
	fi
	dd if=/dev/zero bs=64m count=1 >>"$reserve" 2>/dev/null || die "could not consume lab filesystem space"
done
sync
free_kib=$(df -Pk "$mount_dir" | awk 'NR == 2 { print $4 }')
[[ "$free_kib" =~ ^[0-9]+$ ]] || die "could not read final free space"
(( free_kib < target_free_kib )) || die "lab image did not reach the pressure threshold (free KiB: $free_kib)"
printf 'pressure reached: free_kib=%s\n' "$free_kib"

if "$binary" node rename --store "$store_dir" --server "$listen" pressure-lab >"$lab_dir/pressure.out" 2>"$lab_dir/pressure.err"; then
	tail -20 "$lab_dir/pressure.out" >&2 || true
	die "a new Root proposal succeeded while the Store was under pressure"
fi
if ! grep -Eqi 'pressure|exhaust|retry' "$lab_dir/pressure.err" "$lab_dir/pressure.out"; then
	tail -20 "$lab_dir/pressure.err" >&2 || true
	die "the rejected proposal did not report a pressure/resource error"
fi
printf 'pressure gate: new proposal rejected\n'

rm -f -- "$reserve"
sync
recovered=0
for _ in $(seq 1 80); do
	if "$binary" node rename --store "$store_dir" --server "$listen" pressure-lab >"$lab_dir/recovery.out" 2>"$lab_dir/recovery.err"; then
		recovered=1
		break
	fi
	sleep 0.25
done
(( recovered )) || {
	tail -20 "$lab_dir/recovery.err" >&2 || true
	die "the Store did not pass the pressure recovery hysteresis"
}
printf 'recovery gate: new proposal accepted\n'
printf 'DUBI_DISK_PRESSURE_LAB_PASS\n'
