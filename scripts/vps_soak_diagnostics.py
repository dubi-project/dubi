#!/usr/bin/env python3
"""Summarize a soak run, or collect its evidence and peer journals over SSH.

Only the collect command uses SSH. Keys stay on the machine running collect;
the workload Node does not need SSH access to any other Node.
"""

import argparse
import hashlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_time(value):
    # Go RFC3339Nano varies fractional precision; Python 3.9 accepts only
    # three or six digits. Normalize explicitly, retaining microseconds and
    # the offset. Never replace an invalid sample time with the current time.
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})", value)
    if not match:
        raise ValueError("invalid RFC3339 sample time")
    base, fraction, offset = match.groups()
    if fraction:
        base += "." + fraction[:6].ljust(6, "0")
    return datetime.fromisoformat(base + ("+00:00" if offset == "Z" else offset))


def read_info(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def geo_wait_health(snapshot, baseline):
    """Return Geo debt only after rejecting every other observed fault."""
    cluster = snapshot.get("cluster", {})
    peers = baseline.get("root", {}).get("peers", [])
    current = snapshot.get("root", {}).get("peers", [])
    identity = lambda p: (p.get("node_id"), p.get("raft_id"), p.get("address"), p.get("region_id"))
    if not peers or sorted(map(identity, peers)) != sorted(map(identity, current)):
        raise ValueError("membership changed during Geo wait")
    if not cluster.get("root_quorum_confirmed") or cluster.get("state") not in ("available", "degraded"):
        raise ValueError("Root or cluster unavailable during Geo wait")
    local = snapshot.get("local_repair", {})
    if (snapshot.get("local_repair_error") or snapshot.get("local_storage_error") or
            local.get("total_issues") or local.get("repair_debt") or local.get("volume_write_errors") or
            (local.get("volume_migration") or {}).get("active")):
        raise ValueError("local storage or repair fault during Geo wait")
    observations = cluster.get("repair_observations", [])
    ids = [p.get("node_id") for p in observations]
    if len(ids) != len(set(ids)) or set(ids) != {p["node_id"] for p in peers}:
        raise ValueError("incomplete peer health during Geo wait")
    debts = {}
    for node in observations:
        if (not node.get("reachable") or not node.get("worker_enabled") or not node.get("last_check_unix") or
                node.get("repair_debt") or node.get("write_quarantines") or node.get("volume_write_errors") or
                node.get("error") or node.get("issues")):
            raise ValueError("peer fault during Geo wait")
        for volume in node.get("volumes", []):
            if volume.get("state") != "removed" and (volume.get("state") != "active" or volume.get("health") != "healthy"):
                raise ValueError("Volume fault during Geo wait")
        debt = node.get("geo_debt", 0)
        if type(debt) is not int or debt < 0:
            raise ValueError("invalid Geo debt")
        debts[node["node_id"]] = debt
    total = sum(debts.values())
    pending = total
    for field in ("geo_record_debt", "geo_metadata_debt"):
        value = local.get(field, 0)
        if type(value) is not int or value < 0:
            raise ValueError("invalid local Geo debt")
        pending = max(pending, value)
    geo_evidence = pending > 0
    for reason in cluster.get("reasons", []):
        match = re.fullmatch(r"Geo repair has ([1-9][0-9]*) pending item\(s\)", reason)
        if match:
            pending = max(pending, int(match[1]))
            geo_evidence = True
            continue
        match = re.fullmatch(r"Node (\S+) has ([1-9][0-9]*) repair/maintenance debt item\(s\)", reason)
        if not match or match[1] not in debts:
            raise ValueError("non-Geo degradation during Geo wait")
        pending = max(pending, int(match[2]))
    # CLI summaries and authenticated peer observations are sampled at different
    # times. A Geo-only mismatch is pending, never proof of completed repair.
    # Every hard fault above still fails immediately; only an entirely clean
    # available observation can end the bounded wait.
    if cluster["state"] == "degraded" and (not geo_evidence or not pending or not cluster.get("reasons")):
        raise ValueError("unexplained cluster degradation")
    if cluster["state"] == "available" and (total or cluster.get("reasons") or
            local.get("geo_record_debt") or local.get("geo_metadata_debt")):
        raise ValueError("inconsistent available health")
    return pending


def settle_geo(snapshot_path, binary, store, seconds):
    """The caller pauses writes synchronously; only bounded status reads run."""
    snapshot_path = Path(snapshot_path)
    if not 0 < seconds <= 300:
        raise ValueError("Geo settle bound must be 1..300 seconds")
    directory = snapshot_path.parent
    baseline = json.loads((directory / "baseline.json").read_text())
    original = json.loads(snapshot_path.read_text())
    if geo_wait_health(original, baseline) == 0:
        raise ValueError("failed guard was not a Geo-only backlog")
    prefix = "geo-settle-" + snapshot_path.stem
    result_path = directory / (prefix + ".json")
    started = time.monotonic()
    result = {"outcome": "waiting", "snapshot": snapshot_path.name,
              "snapshot_sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
              "limit_seconds": seconds, "started_utc": utc_now(), "samples": []}
    with result_path.open("x") as f:
        json.dump(result, f)
    try:
        while True:
            remaining = seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("Geo debt did not clear within bounded wait")
            time.sleep(min(5, remaining))
            remaining = seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("Geo debt did not clear within bounded wait")
            p = subprocess.run([binary, "status", "--store", store], capture_output=True, text=True,
                               timeout=min(20, remaining))
            name = prefix + "-sample-" + str(len(result["samples"])) + ".json"
            (directory / name).write_text(p.stdout)
            (directory / (name + ".err")).write_text(p.stderr)
            result["samples"].append(name)
            if p.returncode:
                raise RuntimeError("status request failed during Geo wait: " + p.stderr[-1000:])
            debt = geo_wait_health(json.loads(p.stdout), baseline)
            if time.monotonic() - started > seconds:
                raise TimeoutError("Geo wait exceeded its deadline")
            if debt == 0:
                result.update(outcome="settled", recovered_snapshot=name,
                              recovered_sha256=hashlib.sha256((directory / name).read_bytes()).hexdigest())
                return result
    except Exception as error:
        result.update(outcome="failed", error=str(error))
        raise
    finally:
        result.update(elapsed_seconds=time.monotonic() - started, finished_utc=utc_now())
        result_path.write_text(json.dumps(result, indent=2))


def verified_geo_settlements(directory):
    recovered = {}
    for p in directory.glob("geo-settle-*.json"):
        if "-sample-" in p.name:
            continue
        result = json.loads(p.read_text())
        if result.get("outcome") != "settled":
            continue
        for field, digest in (("snapshot", "snapshot_sha256"), ("recovered_snapshot", "recovered_sha256")):
            name = result[field]
            if Path(name).name != name or hashlib.sha256((directory / name).read_bytes()).hexdigest() != result[digest]:
                raise ValueError("Geo settlement evidence changed")
        if not 0 <= result["elapsed_seconds"] <= result["limit_seconds"] <= 300:
            raise ValueError("invalid Geo wait duration")
        baseline = json.loads((directory / "baseline.json").read_text())
        if not result.get("samples") or result["samples"][-1] != result["recovered_snapshot"]:
            raise ValueError("incomplete Geo wait sample history")
        for name in result["samples"]:
            if Path(name).name != name:
                raise ValueError("invalid Geo wait sample path")
            geo_wait_health(json.loads((directory / name).read_text()), baseline)
        if geo_wait_health(json.loads((directory / result["snapshot"]).read_text()), baseline) <= 0:
            raise ValueError("Geo wait did not begin with a Geo-only backlog")
        if geo_wait_health(json.loads((directory / result["recovered_snapshot"]).read_text()), baseline) != 0:
            raise ValueError("Geo wait did not end healthy")
        recovered[result["snapshot"].replace(".json", ".check.err")] = result
    return recovered


def latencies(rows):
    ok = [int(r[4]) for r in rows if r[5] == "ok"]
    ordered = sorted(ok)
    percentile = lambda p: ordered[max(0, math.ceil(len(ordered) * p) - 1)] if ordered else None
    return {"total": len(rows), "succeeded": len(ok), "failed": len(rows) - len(ok),
            "success_rate": len(ok) / len(rows) if rows else None,
            "success_latency_ms": {"mean": round(sum(ok) / len(ok), 2) if ok else None,
                                   "p50": percentile(.5), "p95": percentile(.95),
                                   "p99": percentile(.99), "max": max(ok) if ok else None}}


def summarize(run_dir):
    run_dir = Path(run_dir)
    info = read_info(run_dir / "run.info")
    rows, malformed = [], []
    for number, line in enumerate((run_dir / "metrics.tsv").read_text().splitlines()[1:], 2):
        row = line.split("\t")
        if len(row) != 6 or not row[4].isdigit():
            malformed.append(number)
            continue
        rows.append(row)
    operations = {event: latencies([r for r in rows if r[1] == event]) for event in ("put", "get")}
    targets = {target: latencies([r for r in rows if r[1] == "get" and r[3] == target])
               for target in sorted({r[3] for r in rows if r[1] == "get"})}
    checks = [r for r in rows if r[1] == "status"]
    snapshots, invalid, issues, guards = [], [], [], []
    try:
        recovered_geo = verified_geo_settlements(run_dir)
    except Exception as error:
        recovered_geo = {}
        guards.append({"file": "geo-settlement", "error": str(error)})
    for path in sorted(run_dir.glob("status-*.json")):
        try:
            snapshot = json.loads(path.read_text())
        except (ValueError, OSError) as err:
            invalid.append({"file": path.name, "error": str(err)})
            continue
        snapshots.append((path.name, snapshot))
        for node in snapshot.get("cluster", {}).get("repair_observations", []):
            if (node.get("repair_debt") or node.get("write_quarantines") or
                    not node.get("reachable") or node.get("error")):
                issues.append({"snapshot": path.name, **node})
    for path in sorted(run_dir.glob("*.check.err")):
        error = path.read_text().strip()
        if error and path.name not in recovered_geo:
            guards.append({"file": path.name, "error": error})
    baseline_guard = run_dir / "baseline-check.err"
    if baseline_guard.exists() and baseline_guard.read_text().strip():
        guards.append({"file": baseline_guard.name, "error": baseline_guard.read_text().strip()})
    finished = info.get("finished_utc")
    elapsed = (parse_time(finished or utc_now()) - parse_time(info["started_utc"])).total_seconds()
    failures = sum(x["failed"] for x in operations.values())
    if not finished:
        outcome = "running"
    elif int(info.get("exit_code", 1)) != 0 or guards or failures or malformed or invalid:
        outcome = "failed"
    elif info.get("termination_reason") == "iteration_limit" or elapsed < int(info["duration_seconds"]):
        outcome = "finished_early"
    else:
        outcome = "completed"
    report = {"generated_utc": utc_now(), "outcome": outcome, "run": info,
              "elapsed_seconds": round(elapsed, 1), "operations": operations,
              "read_targets": targets, "status_checks": len(checks),
              "observed_cluster_states": dict(Counter(r[5].split(":", 1)[0] for r in checks)),
              "guard_failures": guards, "peer_issue_observations": issues,
              "recovered_geo_waits": list(recovered_geo.values()),
              "malformed_metric_lines": malformed, "invalid_snapshots": invalid,
              "latest_status_file": snapshots[-1][0] if snapshots else None}
    start_hash = run_dir / "binary-start-sha256.txt"
    finish_hash = run_dir / "binary-finish-sha256.txt"
    if start_hash.exists() and finish_hash.exists():
        before = start_hash.read_text().split()
        after = finish_hash.read_text().split()
        if before and after:
            report["workload_binary_sha256_start"] = before[0]
            report["workload_binary_sha256_finish"] = after[0]
            report["workload_binary_changed"] = before[0] != after[0]
            if report["workload_binary_changed"]:
                outcome = report["outcome"] = "failed"
    (run_dir / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    lines = [f"Soak outcome: {outcome}", f"Elapsed: {elapsed:.0f} seconds",
             "", "| Operation / target | Success | Failure | Mean ms | P95 ms | Max ms |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for label, data in [*operations.items(), *[(f"get {k}", v) for k, v in targets.items()]]:
        timing = data["success_latency_ms"]
        lines.append(f"| {label} | {data['succeeded']} | {data['failed']} | {timing['mean']} | {timing['p95']} | {timing['max']} |")
    lines += ["", f"Status checks: {len(checks)}; states: {report['observed_cluster_states']}",
              f"Health guard failures: {len(guards)}", "Latency statistics include successful requests only."]
    for guard in guards:
        lines.append(f"- {guard['file']}: {guard['error']}")
    for issue in issues:
        lines.append(f"- {issue['snapshot']}: {issue.get('name') or issue.get('node_id')}; "
                     f"repair debt={issue.get('repair_debt', 0)}, write quarantine={issue.get('write_quarantines', 0)}")
        for detail in issue.get("issues", []):
            lines.append(f"  - {detail.get('group_id')}: {detail.get('error')}")
        if issue.get("issues_truncated"):
            lines.append("  - Issue details are truncated; repair_debt is the full Group/task count.")
        if issue.get("repair_debt") and not issue.get("issues"):
            lines.append("  - This CLI did not capture peer issue details; consult the peer journal.")
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n")
    return report


# Only allowlisted diagnostics are copied. Repositories, credentials, Pack data,
# and Node identity files are never read by this collector.
REMOTE_RUN_FILES = r'''
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
patterns = ("run.info", "metrics.tsv", "failures.log", "failure-*", "baseline*", "status-*", "incident-*", "binary-*", "summary.*", "geo-settle-*")
files, errors, total = [], [], 0
for f in sorted(set(f for pattern in patterns for f in p.glob(pattern))):
    if not f.is_file() or f.is_symlink(): continue
    try:
        size = f.stat().st_size
        if size > 2 * 1024 * 1024 or total + size > 64 * 1024 * 1024:
            errors.append(f.name + ": diagnostic size limit exceeded"); continue
        total += size
        files.append({"name": f.name, "text": f.read_text(errors="replace")})
    except OSError as err:
        errors.append(f.name + ": " + str(err))
print(json.dumps({"files": files, "errors": errors}))
'''

REMOTE_NODE = r'''
import datetime, hashlib, json, pathlib, subprocess, sys
def run(args, timeout=15):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return {"exit_code": p.returncode, "stdout": p.stdout[-1024*1024:], "stderr": p.stderr[-16384:],
                "stdout_truncated": len(p.stdout) > 1024*1024, "stderr_truncated": len(p.stderr) > 16384}
    except Exception as e:
        return {"error": str(e)}
result = {"collected_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "journal_line_limit": 1500}
result["systemd"] = run(["systemctl", "status", "dubi", "--no-pager"])
result["journal"] = run(["journalctl", "-u", "dubi", "--since", sys.argv[1], "--until", sys.argv[2],
                         "--no-pager", "-o", "short-iso", "-n", "1500"])
try:
    pid = subprocess.check_output(["systemctl", "show", "dubi", "--property=MainPID", "--value"], text=True, timeout=10).strip()
    if not pid.isdigit() or int(pid) <= 0: raise ValueError("dubi has no running MainPID")
    proc = pathlib.Path("/proc") / pid
    executable = proc / "exe"
    result["pid"] = int(pid)
    result["executable"] = str(executable.readlink())
    with executable.open("rb") as stream:
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(1024*1024), b""): digest.update(block)
    result["running_binary_sha256"] = digest.hexdigest()
    args = (proc / "cmdline").read_bytes().decode().split("\0")
    store = "/var/lib/dubi"
    for i, arg in enumerate(args):
        if arg == "--store" and i + 1 < len(args): store = args[i+1]
        elif arg.startswith("--store="): store = arg.split("=", 1)[1]
    result["store"] = store
    result["version"] = run([str(executable), "version"])
    result["status"] = run([str(executable), "status", "--store", store], timeout=60)
except Exception as e:
    result["process_error"] = str(e)
print(json.dumps(result))
'''


def ssh_json(host, key, script, *args):
    # Validate destinations before handing them to OpenSSH; a Root member is
    # data, not an SSH option or a shell command.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:\-]*", host):
        raise ValueError("invalid SSH host")
    command = ["ssh", "-i", str(key), "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
               "-o", "StrictHostKeyChecking=yes", f"root@{host}",
               "python3 - " + " ".join(shlex.quote(str(x)) for x in args)]
    result = subprocess.run(command, input=script, capture_output=True, text=True, timeout=110)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"SSH exit {result.returncode}")
    return json.loads(result.stdout)


def peer_host(address):
    if address.startswith("["):
        host, rest = address[1:].split("]", 1)
        if not rest.startswith(":") or not rest[1:].isdigit():
            raise ValueError("invalid peer port")
        return host
    host, port = address.rsplit(":", 1)
    if not port.isdigit():
        raise ValueError("invalid peer port")
    return host


def wait_for_completion(host, key, remote_dir):
    """Wait for this one run, without creating any recurring scheduled task."""
    deadline = time.monotonic() + 7 * 60 * 60
    script = 'import pathlib, json, sys; p = pathlib.Path(sys.argv[1], "run.info"); print(json.dumps(p.read_text() if p.exists() else ""))'
    while time.monotonic() < deadline:
        try:
            info = ssh_json(host, key, script, remote_dir)
            if "finished_utc=" in info:
                return
        except Exception as err:
            print(f"Waiting for run metadata: {err}", file=sys.stderr, flush=True)
        time.sleep(30)
    raise TimeoutError("run did not finish within the seven-hour collector wait limit")


def collect(host, key, remote_dir, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    evidence = output / "run"
    evidence.mkdir()
    bundle = ssh_json(host, key, REMOTE_RUN_FILES, remote_dir)
    for item in bundle["files"]:
        name = item["name"]
        if Path(name).name != name or name in (".", ".."):
            raise ValueError("invalid remote evidence filename")
        (evidence / name).write_text(item["text"])
    report = summarize(evidence)
    # Prefer the exact failed snapshot when selecting members. Later status
    # calls are supplementary and never replace the triggering observation.
    candidates = [evidence / x["file"].replace(".check.err", ".json")
                  for x in report["guard_failures"] if x["file"].startswith("status-")]
    candidates += sorted(evidence.glob("status-*.json"), reverse=True) + [evidence / "baseline.json"]
    snapshot = None
    for path in candidates:
        try:
            snapshot = json.loads(path.read_text())
            if snapshot.get("root", {}).get("peers"):
                break
        except (OSError, ValueError):
            continue
    if not snapshot or not snapshot.get("root", {}).get("peers"):
        raise ValueError("no Root peer list in captured evidence")
    info = report["run"]
    end = parse_time(info.get("finished_utc") or utc_now())
    since = (end - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S UTC")
    until = (end + timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S UTC")
    peers_dir = output / "peers"
    peers_dir.mkdir()

    def capture(item):
        index, node = item
        label = re.sub(r"[^A-Za-z0-9_.-]", "_", node.get("name") or node.get("node_id", "unknown"))[:100]
        try:
            diagnostics = ssh_json(peer_host(node["address"]), key, REMOTE_NODE, since, until)
            result = {"membership": node, **diagnostics}
        except Exception as err:
            result = {"membership": node, "collection_error": str(err)}
        (peers_dir / f"{index:02d}-{label}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        return {"node_id": node.get("node_id"), "name": node.get("name"),
                "running_binary_sha256": result.get("running_binary_sha256"),
                "collection_error": result.get("collection_error") or result.get("process_error")}

    with ThreadPoolExecutor(max_workers=3) as pool:
        nodes = list(pool.map(capture, enumerate(snapshot["root"]["peers"])))
    manifest = {"collected_utc": utc_now(), "source_host": host, "remote_run_dir": remote_dir,
                "journal_since": since, "journal_until": until, "copy_errors": bundle["errors"],
                "nodes": nodes, "version_note": "Peer binary hashes and status are collection-time observations; triggering status files are preserved separately."}
    (output / "collection.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    report = commands.add_parser("report", help="write summary.json and summary.md")
    report.add_argument("run_dir")
    capture = commands.add_parser("collect", help="copy run evidence and collect all peer diagnostics read-only")
    capture.add_argument("--host", required=True, help="workload Node SSH host")
    capture.add_argument("--ssh-key", required=True, type=Path)
    capture.add_argument("--run-dir", required=True, help="run directory on workload Node")
    capture.add_argument("--output", default=None, help="new local directory; existing directories are refused")
    capture.add_argument("--wait", action="store_true", help="wait for this run to finish, then collect; requires this machine to stay awake")
    settle = commands.add_parser("settle-geo", help="pause writes while a Geo-only backlog clears")
    settle.add_argument("snapshot")
    settle.add_argument("--binary", required=True)
    settle.add_argument("--store", required=True)
    settle.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args()
    if args.command == "settle-geo":
        print(json.dumps(settle_geo(args.snapshot, args.binary, args.store, args.seconds)))
    elif args.command == "report":
        result = summarize(args.run_dir)
        print(json.dumps({"outcome": result["outcome"], "elapsed_seconds": result["elapsed_seconds"], "operations": result["operations"]}))
    else:
        if args.wait:
            print("Waiting for this soak run to finish before collecting peer diagnostics.", file=sys.stderr, flush=True)
            wait_for_completion(args.host, args.ssh_key.expanduser(), args.run_dir)
        output = args.output or str(Path(tempfile.gettempdir()) / ("dubi-soak-evidence-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")))
        result = collect(args.host, args.ssh_key.expanduser(), args.run_dir, output)
        print(json.dumps({"output": output, "nodes": result["nodes"], "copy_errors": result["copy_errors"]}))
        if result["copy_errors"] or any(node["collection_error"] for node in result["nodes"]):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
