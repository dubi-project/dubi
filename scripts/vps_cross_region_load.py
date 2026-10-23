#!/usr/bin/env python3
"""Ramped, bounded cross-region read/write load test for the VPS lab."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import datetime as dt
import json
import os
import statistics
import subprocess
import threading
import time
import uuid
from pathlib import Path


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def run_command(command: list[str], timeout: int) -> tuple[int, str, str, int]:
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout, result.stderr, int((time.monotonic() - started) * 1000)
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode(errors="replace") if isinstance(error.stdout, bytes) else (error.stdout or "")
        stderr = error.stderr.decode(errors="replace") if isinstance(error.stderr, bytes) else (error.stderr or "")
        return 124, stdout, stderr or f"command timed out after {timeout}s", int((time.monotonic() - started) * 1000)


def memory_available_mb() -> int:
    with open("/proc/meminfo", encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    return -1


def load_average() -> str:
    with open("/proc/loadavg", encoding="utf-8") as stream:
        return stream.read().split()[0]


def percentile(values: list[int], percent: int) -> int:
    ordered = sorted(values)
    if not ordered:
        return 0
    index = max(0, min(len(ordered) - 1, (len(ordered) * percent + 99) // 100 - 1))
    return ordered[index]


def parse_stages(raw: str) -> list[tuple[int, int]]:
    stages = []
    for item in raw.split(","):
        concurrency, seconds = item.split(":", 1)
        stage = (int(concurrency), int(seconds))
        if stage[0] < 1 or stage[0] > 8 or stage[1] < 30:
            raise argparse.ArgumentTypeError("stages must use concurrency 1-8 and duration >=30s")
        stages.append(stage)
    if not stages:
        raise argparse.ArgumentTypeError("at least one load stage is required")
    return stages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="/usr/local/bin/dubi")
    parser.add_argument("--store", default="/var/lib/dubi-lab6-ready")
    parser.add_argument("--tenant", default="rootread")
    parser.add_argument("--dataset", default="smoke")
    parser.add_argument("--stages", default="1:120,2:180,4:180,8:180", type=parse_stages)
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--rpc-timeout", type=int, default=45)
    args = parser.parse_args()

    binary = args.binary
    if not os.path.isfile(binary) or not os.access(binary, os.X_OK) or not os.path.isdir(args.store):
        parser.error("dubi binary or local Node store is unavailable")
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    run_dir = Path(args.run_dir or f"/var/tmp/dubi-vps-load-{run_id}")
    run_dir.mkdir(parents=True, exist_ok=False)
    metrics_path = run_dir / "metrics.tsv"
    failures_path = run_dir / "failures.log"
    resources_path = run_dir / "resources.tsv"
    metrics_path.write_text("utc\tstage\tworker\toperation\ttarget\telapsed_ms\tresult\n", encoding="utf-8")
    resources_path.write_text("utc\tstage\tconcurrency\tmem_available_mb\tload1\n", encoding="utf-8")
    (run_dir / "run.info").write_text(
        f"started_utc={utc_now()}\nstore={args.store}\ntenant={args.tenant}\ndataset={args.dataset}\nstages={args.stages}\n",
        encoding="utf-8",
    )

    code, stdout, stderr, elapsed = run_command([binary, "status", "--store", args.store], 90)
    if code != 0:
        (run_dir / "baseline.err").write_text(stderr, encoding="utf-8")
        print(f"baseline status failed ({elapsed}ms): {stderr[:300]}")
        return 2
    baseline = json.loads(stdout)
    (run_dir / "baseline.json").write_text(json.dumps(baseline, indent=2) + "\n", encoding="utf-8")
    cluster = baseline.get("cluster", {})
    if cluster.get("state") != "available":
        print(f"refusing load: baseline cluster state is {cluster.get('state', 'unknown')}")
        return 2

    print(f"load test started: {run_dir}; baseline=available; stages={args.stages}", flush=True)
    rows: list[tuple[str, int, int, str, str, int, str]] = []
    rows_lock = threading.Lock()
    stop = threading.Event()
    consecutive_failures = 0
    failure_lock = threading.Lock()
    targets = (("local", None), ("ff01", "146.103.40.3:12443"), ("tt01", "209.200.246.159:12443"))

    def record(stage: int, worker: int, operation: str, target: str, ms: int, result: str) -> None:
        nonlocal consecutive_failures
        row = (utc_now(), stage, worker, operation, target, ms, result)
        with rows_lock:
            rows.append(row)
            with metrics_path.open("a", encoding="utf-8") as stream:
                stream.write("\t".join(map(str, row)) + "\n")
        with failure_lock:
            if result == "ok":
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                with failures_path.open("a", encoding="utf-8") as stream:
                    stream.write(f"{row[0]} stage={stage} worker={worker} {operation} {target} {ms}ms: {result}\n")
                # Stop adding pressure after the first rejected or incorrect
                # operation; let only already-started requests drain.
                stop.set()
                if consecutive_failures >= 5:
                    with failures_path.open("a", encoding="utf-8") as stream:
                        stream.write(f"{row[0]} stopping after 5 consecutive failures\n")

    def worker_loop(stage: int, worker: int, deadline: float) -> None:
        iteration = 0
        while time.monotonic() < deadline and not stop.is_set():
            iteration += 1
            key = f"load-{run_id}-{stage}-{worker}-{iteration}"
            value = f"dubi-load:{run_id}:{stage}:{worker}:{iteration}"
            request_id = f"load-{run_id}-{stage}-{worker}-{iteration}"
            put_cmd = [binary, "record", "put", "--store", args.store, "--request-id", request_id,
                       args.tenant, args.dataset, key, value]
            code, _, stderr, ms = run_command(put_cmd, args.rpc_timeout)
            record(stage, worker, "put", "local", ms, "ok" if code == 0 else stderr.strip()[:1000] or f"exit={code}")
            if code != 0 or stop.is_set():
                continue
            expected = base64.b64encode(value.encode()).decode()
            for name, server in targets:
                get_cmd = [binary, "record", "get", "--store", args.store]
                if server:
                    get_cmd += ["--server", server]
                get_cmd += [args.tenant, args.dataset, key]
                code, output, stderr, ms = run_command(get_cmd, args.rpc_timeout)
                result = "ok"
                if code == 0:
                    try:
                        response = json.loads(output)
                        if response.get("found") is not True or response.get("value") != expected:
                            result = "read-back mismatch"
                    except (json.JSONDecodeError, TypeError):
                        result = "invalid JSON response"
                else:
                    result = stderr.strip()[:1000] or f"exit={code}"
                record(stage, worker, "get", name, ms, result)
                if result != "ok" or stop.is_set():
                    break

    for stage_number, (concurrency, duration) in enumerate(args.stages, 1):
        if stop.is_set():
            break
        deadline = time.monotonic() + duration
        print(f"stage {stage_number}: concurrency={concurrency}, duration={duration}s", flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker_loop, stage_number, worker, deadline) for worker in range(1, concurrency + 1)]
            last_status = time.monotonic()
            while not all(future.done() for future in futures):
                now = time.monotonic()
                available_mb = memory_available_mb()
                resource_row = (utc_now(), stage_number, concurrency, available_mb, load_average())
                with resources_path.open("a", encoding="utf-8") as stream:
                    stream.write("\t".join(map(str, resource_row)) + "\n")
                if available_mb >= 0 and available_mb < 180:
                    with failures_path.open("a", encoding="utf-8") as stream:
                        stream.write(f"{resource_row[0]} stopping: MemAvailable fell below 180MB\n")
                    stop.set()
                if now - last_status >= 60:
                    code, status_out, status_err, status_ms = run_command([binary, "status", "--store", args.store], 45)
                    status_record = {"utc": utc_now(), "stage": stage_number, "elapsed_ms": status_ms,
                                     "result": "ok" if code == 0 else status_err[:300]}
                    if code == 0:
                        status = json.loads(status_out)
                        status_record["cluster"] = status.get("cluster", {}).get("state", "unknown")
                        status_record["leader_id"] = status.get("root", {}).get("leader_id", 0)
                        if status_record["cluster"] != "available":
                            stop.set()
                    with (run_dir / "status.jsonl").open("a", encoding="utf-8") as stream:
                        stream.write(json.dumps(status_record) + "\n")
                    last_status = now
                time.sleep(10)
            for future in futures:
                future.result()
        stage_rows = [row for row in rows if row[1] == stage_number]
        completed = sum(1 for row in stage_rows if row[6] == "ok")
        print(f"stage {stage_number} finished: operations={len(stage_rows)} successful={completed}", flush=True)
        if any(row[6] != "ok" for row in stage_rows):
            stop.set()

    summaries = []
    keys = sorted({(row[1], row[3], row[4]) for row in rows})
    for stage, operation, target in keys:
        values = [row[5] for row in rows if row[1] == stage and row[3] == operation and row[4] == target and row[6] == "ok"]
        failures = sum(1 for row in rows if row[1] == stage and row[3] == operation and row[4] == target and row[6] != "ok")
        summaries.append({"stage": stage, "operation": operation, "target": target, "successes": len(values),
                          "failures": failures, "avg_ms": round(statistics.mean(values)) if values else 0,
                          "p50_ms": percentile(values, 50), "p95_ms": percentile(values, 95),
                          "p99_ms": percentile(values, 99), "max_ms": max(values, default=0)})
    (run_dir / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n", encoding="utf-8")
    end = utc_now()
    exit_code = 1 if stop.is_set() or any(item["failures"] for item in summaries) else 0
    with (run_dir / "run.info").open("a", encoding="utf-8") as stream:
        stream.write(f"finished_utc={end}\nexit_code={exit_code}\noperations={len(rows)}\n")
    print(f"load test finished: exit={exit_code}; operations={len(rows)}; summary={run_dir / 'summary.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
