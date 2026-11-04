#!/usr/bin/env python3
"""Offline diagnostic regressions; no DUBI services or data stores are touched."""

from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("diagnostics", HERE / "vps_soak_diagnostics.py")
diagnostics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostics)

STATUS = {"cluster": {"state": "available", "root_quorum_confirmed": True,
                      "regions": [{"region": "sg", "nodes": 3}, {"region": "ff", "nodes": 3}],
                      "repair_observations": [{"node_id": "node_c", "name": "sg03", "reachable": True,
                                               "worker_enabled": True, "last_check_unix": 123}]},
          "root": {"leader_id": 1, "peers": [{"node_id": "node_c", "name": "sg03", "address": "127.0.0.1:12443"}]}}
BAD_STATUS = deepcopy(STATUS)
BAD_STATUS["cluster"]["state"] = "degraded"
BAD_STATUS["cluster"]["reasons"] = ["Node node_c has 1 repair/maintenance debt item(s)"]
BAD_STATUS["cluster"]["repair_observations"][0].update(
    repair_debt=1, issues=[{"group_id": "region-sg", "error": "local Pack write failed"}])

FAKE_BINARY = r'''#!/usr/bin/env python3
import base64, json, os, pathlib, sys
p = pathlib.Path(os.environ["SOAK_FIXTURE"])
args = sys.argv[1:]
with (p / "commands.jsonl").open("a") as f: f.write(json.dumps(args) + "\n")
if args[0] == "version": print("dubi fixture"); sys.exit(0)
if args[0] == "status":
    count = p / "status_calls"
    calls = int(count.read_text()) + 1 if count.exists() else 1
    count.write_text(str(calls))
    mode = os.environ.get("SOAK_FIXTURE_MODE", "ok")
    bad = mode == "baseline-debt" or (mode == "debt" and calls > 1)
    if mode in ("geo-once", "geo-stuck", "geo-hard") and calls == 2: print((p / "geo.json").read_text())
    elif mode == "geo-stuck" and calls > 2: print((p / "geo.json").read_text())
    elif mode == "geo-hard" and calls > 2: print((p / "bad.json").read_text())
    else: print((p / ("bad.json" if bad else "good.json")).read_text())
elif args[0:2] == ["record", "put"]:
    if os.environ.get("SOAK_FIXTURE_MODE") == "replace-binary":
        with pathlib.Path(sys.argv[0]).open("a") as stream: stream.write("\n# replaced binary\n")
    (p / "value").write_text(args[-1]); print("{}")
elif args[0:2] == ["record", "get"]:
    marker = p / "failed_once"
    if os.environ.get("SOAK_FIXTURE_MODE") == "mismatch" and not marker.exists():
        marker.touch(); print(json.dumps({"found": False}))
    else:
        print(json.dumps({"found": True, "value": base64.b64encode((p / "value").read_text().encode()).decode()}))
else: sys.exit(2)
'''


def geo_fixture():
    value = deepcopy(STATUS)
    value["cluster"].update(state="degraded", reasons=["Geo repair has 1 pending item(s)",
                           "Node node_c has 1 repair/maintenance debt item(s)"])
    value["cluster"]["repair_observations"][0]["geo_debt"] = 1
    return value


class GeoWaitTests(unittest.TestCase):
    def test_only_geo_backlog_may_wait(self):
        self.assertEqual(diagnostics.geo_wait_health(geo_fixture(), STATUS), 1)
        self.assertEqual(diagnostics.geo_wait_health(STATUS, STATUS), 0)
        for kind in ("root", "unreachable", "repair", "volume", "quarantine", "membership", "incomplete", "reason", "error"):
            value = geo_fixture()
            cluster = value["cluster"]; node = cluster["repair_observations"][0]
            if kind == "root": cluster["root_quorum_confirmed"] = False
            if kind == "unreachable": node["reachable"] = False
            if kind == "repair": node["repair_debt"] = 1
            if kind == "volume": node["volumes"] = [{"state": "active", "health": "missing"}]
            if kind == "quarantine": node["write_quarantines"] = 1
            if kind == "membership": value["root"]["peers"][0]["address"] = "foreign:123"
            if kind == "incomplete": cluster["repair_observations"] = []
            if kind == "reason": cluster["reasons"].append("some unknown failure")
            if kind == "error": node["error"] = "RPC failed"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                diagnostics.geo_wait_health(value, STATUS)

    def test_geo_sampling_skew_stays_pending_until_fully_clean(self):
        # Real failure: summary was sampled before the peer's pending task
        # cleared. Neither view can turn this into completed repair alone.
        sample = geo_fixture()
        sample["cluster"]["repair_observations"][0].pop("geo_debt")
        sample["cluster"]["reasons"] = ["Geo repair has 1 pending item(s)"]
        self.assertEqual(diagnostics.geo_wait_health(sample, STATUS), 1)
        sample["cluster"]["reasons"].append("Node node_c has 2 repair/maintenance debt item(s)")
        self.assertEqual(diagnostics.geo_wait_health(sample, STATUS), 2)
        sample["cluster"]["repair_observations"][0]["error"] = "transport failed"
        with self.assertRaises(ValueError): diagnostics.geo_wait_health(sample, STATUS)
        self.assertEqual(diagnostics.geo_wait_health(STATUS, STATUS), 0)

    def test_recovery_evidence_is_required_and_bound_to_original_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp); snapshot = p / "status-trigger.json"
            (p / "baseline.json").write_text(json.dumps(STATUS)); snapshot.write_text(json.dumps(geo_fixture()))
            clock = [0.0]
            def sleep(seconds): clock[0] += seconds
            skewed = geo_fixture()
            skewed["cluster"]["repair_observations"][0].pop("geo_debt")
            responses = [subprocess.CompletedProcess([], 0, json.dumps(skewed), ""),
                         subprocess.CompletedProcess([], 0, json.dumps(STATUS), "")]
            with patch.object(diagnostics.time, "monotonic", side_effect=lambda: clock[0]), patch.object(diagnostics.time, "sleep", side_effect=sleep), patch.object(diagnostics.subprocess, "run", side_effect=responses):
                result = diagnostics.settle_geo(snapshot, "/fixture", "/store", 12)
            self.assertEqual(result["outcome"], "settled")
            self.assertEqual(result["elapsed_seconds"], 10)
            self.assertIn("status-trigger.check.err", diagnostics.verified_geo_settlements(p))
            snapshot.write_text(json.dumps(STATUS))
            with self.assertRaises(ValueError): diagnostics.verified_geo_settlements(p)

    def test_stuck_backlog_and_read_error_fail_without_erasing_evidence(self):
        for mode in ("stuck", "rpc"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                p = Path(tmp); snapshot = p / "status-trigger.json"
                (p / "baseline.json").write_text(json.dumps(STATUS)); snapshot.write_text(json.dumps(geo_fixture()))
                clock = [0.0]
                def sleep(seconds): clock[0] += seconds
                response = subprocess.CompletedProcess([], 1 if mode == "rpc" else 0, json.dumps(geo_fixture()), "RPC failed" if mode == "rpc" else "")
                with patch.object(diagnostics.time, "monotonic", side_effect=lambda: clock[0]), patch.object(diagnostics.time, "sleep", side_effect=sleep), patch.object(diagnostics.subprocess, "run", return_value=response), self.assertRaises((TimeoutError, RuntimeError)):
                    diagnostics.settle_geo(snapshot, "/fixture", "/store", 12)
                self.assertEqual(json.loads(snapshot.read_text()), geo_fixture())
                self.assertEqual(diagnostics.verified_geo_settlements(p), {})


class DiagnosticTests(unittest.TestCase):
    def test_rfc3339_nano_normalizes_for_older_python(self):
        for digits in range(1, 10):
            fraction = "889159440"[:digits]
            expected = "2026-11-03T17:19:11." + fraction[:6].ljust(6, "0") + "+00:00"
            # Assert the input sent to the native parser, even on a newer VPS
            # Python which would otherwise hide the Python 3.9 regression.
            with patch.object(diagnostics, "datetime", wraps=diagnostics.datetime) as parser:
                actual = diagnostics.parse_time("2026-11-03T17:19:11." + fraction + "Z")
                parser.fromisoformat.assert_called_once_with(expected)
                self.assertEqual(actual.isoformat(), expected)

    def test_rfc3339_offsets_and_whole_seconds(self):
        self.assertEqual(diagnostics.parse_time("2026-11-03T17:19:11Z"),
                         diagnostics.parse_time("2026-11-03T09:19:11-08:00"))
        self.assertEqual(diagnostics.parse_time("2026-11-03T17:19:11.88915944Z").microsecond, 889159)

    def test_invalid_sample_time_is_not_silently_replaced(self):
        for value in ("", "2026-11-03", "2026-11-03T17:19:11", "2026-11-03T17:19:11.Z",
                      "2026-11-03T17:19:11.1234567890Z", "2026-11-03T25:19:11Z"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                diagnostics.parse_time(value)

    def test_latency_percentiles_exclude_failed_requests(self):
        rows = [["time", "put", "1", "local", str(ms), result]
                for ms, result in ((100, "ok"), (300, "ok"), (200, "ok"), (90000, "fail"))]
        result = diagnostics.latencies(rows)
        self.assertEqual(result["success_rate"], .75)
        self.assertEqual(result["success_latency_ms"], {"mean": 200.0, "p50": 200, "p95": 300, "p99": 300, "max": 300})

    def test_summary_counts_health_states_separately_and_preserves_original_issue(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p / "run.info").write_text("started_utc=2026-10-27T00:00:00Z\nfinished_utc=2026-10-27T06:00:00Z\nduration_seconds=21600\nexit_code=1\n")
            (p / "metrics.tsv").write_text("utc\tevent\titeration\ttarget\telapsed_ms\tresult\n"
                                           "time\tput\t1\tlocal\t900\tok\n"
                                           "time\tget\t1\tremote\t2000\tok\n"
                                           "time\tstatus\t0\tcluster\t1500\tdegraded:1\n")
            (p / "status-first.json").write_text(json.dumps(BAD_STATUS))
            (p / "status-first.check.err").write_text("Node sg03 repair debt=1\n")
            (p / "status-last.json").write_text(json.dumps(STATUS))
            result = diagnostics.summarize(p)
            self.assertEqual(result["operations"]["put"]["failed"], 0)
            self.assertEqual(result["operations"]["get"]["failed"], 0)
            self.assertEqual(result["observed_cluster_states"], {"degraded": 1})
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual(result["peer_issue_observations"][0]["issues"][0]["error"], "local Pack write failed")
            self.assertEqual(result["read_targets"]["remote"]["success_latency_ms"]["p95"], 2000)
            self.assertIn("region-sg", (p / "summary.md").read_text())

    def test_collector_keeps_failed_snapshot_journal_window_and_ssh_errors(self):
        info = "started_utc=2026-10-27T00:00:00Z\nfinished_utc=2026-10-27T00:16:00Z\nduration_seconds=21600\nexit_code=1\n"
        files = {"run.info": info, "metrics.tsv": "utc\tevent\titeration\ttarget\telapsed_ms\tresult\n",
                 "status-first.json": json.dumps(BAD_STATUS), "status-first.check.err": "repair debt=1",
                 "status-last.json": json.dumps(STATUS), "failure-get-1.json": '{"found":false}'}

        def fake_ssh(host, key, script, *args):
            if script == diagnostics.REMOTE_RUN_FILES:
                return {"files": [{"name": name, "text": content} for name, content in files.items()], "errors": []}
            self.assertEqual(host, "127.0.0.1")
            self.assertEqual(args[0], "2026-10-27 00:06:00 UTC")
            self.assertEqual(args[1], "2026-10-27 00:16:30 UTC")
            raise RuntimeError("fixture SSH unavailable")

        with tempfile.TemporaryDirectory() as tmp, patch.object(diagnostics, "ssh_json", side_effect=fake_ssh):
            output = Path(tmp) / "bundle"
            manifest = diagnostics.collect("source", Path("unused-key"), "/remote/run", output)
            self.assertIn("SSH unavailable", manifest["nodes"][0]["collection_error"])
            self.assertEqual((output / "run" / "status-first.json").read_text(), json.dumps(BAD_STATUS))
            self.assertTrue((output / "run" / "failure-get-1.json").exists())
            with self.assertRaises(FileExistsError):
                diagnostics.collect("source", Path("unused-key"), "/remote/run", output)

    def test_wait_is_for_one_finite_run(self):
        with patch.object(diagnostics, "ssh_json", side_effect=["started_utc=now\n", "started_utc=now\nfinished_utc=later\n"]), \
                patch.object(diagnostics.time, "sleep") as sleep:
            diagnostics.wait_for_completion("source", "key", "/remote/run")
            sleep.assert_called_once_with(30)

    def test_ssh_host_cannot_be_an_option_or_shell_command(self):
        for host in ("-oProxyCommand=bad", "host;touch /tmp/bad", "host\ncommand"):
            with self.assertRaises(ValueError):
                diagnostics.ssh_json(host, "key", "")


class WorkloadExitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.p = Path(self.tmp.name)
        (self.p / "good.json").write_text(json.dumps(STATUS))
        (self.p / "geo.json").write_text(json.dumps(geo_fixture()))
        (self.p / "bad.json").write_text(json.dumps(BAD_STATUS))
        self.binary = self.p / "dubi"
        self.binary.write_text(FAKE_BINARY)
        self.binary.chmod(0o755)
        # No real systemd/journal probes during these offline fixtures.
        for name in ("systemctl", "journalctl"):
            tool = self.p / name
            tool.write_text("#!/bin/sh\nprintf 'fixture diagnostic\\n'\n")
            tool.chmod(0o755)

    def environment(self, mode="ok", iterations="2"):
        return {**os.environ, "SOAK_FIXTURE": str(self.p), "SOAK_FIXTURE_MODE": mode,
                "PATH": str(self.p) + os.pathsep + os.environ["PATH"],
                "DUBI_BINARY": str(self.binary), "DUBI_STORE": str(self.p),
                "DUBI_SOAK_DIAGNOSTICS": str(HERE / "vps_soak_diagnostics.py"),
                "DUBI_SOAK_DIR": str(self.p / "run"), "DUBI_SOAK_DURATION_SECONDS": "30",
                "DUBI_SOAK_INTERVAL_SECONDS": "1", "DUBI_SOAK_STATUS_INTERVAL_SECONDS": "1",
                "DUBI_SOAK_MAX_ITERATIONS": iterations, "DUBI_SOAK_ALLOW_GEO_DEGRADED": "1"}

    def run_workload(self, mode="ok", iterations="2"):
        return subprocess.run(["bash", str(HERE / "vps_cross_region_soak.sh")],
                              env=self.environment(mode, iterations), capture_output=True, text=True, timeout=25)

    def report(self):
        return json.loads((self.p / "run" / "summary.json").read_text())

    def test_geo_wait_pauses_writes_and_preserves_failed_sample(self):
        env = self.environment("geo-once", "4")
        env.update(DUBI_SOAK_ALLOW_GEO_DEGRADED="0", DUBI_SOAK_GEO_SETTLE_SECONDS="12")
        result = subprocess.run(["bash", str(HERE / "vps_cross_region_soak.sh")], env=env, capture_output=True, text=True, timeout=25)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = self.report()
        self.assertEqual(report["outcome"], "finished_early")
        self.assertEqual(len(report["recovered_geo_waits"]), 1)
        self.assertFalse(report["guard_failures"])
        wait = report["recovered_geo_waits"][0]
        self.assertTrue((self.p / "run" / wait["snapshot"].replace(".json", ".check.err")).read_text().strip())
        commands = [json.loads(l) for l in (self.p / "commands.jsonl").read_text().splitlines()]
        checks = [i for i,c in enumerate(commands) if c[0] == "status"]
        self.assertFalse(any(c[:2] == ["record", "put"] for c in commands[checks[1]+1:checks[2]]))

    def test_geo_wait_stops_on_a_new_hard_fault(self):
        env = self.environment("geo-hard", "4")
        env.update(DUBI_SOAK_ALLOW_GEO_DEGRADED="0", DUBI_SOAK_GEO_SETTLE_SECONDS="12")
        result = subprocess.run(["bash", str(HERE / "vps_cross_region_soak.sh")], env=env, capture_output=True, text=True, timeout=25)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(self.report()["outcome"], "failed")
        waits = list((self.p / "run").glob("geo-settle-status-*.json"))
        self.assertTrue(any("peer fault" in p.read_text() for p in waits))

    def test_geo_wait_does_not_hide_stuck_debt(self):
        env = self.environment("geo-stuck", "4")
        env.update(DUBI_SOAK_ALLOW_GEO_DEGRADED="0", DUBI_SOAK_GEO_SETTLE_SECONDS="3")
        result = subprocess.run(["bash", str(HERE / "vps_cross_region_soak.sh")], env=env, capture_output=True, text=True, timeout=25)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(self.report()["outcome"], "failed")
        self.assertFalse(self.report()["recovered_geo_waits"])

    def test_successful_short_run_does_not_claim_full_duration_pass(self):
        result = self.run_workload(iterations="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report()["outcome"], "finished_early")
        self.assertEqual(self.report()["operations"]["get"]["succeeded"], 3)
        epoch = self.report()["run"]["key_epoch"]
        self.assertEqual((self.p / "value").read_text(), f"dubi-cross-region-soak:{epoch}:1")
        metrics = (self.p / "run" / "metrics.tsv").read_text()
        self.assertEqual(self.run_workload().returncode, 2)
        self.assertEqual((self.p / "run" / "metrics.tsv").read_text(), metrics)

    def test_guard_retains_exact_remote_issue_before_later_recovery(self):
        result = self.run_workload("debt", iterations="4")
        self.assertEqual(result.returncode, 1, result.stderr)
        report = self.report()
        self.assertEqual(report["outcome"], "failed")
        self.assertEqual(report["operations"]["put"]["failed"], 0)
        self.assertIn("local Pack write failed", report["guard_failures"][0]["error"])
        incidents = list((self.p / "run").glob("incident-*-status.json"))
        self.assertTrue(incidents)
        self.assertEqual(json.loads(incidents[-1].read_text())["cluster"]["repair_observations"][0]["issues"],
                         BAD_STATUS["cluster"]["repair_observations"][0]["issues"])

    def test_value_mismatch_is_saved_and_fails_run_even_after_later_success(self):
        result = self.run_workload("mismatch")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(self.report()["operations"]["get"]["failed"], 1)
        self.assertGreater(self.report()["operations"]["get"]["succeeded"], 0)
        error = next((self.p / "run").glob("failure-get-*.err")).read_text()
        self.assertIn("did not return the value", error)

    def test_baseline_failure_still_produces_summary_and_incident(self):
        result = self.run_workload("baseline-debt")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(self.report()["outcome"], "failed")
        self.assertEqual(self.report()["operations"]["put"]["total"], 0)
        self.assertTrue(list((self.p / "run").glob("incident-*-status.json")))

    def test_changed_workload_binary_cannot_pass(self):
        result = self.run_workload("replace-binary", iterations="1")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(self.report()["outcome"], "failed")
        self.assertTrue(self.report()["workload_binary_changed"])

    def test_zero_debt_without_completed_repair_check_cannot_pass(self):
        state = deepcopy(STATUS)
        state["cluster"]["repair_observations"][0]["last_check_unix"] = 0
        (self.p / "good.json").write_text(json.dumps(state))
        result = self.run_workload()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(self.report()["operations"]["put"]["total"], 0)
        self.assertIn("has not completed a check", self.report()["guard_failures"][0]["error"])

    def test_termination_finalizes_without_claiming_completion(self):
        process = subprocess.Popen(["bash", str(HERE / "vps_cross_region_soak.sh")],
                                   env=self.environment(iterations="0"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 10
            while not (self.p / "value").exists() and time.monotonic() < deadline:
                time.sleep(.05)
            self.assertTrue((self.p / "value").exists())
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=10), 143)
            self.assertEqual(self.report()["outcome"], "failed")
            self.assertEqual(self.report()["run"]["exit_code"], "143")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
