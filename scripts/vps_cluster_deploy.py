#!/usr/bin/env python3
"""Plan or execute a serial deployment on the developer VPS cluster."""
import argparse
import json
from pathlib import Path
import re
import time
import uuid

from vps_cluster_tools import Fleet, executable_sha256, no_workload, now, require_healthy, validate_plan


def deployment_order(inventory):
    leader = inventory["nodes"][0]["status"]["root"]["leader_id"]
    voters = set(inventory["nodes"][0]["status"]["root"]["voters"])
    if leader not in voters:
        raise RuntimeError("Root leader is not known in the confirmed voter set")
    return sorted(inventory["nodes"], key=lambda n: (n["identity"]["raft_id"] == leader,
                                                    n["identity"]["raft_id"] in voters, n["name"]))


def make_plan(fleet, host, binary):
    checksum = executable_sha256(binary)
    inventory = fleet.discover(host)
    return {"kind": "deployment", "created_utc": now(), "run_id": uuid.uuid4().hex,
            "binary": str(Path(binary).resolve()), "sha256": checksum, "inventory": inventory,
            "order": [n["name"] for n in deployment_order(inventory)]}


def wait_node(fleet, node, timeout=180):
    deadline = time.monotonic() + timeout
    last = "no successful probe"
    while time.monotonic() < deadline:
        try:
            current = fleet.inspect(node["host"])
            repair = current["status"].get("local_repair", {})
            if current["identity"] != node["identity"] or current["store_id"] != node["store_id"]:
                raise ValueError("Node/Store identity changed during a binary-only deployment")
            if (current["status"]["cluster"].get("root_quorum_confirmed") and repair.get("worker_enabled") and
                    repair.get("last_check_unix") and not repair.get("total_issues")):
                return current
            last = current["status"]["cluster"].get("reasons")
        except ValueError:
            raise
        except Exception as error:
            last = str(error)
        time.sleep(2)
    raise RuntimeError(f"Node did not recover before timeout: {last}")


def require_rollout_start(inventory, allow_geo_debt):
    if not allow_geo_debt:
        require_healthy(inventory["nodes"][0]["status"])
        return
    for node in inventory["nodes"]:
        status = node["status"]
        cluster = status["cluster"]
        local = status.get("local_repair") or {}
        storage = status.get("local_storage") or {}
        if (not cluster.get("root_quorum_confirmed") or cluster.get("state") not in ("available", "degraded") or
                status.get("local_repair_error") or status.get("local_storage_error") or
                not local.get("worker_enabled") or not local.get("last_check_unix") or
                local.get("total_issues") or local.get("repair_debt") or local.get("volume_write_errors") or
                (local.get("volume_migration") or {}).get("active") or storage.get("store_id") != node["store_id"]):
            raise RuntimeError("corrective rollout requires confirmed Root and healthy local repair/storage")
        volumes = storage.get("volumes", [])
        if not any(v["state"] == "active" and v["health"] == "healthy" for v in volumes):
            raise RuntimeError("corrective rollout requires a healthy active Volume")
        if any(v["state"] != "removed" and (v["state"] != "active" or v["health"] != "healthy") for v in volumes):
            raise RuntimeError("corrective rollout refuses Volume failure or active maintenance")
        peers = {p["node_id"] for p in status["root"]["peers"]}
        observations = cluster.get("repair_observations", [])
        if len(observations) != len(peers) or {p["node_id"] for p in observations} != peers:
            raise RuntimeError("corrective rollout requires complete peer observations")
        for peer in observations:
            if (not peer.get("reachable") or not peer.get("worker_enabled") or not peer.get("last_check_unix") or
                    peer.get("repair_debt") or peer.get("write_quarantines") or peer.get("error") or
                    peer.get("volume_write_errors") or any(v.get("state") != "removed" and
                        (v.get("state") != "active" or v.get("health") != "healthy") for v in peer.get("volumes", []))):
                # Keep the sampled identity and original error, rather than
                # discarding the only evidence of a short-lived peer fault.
                raise RuntimeError("corrective rollout refuses a peer fault beyond Geo debt: " +
                                   json.dumps(peer, sort_keys=True))
        for reason in cluster.get("reasons", []):
            if re.fullmatch(r"Geo repair has [1-9][0-9]* pending item\(s\)", reason):
                continue
            match = re.fullmatch(r"Node (\S+) has [1-9][0-9]* repair/maintenance debt item\(s\)", reason)
            if not match or match[1] not in peers:
                raise RuntimeError("corrective rollout refuses a non-Geo degradation: " + reason)
        if cluster["state"] == "degraded" and not cluster.get("reasons"):
            raise RuntimeError("corrective rollout refuses unexplained degradation")


def apply(fleet, plan, *, allow_geo_debt=False):
    if plan.get("kind") != "deployment" or executable_sha256(plan["binary"]) != plan["sha256"]:
        raise ValueError("wrong plan type or changed local candidate binary")
    inventory = fleet.discover(plan["inventory"]["entry_host"])
    validate_plan(plan, inventory)
    # Initial and final readiness are global. During each serial step require
    # Node recovery and Root quorum, and stop immediately on a failed step.
    require_rollout_start(inventory, allow_geo_debt)
    source = {n["identity"]["node_id"]: n for n in plan["inventory"]["nodes"]}
    stage = "/var/tmp/dubi-deploy-" + plan["run_id"]
    report = {"started_utc": now(), "sha256": plan["sha256"], "completed": [], "outcome": "running",
              "allow_geo_debt_at_start": allow_geo_debt,
              "initial_reasons": {n["name"]: n["status"]["cluster"].get("reasons", []) for n in inventory["nodes"]}}
    save = lambda: (fleet.evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    try:
        for node in inventory["nodes"]:
            if node["binary_sha256"] not in (source[node["identity"]["node_id"]]["binary_sha256"], plan["sha256"]):
                raise RuntimeError("an unplanned binary is installed")
        # Stage on every Node before restarting the first one.
        for node in inventory["nodes"]:
            if node["binary_sha256"] == plan["sha256"]:
                continue
            fleet.request(node["host"], "mkdir", path=stage)
            fleet.copy(plan["binary"], node["host"], stage + "/candidate")
            fleet.request(node["host"], "stage-check", expected=source[node["identity"]["node_id"]],
                          staged=stage + "/candidate", sha256=plan["sha256"])
        for node in deployment_order(inventory):
            current = fleet.inspect(node["host"])
            no_workload({"nodes": [{**node, **current}]})
            if current["binary_sha256"] != plan["sha256"]:
                fleet.request(node["host"], "install", expected=source[node["identity"]["node_id"]],
                              staged=stage + "/candidate", sha256=plan["sha256"])
            current = wait_node(fleet, node)
            if current["binary_sha256"] != plan["sha256"]:
                raise RuntimeError("running process did not switch to the candidate")
            report["completed"].append({"name": node["name"], "node_id": node["identity"]["node_id"],
                                        "sha256": current["binary_sha256"]})
            save()
        final = fleet.discover(inventory["entry_host"])
        require_healthy(final["nodes"][0]["status"])
        if any(n["binary_sha256"] != plan["sha256"] for n in final["nodes"]):
            raise RuntimeError("final running binary hashes differ")
        report["outcome"] = "passed"
    except Exception as error:
        report.update(outcome="stopped", error=str(error))
        raise
    finally:
        report["finished_utc"] = now()
        save()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    plan = commands.add_parser("plan", help="read-only inventory and deployment plan")
    plan.add_argument("--host", required=True)
    plan.add_argument("--binary", required=True)
    plan.add_argument("--output", required=True, type=Path)
    plan.add_argument("--ssh-key", required=True)
    execute = commands.add_parser("apply", help="apply an existing plan after fresh safety checks")
    execute.add_argument("--plan", required=True, type=Path)
    execute.add_argument("--output", required=True, type=Path)
    execute.add_argument("--ssh-key", required=True)
    execute.add_argument("--allow-geo-debt", action="store_true",
                         help="corrective rollout with only existing Geo debt; all other gates and final health remain required")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    fleet = Fleet(args.ssh_key, args.output)
    if args.action == "plan":
        result = make_plan(fleet, args.host, args.binary)
        (args.output / "plan.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"plan": str(args.output / "plan.json"), "order": result["order"], "sha256": result["sha256"],
                          "active_workloads": {n["name"]: n["active_workloads"] for n in result["inventory"]["nodes"] if n["active_workloads"]}}))
    else:
        print(json.dumps(apply(fleet, json.loads(args.plan.read_text()), allow_geo_debt=args.allow_geo_debt)))


if __name__ == "__main__":
    main()
