#!/usr/bin/env python3
"""Prepare and run explicit VPS Node/Volume fault scenarios after the soak."""
import argparse
import base64
import json
from pathlib import Path
import re
import time
import uuid

from vps_cluster_tools import Fleet, executable_sha256, now, require_healthy, server_options, validate_plan

CASES = ("node-recover", "node-remove-join", "volume-add-before", "volume-add-after", "volume-no-add", "volume-no-capacity", "volume-no-capacity-cross-region")


def physical_coverage(proof, status):
    members = {n["node_id"]: n for n in status["root"]["peers"]}
    held = {n["node_id"] for n in proof["nodes"] if n["held"] and n["node_id"] in members}
    regions = {n["region_id"] for n in members.values()}
    covered = []
    for region in regions:
        cohort = {n["node_id"] for n in members.values() if n["region_id"] == region}
        if len(held & cohort) >= len(cohort) // 2 + 1:
            covered.append(region)
    return sorted(covered), len(regions) // 2 + 1


def target_from(inventory, name):
    nodes = [n for n in inventory["nodes"] if n["name"] == name]
    if len(nodes) != 1:
        raise ValueError("select one unique Node name from the Root inventory")
    return nodes[0]


def make_plan(fleet, host, name, case, probe, volume_id=None):
    probe_hash = executable_sha256(probe)
    inventory = fleet.discover(host)
    target = target_from(inventory, name)
    if case == "node-remove-join":
        server_options(target)  # Reject an unsupported unit before any Remove.
    if target["host"] == host:
        raise ValueError("use a surviving Node as --host; the target must be another Node")
    voters = target["status"]["root"]["voters"]
    if target["identity"]["raft_id"] in voters and len(voters) - 1 < len(voters) // 2 + 1:
        raise ValueError("stopping the target would lose the existing Root majority")
    regional = [n for n in inventory["nodes"] if n["region"] == target["region"]]
    if case.endswith("cross-region"):
        if len(regional) != 1 or len({n["region"] for n in inventory["nodes"]}) < 3:
            raise ValueError("cross-Region capacity case requires a dedicated one-Node Region and at least three Regions")
    elif len(regional) < 3:
        raise ValueError("this scenario requires at least three Nodes in the target Region")
    selected = None
    if case.startswith("volume-"):
        volumes = [v for v in target["status"]["local_storage"]["volumes"] if v["state"] == "active" and v["health"] == "healthy"]
        if volume_id:
            volumes = [v for v in volumes if v["volume_id"] == volume_id]
        if len(volumes) != 1:
            raise ValueError("select one healthy active Volume with --volume-id")
        selected = volumes[0]
        if len([v for v in target["status"]["local_storage"]["volumes"] if v["state"] == "active"]) != 1:
            raise ValueError("use a dedicated single-active-Volume Node for reproducible fault placement")
    return {"kind": "foundation-fault", "case": case, "created_utc": now(), "run_id": uuid.uuid4().hex,
            "inventory": inventory, "target": name, "volume": selected,
            "probe": str(Path(probe).resolve()), "probe_sha256": probe_hash}


class Scenario:
    def __init__(self, fleet, plan, timeout=1800):
        self.fleet, self.plan, self.timeout = fleet, plan, timeout
        self.target = target_from(plan["inventory"], plan["target"])
        self.entry = next(n for n in plan["inventory"]["nodes"] if n["host"] == plan["inventory"]["entry_host"])
        self.work = "/var/tmp/dubi-fault-" + plan["run_id"]
        self.tenant = "vpscase-" + plan["run_id"]
        self.group = None
        self.fixtures = []
        self.stopped = False
        self.result = {"case": plan["case"], "started_utc": now(), "assertions": [], "outcome": "running"}

    def save(self):
        (self.fleet.evidence / "result.json").write_text(json.dumps(self.result, indent=2) + "\n")

    def assertion(self, label, **evidence):
        self.result["assertions"].append({"utc": now(), "label": label, **evidence})
        self.save()

    def cli(self, node, verb, values=(), server=None, request_id=None, allow_failure=False):
        args = [*verb, "--store", node["store"]]
        if server:
            args += ["--server", server]
        if request_id:
            args += ["--request-id", request_id]
        response = self.fleet.cli(node, [*args, *values], allow_failure=allow_failure)
        return response if allow_failure else json.loads(response["stdout"])

    def status(self):
        return self.cli(self.entry, ["status"])

    def wait(self, label, check):
        deadline, last = time.monotonic() + self.timeout, "not yet observed"
        while time.monotonic() < deadline:
            try:
                result = check()
                if result:
                    self.assertion(label, evidence=result)
                    return result
            except (ValueError, TypeError):
                raise
            except Exception as error:
                last = str(error)
            time.sleep(5)
        raise RuntimeError(f"{label} timed out: {last}")

    def write(self, key, server=None):
        fixture = {"tenant": self.tenant, "dataset": "probe", "key": key,
                   "value": f"dubi-vps-fault:{self.plan['run_id']}:{key}", "owner_group_id": self.group}
        response = self.cli(self.entry, ["record", "put"], (self.tenant, "probe", key, fixture["value"]),
                            server=server, request_id="fault-" + self.plan["run_id"] + "-" + key)
        self.fixtures.append(fixture)
        self.assertion("write-" + key, response=response)
        return fixture, response

    def probe(self, fixture):
        result = self.fleet.request(self.entry["host"], "probe", expected=self.entry,
                                    helper=self.work + "/holding-probe", helper_sha256=self.plan["probe_sha256"],
                                    input={"status": self.status(), **fixture})
        if result["code"]:
            raise RuntimeError(result["stderr"] or result["stdout"])
        return json.loads(result["stdout"])

    def held(self, fixture, node, volume_id=None):
        proof = self.probe(fixture)
        for holding in proof["nodes"]:
            if (holding["node_id"] == node["identity"]["node_id"] and holding["held"] and
                    holding["store_id"] == node["store_id"] and (not volume_id or holding["volume_id"] == volume_id)):
                return {"record_id": proof["record_id"], **holding}
        return False

    def reads(self, server=None):
        for fixture in self.fixtures:
            result = self.cli(self.entry, ["record", "get"], (self.tenant, "probe", fixture["key"]), server=server)
            if result.get("found") is not True or base64.b64decode(result["value"]) != fixture["value"].encode():
                raise ValueError("read returned different bytes")
        self.assertion("logical-reads", server=server or self.entry["address"])

    def wait_for_serving_reads(self, server):
        # Bytes can be present before the successor cohort is published and
        # activated. Wait for that normal transition; wrong bytes, identities,
        # authorization and unrelated protocol errors remain immediate failures.
        def serving():
            try:
                self.reads(server)
            except RuntimeError as error:
                message = str(error)
                if re.search(r"permission.?denied|unauthenticated|unauthori[sz]ed|data.?loss|corrupt|certificate|mismatch", message, re.I):
                    raise ValueError("permanent read failure after Join: " + message) from error
                transitions = ("confirmed Region members are not available at Root read cut",
                               "regional placement participant is not a current Root member",
                               "Range placement Region generation is not current",
                               "obsolete Region plan or publication")
                if not any(text in message for text in transitions):
                    raise ValueError("read failed outside membership convergence: " + message) from error
                return False
            return {"checked": True, "server": server}
        return self.wait("new-identity-serving-reads", serving)

    def service(self, action):
        result = self.fleet.request(self.target["host"], "service", expected=self.target, action=action)
        self.stopped = action == "stop"
        return result

    def wait_target(self):
        def inspect():
            node = self.fleet.inspect(self.target["host"])
            if not node["status"]["cluster"]["root_quorum_confirmed"]:
                return False
            peers = [p for p in node["status"]["root"]["peers"] if p["node_id"] == node["identity"]["node_id"]]
            if len(peers) != 1:
                return False
            peer = peers[0]
            # Root admission precedes local Group recovery and desired-name
            # publication. A fresh joining identity can briefly be unnamed;
            # it is not ready for any subsequent fault/volume operation yet.
            if (self.plan["case"] == "node-remove-join" and
                    node["identity"]["node_id"] != self.target["identity"]["node_id"] and
                    not peer.get("name") and self.target["name"] and
                    peer["region_id"] == self.target["region"] and peer["address"] == self.target["address"]):
                raise RuntimeError("joining Node has not yet published its intended name")
            if (peer.get("name", ""), peer["region_id"], peer["address"]) != (self.target["name"], self.target["region"], self.target["address"]):
                raise ValueError("restarted/joined Node changed its planned name, Region or address")
            return {**self.target, **node}
        return self.wait("target-reachable", inspect)

    def add_volume(self, path):
        # Volume registration requires an existing directory. Keep test data
        # under the helper's private managed paths; mounted fixtures already
        # exist and this check does not recreate an unavailable production path.
        self.fleet.request(self.target["host"], "mkdir", path=path)
        return self.cli(self.target, ["volume", "add"], (path,))

    def remove_volume(self, volume_id):
        response = self.cli(self.target, ["volume", "remove"], (volume_id,), allow_failure=True)
        if response["code"]:
            # A lost reply is accepted only if the persistent registry already
            # contains the exact removal tombstone, never by guessing success.
            volumes = self.fleet.inspect(self.target["host"])["status"]["local_storage"]["volumes"]
            if not any(v["volume_id"] == volume_id and v["state"] == "removed" for v in volumes):
                raise RuntimeError(response["stderr"])

    def healthy(self):
        status = self.status()
        require_healthy(status)
        return {"state": status["cluster"]["state"], "root_index": status["root"]["applied_index"]}

    def prepare(self):
        fresh = self.fleet.discover(self.plan["inventory"]["entry_host"])
        validate_plan(self.plan, fresh)
        require_healthy(fresh["nodes"][0]["status"])
        expected_versions = {n["identity"]["node_id"]: n["binary_sha256"] for n in self.plan["inventory"]["nodes"]}
        if any(n["binary_sha256"] != expected_versions[n["identity"]["node_id"]] for n in fresh["nodes"]):
            raise RuntimeError("running binary changed since fault planning; create a new plan")
        target = target_from(fresh, self.target["name"])
        if target["identity"] != self.target["identity"]:
            raise RuntimeError("target identity changed")
        if target["server_args"] != self.target["server_args"]:
            raise RuntimeError("target service arguments changed since planning")
        if self.plan["case"] == "node-remove-join":
            server_options(target)
        if self.plan["volume"]:
            matching = [v for v in target["status"]["local_storage"]["volumes"] if v["volume_id"] == self.plan["volume"]["volume_id"]]
            active = {v["volume_id"] for v in target["status"]["local_storage"]["volumes"] if v["state"] == "active"}
            if (len(matching) != 1 or matching[0]["path"] != self.plan["volume"]["path"] or matching[0]["health"] != "healthy" or
                    matching[0]["state"] != "active" or active != {self.plan["volume"]["volume_id"]}):
                raise RuntimeError("target Volume changed since planning")
        self.fleet.request(self.entry["host"], "mkdir", path=self.work)
        self.fleet.copy(self.plan["probe"], self.entry["host"], self.work + "/holding-probe")
        self.fleet.request(self.entry["host"], "stage-check", expected=self.entry,
                           staged=self.work + "/holding-probe", sha256=self.plan["probe_sha256"])
        response = self.cli(self.entry, ["dataset", "create"], (self.tenant, "probe"),
                            server=self.target["address"], request_id="fault-dataset-" + self.plan["run_id"])
        self.group = response["owner_group_id"]
        fixture, _ = self.write("before", self.target["address"])
        # Fixture warm-up is preparation only. After the fault no target reads
        # or fetches are allowed until a physical holding has been observed.
        self.reads(self.target["address"])
        volume = self.plan["volume"]
        self.wait("initial-target-physical-holding", lambda: self.held(fixture, self.target, volume["volume_id"] if volume else None))
        return fixture

    def node_case(self):
        original = self.target
        self.service("stop")
        self.wait("surviving-root-quorum", lambda: self.status()["cluster"].get("root_quorum_confirmed"))
        self.wait("surviving-logical-reads", lambda: self.reads() or {"checked": True})
        self.write("offline")
        if self.plan["case"] == "node-recover":
            self.service("start")
            current = self.wait_target()
            if current["identity"] != original["identity"] or current["store_id"] != original["store_id"]:
                raise RuntimeError("ordinary restart changed Node or Store identity")
            old_volumes = {v["volume_id"] for v in original["status"]["local_storage"]["volumes"]}
            new_volumes = {v["volume_id"] for v in current["status"]["local_storage"]["volumes"]}
            if old_volumes != new_volumes:
                raise RuntimeError("ordinary restart changed Volume identity")
        else:
            self.cli(self.entry, ["node", "remove"], (original["name"],))
            status = self.status()
            if any(p["node_id"] == original["identity"]["node_id"] for p in status["root"]["peers"]):
                raise RuntimeError("removed identity remains in Root membership")
            denied = self.cli(original, ["status"], server=self.entry["address"], allow_failure=True)
            if not denied["code"] or not re.search(r"permission|fenc|member|unauthor|unauthentic", denied["stderr"], re.I):
                raise RuntimeError("old identity was not explicitly rejected by the surviving Node")
            self.assertion("old-identity-fenced", stderr=denied["stderr"])
            ticket = self.fleet.request(self.entry["host"], "ticket", expected=self.entry)
            new_store = "/var/lib/dubi-acceptance-" + self.plan["run_id"]
            self.fleet.request(original["host"], "join-service", expected=original, new_store=new_store,
                               work=self.work, ticket_b64=ticket["ticket_b64"])
            current = self.wait_target()
            if current["store"] != new_store or current["identity"]["cluster_id"] != original["identity"]["cluster_id"]:
                raise RuntimeError("Join did not create a new Store in the same Cluster")
            for key in ("node_id", "raft_id"):
                if current["identity"][key] == original["identity"][key]:
                    raise RuntimeError("Join reused a fenced identity")
            if current["store_id"] == original["store_id"]:
                raise RuntimeError("Join reused StoreID")
            old_volumes = {v["volume_id"] for v in original["status"]["local_storage"]["volumes"]}
            if old_volumes & {v["volume_id"] for v in current["status"]["local_storage"]["volumes"]}:
                raise RuntimeError("Join reused VolumeID")
            self.assertion("new-identities", identity=current["identity"], store_id=current["store_id"])
        for fixture in self.fixtures:
            self.wait("automatic-physical-repair-" + fixture["key"], lambda f=fixture: self.held(f, current))
        if self.plan["case"] == "node-remove-join":
            self.fleet.request(original["host"], "finish-join", expected=original, new_store=current["store"],
                               new_node_id=current["identity"]["node_id"], work=self.work)
            self.target = current
            restarted = self.wait_target()
            if restarted["identity"] != current["identity"] or restarted["store_id"] != current["store_id"]:
                raise RuntimeError("cold restart after Join changed the new identity")
            for fixture in self.fixtures:
                self.wait("new-identity-restart-holding-" + fixture["key"], lambda f=fixture: self.held(f, restarted))
        if self.plan["case"] == "node-remove-join":
            self.wait_for_serving_reads(self.target["address"])
        else:
            self.reads(self.target["address"])

    def volume_case(self):
        case, original = self.plan["case"], self.plan["volume"]
        target_work = self.work + "-volume"
        capacity = None
        fresh_id = None
        fresh_path = "/var/tmp/dubi-acceptance-volume-" + self.plan["run_id"]
        if "no-capacity" in case:
            capacity = self.fleet.request(self.target["host"], "limited-volume", expected=self.target, work=target_work)
            added = self.add_volume(capacity["path"])
            fresh_id = added["volume_id"]
        elif case == "volume-add-before":
            fresh_id = self.add_volume(fresh_path)["volume_id"]
        self.service("stop")
        self.fleet.request(self.target["host"], "offline-volume", expected=self.target,
                           path=original["path"], volume_id=original["volume_id"], run_id=self.plan["run_id"])
        self.service("start")
        self.wait_target()
        self.remove_volume(original["volume_id"])
        if capacity:
            self.fleet.request(self.target["host"], "reserve-space", expected=self.target, work=target_work, device=capacity["device"])
        self.reads()  # Surviving entry, not a target read that could repair it.
        fixture, response = self.write("unreadable")
        if case.endswith("cross-region") and response.get("state") != "geo_ready":
            raise RuntimeError("one-Node Region write did not require synchronous GeoReady")
        if case == "volume-add-after":
            fresh_id = self.add_volume(fresh_path)["volume_id"]
        if case == "volume-no-add" or capacity:
            def degraded_coverage():
                proof = self.probe(fixture)
                target_id = self.target["identity"]["node_id"]
                if any(n["held"] for n in proof["nodes"] if n["node_id"] == target_id):
                    raise ValueError("capacity-less target unexpectedly claimed bytes for a new record")
                status = self.status()
                if {p["node_id"] for p in status["root"]["peers"]} != {n["identity"]["node_id"] for n in self.plan["inventory"]["nodes"]}:
                    raise ValueError("capacity failure silently changed membership/denominators")
                covered, required = physical_coverage(proof, status)
                if (not status["cluster"]["root_quorum_confirmed"] or status["cluster"]["state"] != "degraded" or
                        len(covered) < required or (not case.endswith("cross-region") and self.target["region"] not in covered)):
                    return False
                return {"physical_observations": proof, "covered_regions": covered, "required_regions": required, "response": response}
            self.wait("available-without-local-capacity", degraded_coverage)
            # Recovery happens only after the no-new-Volume assertions above.
            # It is test teardown, not evidence that the no-add phase was healthy.
            if capacity:
                self.fleet.request(self.target["host"], "release-space", expected=self.target, work=target_work, device=capacity["device"])
            else:
                fresh_id = self.add_volume(fresh_path)["volume_id"]
        for record in self.fixtures:
            self.wait("automatic-volume-repair-" + record["key"], lambda f=record: self.held(f, self.target, fresh_id))
        if capacity:
            # Move data off the bounded mount before unmounting; never discard
            # live Pack bytes or leave the Node dependent on an ephemeral mount.
            final_id = self.add_volume(fresh_path)["volume_id"]
            self.cli(self.target, ["volume", "drain"], (fresh_id,))
            self.wait("bounded-volume-drained", lambda: self._try_remove(fresh_id))
            for record in self.fixtures:
                self.wait("recovery-volume-holding-" + record["key"], lambda f=record: self.held(f, self.target, final_id))
            self.fleet.request(self.target["host"], "detach-limited", expected=self.target, work=target_work, device=capacity["device"])
        self.reads(self.target["address"])

    def _try_remove(self, volume_id):
        self.remove_volume(volume_id)
        return {"removed_volume_id": volume_id}

    def run(self):
        self.fleet.evidence.mkdir(parents=True, exist_ok=True)
        try:
            if not re.fullmatch(r"[a-f0-9]{32}", self.plan["run_id"]) or executable_sha256(self.plan["probe"]) != self.plan["probe_sha256"]:
                raise ValueError("changed probe or invalid plan identifier")
            self.prepare()
            if self.plan["case"].startswith("node-"):
                self.node_case()
            else:
                self.volume_case()
            self.wait("cluster-converged", self.healthy)
            final = self.fleet.discover(self.entry["host"])
            before_ids = {n["identity"]["node_id"] for n in self.plan["inventory"]["nodes"]}
            if self.plan["case"] == "node-remove-join":
                before_ids.remove(target_from(self.plan["inventory"], self.plan["target"])["identity"]["node_id"])
                before_ids.add(self.target["identity"]["node_id"])
            if before_ids != {n["identity"]["node_id"] for n in final["nodes"]}:
                raise RuntimeError("final membership differs from the explicit scenario")
            initial_hashes = {n["host"]: n["binary_sha256"] for n in self.plan["inventory"]["nodes"]}
            if any(n["binary_sha256"] != initial_hashes[n["host"]] for n in final["nodes"]):
                raise RuntimeError("a running binary changed during fault acceptance")
            self.result["outcome"] = "passed"
        except Exception as error:
            # Do not resurrect a fenced identity, rename an obsolete Volume
            # back into place, or hide a partially completed operation.
            self.result.update(outcome="stopped", error=str(error))
            if self.stopped:
                try:
                    status = self.status()
                    original = target_from(self.plan["inventory"], self.plan["target"])
                    if (status["cluster"]["root_quorum_confirmed"] and
                            any(n["node_id"] == original["identity"]["node_id"] for n in status["root"]["peers"])):
                        self.fleet.request(original["host"], "service", expected=original, action="start")
                        self.result["cleanup"] = "started the still-enrolled identity; fault evidence retained"
                except Exception as cleanup_error:
                    self.result["cleanup_error"] = str(cleanup_error)
            raise
        finally:
            self.result["finished_utc"] = now()
            self.save()
        return self.result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    plan = commands.add_parser("plan", help="read-only scenario plan; does not inject a fault")
    plan.add_argument("--host", required=True)
    plan.add_argument("--node", required=True)
    plan.add_argument("--case", choices=CASES, required=True)
    plan.add_argument("--holding-probe", required=True)
    plan.add_argument("--volume-id")
    plan.add_argument("--output", required=True, type=Path)
    plan.add_argument("--ssh-key", required=True)
    run = commands.add_parser("run", help="execute the saved scenario after fresh identity/soak gates")
    run.add_argument("--plan", required=True, type=Path)
    run.add_argument("--output", required=True, type=Path)
    run.add_argument("--timeout", type=int, default=1800)
    run.add_argument("--ssh-key", required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    fleet = Fleet(args.ssh_key, args.output)
    if args.action == "plan":
        result = make_plan(fleet, args.host, args.node, args.case, args.holding_probe, args.volume_id)
        (args.output / "plan.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"plan": str(args.output / "plan.json"), "case": args.case, "node": args.node,
                          "active_workloads": {n["name"]: n["active_workloads"] for n in result["inventory"]["nodes"] if n["active_workloads"]}}))
    else:
        saved = json.loads(args.plan.read_text())
        if saved.get("kind") != "foundation-fault" or saved.get("case") not in CASES or args.timeout <= 0:
            raise ValueError("invalid scenario plan or timeout")
        print(json.dumps(Scenario(fleet, saved, args.timeout).run()))


if __name__ == "__main__":
    main()
