#!/usr/bin/env python3
"""Offline safety tests; never invoke a real service, mount, or fault action."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import pathlib
import tempfile
import unittest
from unittest.mock import Mock, patch

import vps_cluster_deploy as deploy
import vps_cluster_tools as tools
import vps_foundation_faults as faults


def inventory():
    nodes = []
    for region, base in (("a", 10), ("b", 20), ("c", 30)):
        for i in range(1, 4):
            name = region + str(i)
            nodes.append({"name": name, "host": f"10.{base}.0.{i}", "address": f"10.{base}.0.{i}:12443", "region": region,
                          "identity": {"node_id": "node-" + name, "raft_id": base+i, "cluster_id": "cluster-test"},
                          "store": "/var/lib/test-" + name, "store_id": "store-" + name,
                          "binary": "/usr/local/bin/dubi", "binary_sha256": "old-" + name,
                          "active_workloads": [], "server_args": ["server", "--store", "/var/lib/test-"+name]})
    root = {"cluster_id": "cluster-test", "voters": [11, 21, 31], "learners": [12,13,22,23,32,33], "leader_id": 11,
            "peers": [{"name": n["name"], "node_id": n["identity"]["node_id"], "raft_id": n["identity"]["raft_id"],
                       "address": n["address"], "region_id": n["region"]} for n in nodes]}
    for node in nodes:
        node["status"] = {"observed_node_id": node["identity"]["node_id"], "root": deepcopy(root),
                          "cluster": {"state": "available", "root_quorum_confirmed": True,
                                      "repair_observations": [{"name": n["name"], "node_id": n["identity"]["node_id"],
                                                               "reachable": True, "worker_enabled": True, "last_check_unix": 100} for n in nodes]},
                          "local_storage": {"store_id": node["store_id"], "volumes": [{"volume_id": "volume-"+node["name"],
                                            "path": node["store"]+"/volume", "state": "active", "health": "healthy"}]},
                          "local_repair": {"worker_enabled": True, "last_check_unix": 100, "total_issues": 0}}
    return {"entry_host": nodes[0]["host"], "nodes": nodes, "topology_sha256": tools.fingerprint(nodes[0]["status"])}


class FakeFleet:
    def __init__(self, directory, state=None):
        self.evidence = Path(directory)
        self.state = deepcopy(state or inventory())
        self.operations = []
        self.fail_stage = None
        self.change_identity = False

    def discover(self, host):
        return deepcopy(self.state)

    def inspect(self, host):
        return deepcopy(next(n for n in self.state["nodes"] if n["host"] == host))

    def request(self, host, operation, **args):
        self.operations.append((operation, host, args))
        if operation == "stage-check" and host == self.fail_stage:
            raise RuntimeError("staged file corrupted")
        if operation == "install":
            node = next(n for n in self.state["nodes"] if n["host"] == host)
            node["binary_sha256"] = args["sha256"]
            if self.change_identity:
                node["identity"]["node_id"] = "unplanned-new-node"
        return {}

    def copy(self, source, host, destination):
        self.operations.append(("copy", host, destination))


class SafetyTests(unittest.TestCase):
    def test_mount_cleanup_refuses_live_volumes_in_child_directories(self):
        root = "/var/tmp/dubi-fixture/limited-volume"
        for state in ("registered", "active", "draining"):
            for path in (root, root+"/data", root+"/nested/data"):
                with self.subTest(state=state, path=path):
                    self.assertTrue(tools.volume_uses_mount({"state": state, "path": path}, root))
        self.assertFalse(tools.volume_uses_mount({"state": "removed", "path": root+"/data"}, root))
        self.assertFalse(tools.volume_uses_mount({"state": "active", "path": root+"-other/data"}, root))

    def test_volume_directory_is_prepared_before_registration_and_failure_stops_cli(self):
        scenario = self.scenario("volume-add-after")
        destination = "/var/tmp/dubi-acceptance-volume-test"
        events = []
        scenario.fleet.request = lambda host, operation, **args: events.append((operation, host, args))
        scenario.cli = lambda node, args, values: events.append(("cli", args, values)) or {"volume_id": "new-volume"}
        self.assertEqual(scenario.add_volume(destination), {"volume_id": "new-volume"})
        self.assertEqual(events, [("mkdir", scenario.target["host"], {"path": destination}),
                                  ("cli", ["volume", "add"], (destination,))])
        scenario.fleet.request = Mock(side_effect=RuntimeError("unsafe directory"))
        scenario.cli = Mock()
        with self.assertRaisesRegex(RuntimeError, "unsafe directory"):
            scenario.add_volume(destination)
        scenario.cli.assert_not_called()

    def test_join_reads_wait_for_successor_cohort_after_physical_copy(self):
        fleet = FakeFleet(self.path)
        plan = {"inventory": fleet.state, "target": "a3", "case": "node-remove-join", "run_id": "a"*32}
        scenario = faults.Scenario(fleet, plan, 2)
        scenario.reads = Mock(side_effect=[RuntimeError("confirmed Region members are not available at Root read cut"), None])
        with patch.object(faults.time, "sleep"):
            scenario.wait_for_serving_reads("new-node:12443")
        self.assertEqual(scenario.reads.call_count, 2)
        self.assertEqual(fleet.operations, [])
        self.assertEqual(scenario.result["assertions"][-1]["label"], "new-identity-serving-reads")

    def test_join_read_wait_cannot_mask_permanent_failures(self):
        transition = "confirmed Region members are not available at Root read cut"
        for message in ["PermissionDenied " + transition, "Unauthenticated " + transition,
                        "DataLoss corrupt Pack " + transition, "protocol header invalid"]:
            with self.subTest(message=message):
                fleet = FakeFleet(self.path)
                scenario = faults.Scenario(fleet, {"inventory":fleet.state,"target":"a3","case":"node-remove-join","run_id":"b"*32}, 2)
                scenario.reads = Mock(side_effect=RuntimeError(message))
                with self.assertRaises(ValueError):
                    scenario.wait_for_serving_reads("new-node:12443")
                self.assertEqual(scenario.reads.call_count, 1)
                self.assertEqual(fleet.operations, [])

    def test_join_read_wait_never_retries_wrong_data(self):
        fleet = FakeFleet(self.path)
        scenario = faults.Scenario(fleet, {"inventory":fleet.state,"target":"a3","case":"node-remove-join","run_id":"c"*32}, 2)
        scenario.reads = Mock(side_effect=ValueError("read returned different bytes"))
        with self.assertRaisesRegex(ValueError,"different bytes"):
            scenario.wait_for_serving_reads("new-node:12443")
        self.assertEqual(scenario.reads.call_count, 1)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.binary = self.path / "candidate"
        header = bytearray(20)
        header[:6] = b"\x7fELF\x02\x01"
        header[18:20] = b"\x3e\x00"
        self.binary.write_bytes(header)

    def test_deployment_stages_all_before_serial_restarts_and_keeps_leader_last(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        result = deploy.apply(fleet, plan)
        self.assertEqual(result["outcome"], "passed")
        installs = [op for op in fleet.operations if op[0] == "install"]
        self.assertEqual(len(installs), 9)
        self.assertEqual(installs[-1][1], fleet.state["entry_host"])
        first_install = next(i for i, op in enumerate(fleet.operations) if op[0] == "install")
        self.assertEqual(sum(op[0] == "stage-check" for op in fleet.operations[:first_install]), 9)
        self.assertEqual(len(result["completed"]), 9)

    def test_failed_stage_never_restarts_any_node(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        fleet.fail_stage = fleet.state["nodes"][3]["host"]
        with self.assertRaisesRegex(RuntimeError, "corrupted"):
            deploy.apply(fleet, plan)
        self.assertFalse(any(op[0] == "install" for op in fleet.operations))
        result = json.loads((self.path / "result.json").read_text())
        self.assertEqual(result["outcome"], "stopped")
        self.assertEqual(result["completed"], [])

    def geo_degraded_inventory(self):
        state = inventory()
        for node in state["nodes"]:
            node["status"]["cluster"].update(state="degraded", reasons=["Geo repair has 2 pending item(s)"])
            node["status"]["local_repair"]["geo_record_debt"] = 2
            for peer in node["status"]["cluster"]["repair_observations"]:
                peer["geo_debt"] = 2
        return state

    def test_corrective_rollout_allows_only_initial_geo_debt_and_still_requires_final_health(self):
        fleet = FakeFleet(self.path, self.geo_degraded_inventory())
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        with self.assertRaisesRegex(RuntimeError, "converged"):
            deploy.apply(fleet, plan)
        self.assertEqual(fleet.operations, [])
        with self.assertRaisesRegex(RuntimeError, "converged"):
            deploy.apply(fleet, plan, allow_geo_debt=True)
        self.assertEqual(sum(op[0] == "install" for op in fleet.operations), 9)
        result = json.loads((self.path / "result.json").read_text())
        self.assertTrue(result["allow_geo_debt_at_start"])
        self.assertEqual(result["outcome"], "stopped")

    def test_corrective_rollout_refuses_non_geo_faults_and_active_workloads(self):
        mutations = [
            lambda s: s["nodes"][0]["status"]["cluster"].update(root_quorum_confirmed=False),
            lambda s: s["nodes"][0]["status"]["cluster"].update(reasons=["unknown degradation"]),
            lambda s: s["nodes"][0]["status"]["local_repair"].update(total_issues=1),
            lambda s: s["nodes"][0]["status"]["local_storage"]["volumes"][0].update(health="missing"),
            lambda s: s["nodes"][0]["status"]["cluster"]["repair_observations"][0].update(reachable=False),
            lambda s: s["nodes"][0]["status"]["local_repair"].update(volume_migration={"active": True}),
            lambda s: s["nodes"][0].update(active_workloads=[{"pid": 123, "command": ["vps_cross_region_soak.sh"]}]),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                state = self.geo_degraded_inventory()
                mutate(state)
                fleet = FakeFleet(self.path, state)
                plan = deploy.make_plan(fleet, state["entry_host"], self.binary)
                with self.assertRaises(RuntimeError):
                    deploy.apply(fleet, plan, allow_geo_debt=True)
                self.assertEqual(fleet.operations, [])

    def test_changed_running_identity_stops_the_rollout(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        fleet.change_identity = True
        with self.assertRaisesRegex(ValueError, "identity changed"):
            deploy.apply(fleet, plan)
        self.assertEqual(sum(op[0] == "install" for op in fleet.operations), 1)

    def test_candidate_modified_after_planning_is_not_uploaded(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        with self.binary.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(ValueError, "changed"):
            deploy.apply(fleet, plan)
        self.assertEqual(fleet.operations, [])

    def test_running_soak_blocks_both_mutating_tools_before_staging(self):
        state = inventory()
        state["nodes"][0]["active_workloads"] = [{"pid": 123, "command": ["bash", "vps_cross_region_soak.sh"]}]
        fleet = FakeFleet(self.path, state)
        plan = deploy.make_plan(fleet, state["entry_host"], self.binary)
        with self.assertRaisesRegex(RuntimeError, "workload"):
            deploy.apply(fleet, plan)
        scenario = faults.make_plan(fleet, state["entry_host"], "a3", "node-recover", self.binary)
        with self.assertRaisesRegex(RuntimeError, "workload"):
            faults.Scenario(fleet, scenario).run()
        self.assertEqual(fleet.operations, [])
        self.assertEqual(json.loads((self.path / "result.json").read_text())["outcome"], "stopped")

    def test_stale_topology_is_refused_without_remote_mutations(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        fleet.state["topology_sha256"] = "changed-membership"
        with self.assertRaisesRegex(RuntimeError, "membership changed"):
            deploy.apply(fleet, plan)
        self.assertEqual(fleet.operations, [])

    def test_fault_plan_checks_region_shape_entry_node_and_volume(self):
        fleet = FakeFleet(self.path)
        for case, name in (("node-recover", "a1"), ("volume-no-capacity-cross-region", "a3")):
            with self.assertRaises(ValueError):
                faults.make_plan(fleet, fleet.state["entry_host"], name, case, self.binary)
        plan = faults.make_plan(fleet, fleet.state["entry_host"], "a3", "volume-add-after", self.binary)
        self.assertEqual(plan["volume"]["volume_id"], "volume-a3")
        self.assertEqual(faults.make_plan(fleet, fleet.state["entry_host"], "b1", "node-remove-join", self.binary)["target"], "b1")
        self.assertEqual(fleet.operations, [])

    def test_node_and_volume_faults_cannot_stop_an_indispensable_root_voter(self):
        fleet = FakeFleet(self.path)
        for node in fleet.state["nodes"]:
            node["status"]["root"]["voters"] = [11, 21]
        for case in ("node-recover", "node-remove-join", "volume-add-after"):
            with self.assertRaisesRegex(ValueError, "Root majority"):
                faults.make_plan(fleet, fleet.state["entry_host"], "b1", case, self.binary)

    def test_changed_store_path_and_invalid_plan_id_are_refused(self):
        fleet = FakeFleet(self.path)
        plan = deploy.make_plan(fleet, fleet.state["entry_host"], self.binary)
        current = deepcopy(plan["inventory"])
        current["nodes"][2]["store"] += "-new"
        with self.assertRaisesRegex(RuntimeError, "Store"):
            tools.validate_plan(plan, current)
        plan["run_id"] = "../../etc"
        with self.assertRaises(ValueError):
            tools.validate_plan(plan, plan["inventory"])

    def test_physical_majorities_never_shrink_to_reachable_nodes_or_count_duplicate_nodes(self):
        status = inventory()["nodes"][0]["status"]
        proof = {"nodes": [{"node_id": "node-a1", "held": True}, {"node_id": "node-a1", "held": True},
                           {"node_id": "foreign", "held": True}]}
        self.assertEqual(faults.physical_coverage(proof, status), ([], 2))
        proof["nodes"] += [{"node_id": "node-a2", "held": True}, {"node_id": "node-b1", "held": True},
                           {"node_id": "node-b2", "held": True}]
        self.assertEqual(faults.physical_coverage(proof, status), (["a", "b"], 2))

    def test_credential_is_not_put_in_argv_or_saved_events(self):
        fleet = tools.Fleet("unused-key", self.path)
        secret = "private-one-time-ticket"
        with patch.object(tools, "ssh_json", return_value={"ticket_b64": secret}) as call:
            result = fleet.request("127.0.0.1", "join-service", ticket_b64=secret)
        self.assertEqual(result["ticket_b64"], secret)
        self.assertEqual(len(call.call_args.args), 3)  # host, key, encrypted stdin program; no argv payload
        self.assertNotIn(secret, (self.path / "events.jsonl").read_text())

    def test_managed_paths_refuse_traversal_and_symlink_aliases(self):
        tree = ast.parse(tools.REMOTE)
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "managed_path")
        scope = {"pathlib": pathlib}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "remote-path-fixture", "exec"), scope)
        for path in ("/etc/passwd", "/var/tmp/dubi-test/../../etc", "relative/dubi-path"):
            with self.assertRaises(ValueError):
                scope["managed_path"](path)
        with tempfile.TemporaryDirectory(prefix="dubi-path-test-", dir="/var/tmp") as tmp:
            alias = Path(tmp) / "alias"
            alias.symlink_to("/etc")
            with self.assertRaises(ValueError):
                scope["managed_path"](str(alias))

    def test_private_key_cannot_be_mistaken_for_an_uploadable_binary(self):
        key = self.path / "private-key"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----")
        with self.assertRaisesRegex(ValueError, "ELF"):
            deploy.make_plan(FakeFleet(self.path), "127.0.0.1", key)

    def test_join_options_reuse_confirmed_topology_even_for_minimal_or_stale_units(self):
        tree = ast.parse(tools.REMOTE)
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "server_options")
        scope = {}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "remote-server-fixture", "exec"), scope)
        node = inventory()["nodes"][2]
        for old in (node["server_args"], [*node["server_args"], "--name=old-name", "--region", "old-region"]):
            options = scope["server_options"]({**node, "server_args": old})
            values = dict(zip(options[::2], options[1::2]))
            self.assertEqual(values["--name"], node["name"])
            self.assertEqual(values["--region"], node["region"])
            self.assertEqual(values["--advertise"], node["address"])
            self.assertEqual(values["--listen"], "0.0.0.0:12443")
            self.assertNotIn("--store", values)
        with self.assertRaisesRegex(ValueError, "unsupported server option"):
            scope["server_options"]({**node, "server_args": ["server", "--volume", "/old-volume"]})

    def test_reachable_join_in_wrong_region_is_not_accepted(self):
        scenario = self.scenario("node-remove-join")
        original_inspect = scenario.fleet.inspect
        def wrong_region(host):
            current = original_inspect(host)
            for peer in current["status"]["root"]["peers"]:
                if peer["node_id"] == current["identity"]["node_id"]:
                    peer["region_id"] = "unexpected-region"
            return current
        scenario.fleet.inspect = wrong_region
        with self.assertRaisesRegex(ValueError, "planned name, Region or address"):
            scenario.wait_target()

    def test_fresh_join_waits_for_name_publication_after_admission(self):
        scenario = self.scenario("node-remove-join")
        fresh = deepcopy(scenario.target)
        old_id = fresh["identity"]["node_id"]
        fresh["identity"] = {**fresh["identity"], "node_id": "new-joining-node", "raft_id": 999}
        fresh["status"]["observed_node_id"] = fresh["identity"]["node_id"]
        for peer in fresh["status"]["root"]["peers"]:
            if peer["node_id"] == old_id:
                peer.update(node_id=fresh["identity"]["node_id"], raft_id=999)
        unnamed = deepcopy(fresh)
        for peer in unnamed["status"]["root"]["peers"]:
            if peer["node_id"] == fresh["identity"]["node_id"]:
                peer["name"] = ""
        scenario.fleet.inspect = Mock(side_effect=[unnamed, fresh])
        with patch.object(faults.time, "sleep", lambda _: None):
            ready = scenario.wait_target()
        self.assertEqual(ready["identity"]["node_id"], "new-joining-node")
        self.assertEqual(scenario.fleet.inspect.call_count, 2)

    def test_unsupported_join_unit_is_rejected_during_read_only_planning(self):
        fleet = FakeFleet(self.path)
        fleet.state["nodes"][2]["server_args"] += ["--volume", "/old-volume"]
        with self.assertRaisesRegex(ValueError, "unsupported server option"):
            faults.make_plan(fleet, fleet.state["entry_host"], "a3", "node-remove-join", self.binary)
        self.assertEqual(fleet.operations, [])

    def scenario(self, case):
        fleet = FakeFleet(self.path)
        plan = faults.make_plan(fleet, fleet.state["entry_host"], "a3", case, self.binary)
        scenario = faults.Scenario(fleet, plan)
        scenario.group = "region-a"
        scenario.fixtures = [{"tenant": scenario.tenant, "dataset": "probe", "key": "before", "value": "original", "owner_group_id": "region-a"}]
        scenario.status = lambda: deepcopy(scenario.entry["status"])
        return scenario

    def test_node_recovery_verifies_physical_holdings_before_target_reads(self):
        scenario = self.scenario("node-recover")
        events = []
        scenario.service = lambda action: events.append("service:"+action)
        scenario.reads = lambda server=None: events.append("read:"+str(server))
        def write(key):
            scenario.fixtures.append({**scenario.fixtures[0], "key": key})
            events.append("write:"+key)
        scenario.write = write
        scenario.wait_target = lambda: deepcopy(scenario.target)
        def held(fixture, node, volume_id=None):
            events.append("holding:"+fixture["key"])
            return {"held": True}
        scenario.held = held
        scenario.wait = lambda label, check: check()
        scenario.node_case()
        target_read = events.index("read:"+scenario.target["address"])
        self.assertLess(events.index("holding:before"), target_read)
        self.assertLess(events.index("holding:offline"), target_read)
        self.assertLess(events.index("service:stop"), events.index("write:offline"))

    def test_connection_failure_is_not_counted_as_fencing(self):
        scenario = self.scenario("node-remove-join")
        scenario.service = Mock()
        scenario.reads = Mock(return_value=None)
        scenario.write = Mock()
        status = deepcopy(scenario.target["status"])
        old_id = scenario.target["identity"]["node_id"]
        status["root"]["peers"] = [n for n in status["root"]["peers"] if n["node_id"] != old_id]
        scenario.status = lambda: status
        scenario.cli = lambda node, verb, *a, **kw: {"code": 1, "stderr": "connection refused"} if verb == ["status"] else {}
        with self.assertRaisesRegex(RuntimeError, "explicitly rejected"):
            scenario.node_case()
        self.assertFalse(any(op[0] == "ticket" for op in scenario.fleet.operations))

    def test_fresh_join_cannot_reuse_any_removed_raft_identity(self):
        scenario = self.scenario("node-remove-join")
        original = deepcopy(scenario.target)
        fresh = deepcopy(original)
        fresh["identity"]["node_id"] = "new-node"
        fresh["store_id"] = "new-store"
        fresh["store"] = "/var/lib/dubi-acceptance-" + scenario.plan["run_id"]
        # Retaining the old RaftID must be rejected despite a new NodeID.
        scenario.service = Mock()
        scenario.reads = Mock(return_value=None)
        scenario.write = Mock()
        status = deepcopy(original["status"])
        status["root"]["peers"] = [n for n in status["root"]["peers"] if n["node_id"] != original["identity"]["node_id"]]
        scenario.status = lambda: status
        scenario.cli = lambda node, verb, *a, **kw: {"code": 1, "stderr": "PermissionDenied: fenced identity"} if verb == ["status"] else {}
        scenario.wait_target = lambda: fresh
        scenario.fleet.request = Mock(return_value={"ticket_b64": "fixture"})
        with self.assertRaisesRegex(RuntimeError, "reused a fenced identity"):
            scenario.node_case()
        self.assertFalse(any(c.args[1] == "finish-join" for c in scenario.fleet.request.call_args_list))

    def test_remove_join_preserves_new_identity_and_requires_physical_repair_before_reads(self):
        scenario = self.scenario("node-remove-join")
        original, events = deepcopy(scenario.target), []
        fresh = deepcopy(original)
        fresh["identity"].update(node_id="node-new", raft_id=999)
        fresh["store_id"] = "store-new"
        fresh["store"] = "/var/lib/dubi-acceptance-" + scenario.plan["run_id"]
        fresh["status"]["local_storage"]["volumes"][0]["volume_id"] = "volume-new"
        removed = False
        def status():
            result = deepcopy(original["status"])
            if removed:
                result["root"]["peers"] = [n for n in result["root"]["peers"] if n["node_id"] != original["identity"]["node_id"]]
            return result
        scenario.status = status
        def cli(node, verb, *args, **kwargs):
            nonlocal removed
            if verb == ["node", "remove"]:
                removed = True
                events.append("remove")
                return {}
            return {"code": 1, "stderr": "PermissionDenied: old identity fenced"}
        scenario.cli = cli
        scenario.service = lambda action: events.append(action)
        scenario.reads = lambda server=None: events.append("read-target" if server else "read-survivor")
        scenario.write = lambda key: scenario.fixtures.append({**scenario.fixtures[0], "key": key})
        scenario.wait = lambda label, check: check()
        scenario.wait_target = Mock(side_effect=[fresh, deepcopy(fresh)])
        def held(fixture, node, volume_id=None):
            self.assertEqual(node["identity"]["node_id"], "node-new")
            self.assertEqual(node["store_id"], "store-new")
            events.append("holding:"+fixture["key"])
            return {"held": True}
        scenario.held = held
        def request(host, operation, **args):
            events.append(operation)
            return {"ticket_b64": "private-fixture"} if operation == "ticket" else {}
        scenario.fleet.request = request
        scenario.node_case()
        self.assertLess(events.index("remove"), events.index("join-service"))
        self.assertLess(events.index("holding:offline"), events.index("finish-join"))
        self.assertEqual(events[-1], "read-target")
        self.assertEqual(scenario.target["identity"], fresh["identity"])

    def volume_scenario(self, case, fail_drain=False):
        scenario = self.scenario(case)
        events = []
        original = deepcopy(scenario.target)
        status = deepcopy(original["status"])
        status["cluster"]["state"] = "degraded"
        scenario.status = lambda: status
        scenario.service = lambda action: events.append("service:"+action)
        scenario.wait_target = lambda: original
        scenario.reads = lambda server=None: events.append("read:"+str(server))
        def write(key):
            fixture = {**scenario.fixtures[0], "key": key}
            scenario.fixtures.append(fixture)
            events.append("write:"+key)
            return fixture, {"state": "local_ready"}
        scenario.write = write
        scenario.probe = lambda fixture: {"record_id": "record-test", "nodes": [
            {"node_id": n["identity"]["node_id"], "held": n["identity"]["node_id"] != original["identity"]["node_id"]}
            for n in scenario.plan["inventory"]["nodes"]]}
        scenario.held = lambda fixture, node, volume_id=None: {"held": True, "volume_id": volume_id}
        def wait(label, check):
            events.append("wait:"+label)
            result = check()
            if not result: raise RuntimeError("fixture predicate was not met")
            return result
        scenario.wait = wait
        volumes = []
        def add(path):
            volume_id = "new-volume-"+str(len(volumes))
            volumes.append(volume_id)
            events.append("add:"+volume_id)
            return {"volume_id": volume_id}
        scenario.add_volume = add
        def remove(volume_id):
            events.append("remove:"+volume_id)
            if fail_drain and volume_id.startswith("new-volume"):
                raise RuntimeError("live Pack bytes remain")
        scenario.remove_volume = remove
        def request(host, operation, **args):
            events.append(operation)
            return {"path": "/var/tmp/dubi-fixture/limited-volume", "device": 123} if operation == "limited-volume" else {}
        scenario.fleet.request = request
        scenario.cli = lambda *args, **kw: events.append("drain")
        return scenario, events

    def test_volume_addition_ordering_is_real_and_no_add_phase_precedes_cleanup(self):
        for case in ("volume-add-before", "volume-add-after", "volume-no-add"):
            with self.subTest(case=case):
                scenario, events = self.volume_scenario(case)
                scenario.volume_case()
                added = next(i for i, e in enumerate(events) if e.startswith("add:"))
                if case == "volume-add-before":
                    self.assertLess(added, events.index("offline-volume"))
                elif case == "volume-add-after":
                    self.assertGreater(added, events.index("write:unreadable"))
                else:
                    self.assertGreater(added, events.index("wait:available-without-local-capacity"))
                self.assertEqual(events[-1], "read:"+scenario.target["address"])

    def test_capacity_mount_is_detached_only_after_drain_and_new_holding_verification(self):
        scenario, events = self.volume_scenario("volume-no-capacity")
        scenario.volume_case()
        self.assertLess(events.index("reserve-space"), events.index("write:unreadable"))
        self.assertLess(events.index("wait:available-without-local-capacity"), events.index("release-space"))
        self.assertLess(events.index("remove:new-volume-0"), events.index("detach-limited"))
        self.assertLess(events.index("wait:recovery-volume-holding-unreadable"), events.index("detach-limited"))
        scenario, events = self.volume_scenario("volume-no-capacity", fail_drain=True)
        with self.assertRaisesRegex(RuntimeError, "live Pack bytes"):
            scenario.volume_case()
        self.assertNotIn("detach-limited", events)


if __name__ == "__main__":
    unittest.main()
