#!/usr/bin/env python3
"""Fault fixtures for the physical observer; execute on the acceptance VPS."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, call, patch

from vps_cluster_tools_test import inventory
from vps_cluster_tools import fingerprint
import vps_soak_watch as watcher


class WatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.inventory = inventory()
        self.entry = self.inventory['nodes'][0]
        self.status = self.entry['status']
        self.status['local_repair'].update(repair_debt=0)
        self.config = {'run_dir': str(self.root/'workload'), 'output': str(self.root/'physical'),
                       'nodes': self.inventory['nodes'], 'entry': self.entry, 'store': self.entry['store'],
                       'tenant': 'tenant', 'dataset': 'objects', 'owner_group_id': 'group-test',
                       'sha256': 'candidate', 'probe': '/fixture/probe', 'probe_sha256': 'probe',
                       'topology_sha256': fingerprint(self.status)}
        self.path = self.root/'config.json'
        self.path.write_text(json.dumps(self.config))
        self.watch = watcher.Watch(self.path)
        self.run_dir = Path(self.config['run_dir'])
        self.run_dir.mkdir()
        end = datetime.now(timezone.utc)-timedelta(minutes=10)
        start = end-timedelta(hours=6)
        self.info = {'started_utc': start.isoformat(), 'key_epoch': 'test-epoch',
                     'tenant': 'tenant', 'dataset': 'objects', 'store': self.entry['store'],
                     'duration_seconds': '21600'}
        self.final = {'finished_utc': end.isoformat(), 'exit_code': '0', 'termination_reason': 'duration'}
        self.write_info()
        self.rows = [dict(utc=end.isoformat(), event='put', iteration=str(i), result='ok') for i in (1,2,3)]
        (self.run_dir/'metrics.tsv').write_text('utc\tevent\titeration\tresult\n' + ''.join(
            '\t'.join(r[k] for k in ('utc','event','iteration','result'))+'\n' for r in self.rows))
        self.proof = {'nodes': [{'node_id': n['identity']['node_id'], 'store_id': n['store_id'], 'held': True}
                                for n in self.inventory['nodes']]}

    def write_info(self, **extra):
        (self.run_dir/'run.info').write_text(''.join(f'{k}={v}\n' for k,v in {**self.info, **extra}.items()))

    def response(self, value):
        return subprocess.CompletedProcess([], 0, json.dumps(value), '')

    def run_watch(self, responses, child=None):
        with patch.object(self.watch, 'verify_binaries'), patch.object(watcher.subprocess, 'run', side_effect=responses):
            self.watch.run(child)

    def result(self):
        return json.loads((self.watch.output/'result.json').read_text())

    def test_transient_peer_error_retains_exact_snapshot_and_stops_owned_workload(self):
        bad = deepcopy(self.status)
        peer = bad['cluster']['repair_observations'][4]
        peer.update(reachable=False, error='rpc error: DeadlineExceeded: original peer response')
        child = Mock(pid=12345)
        child.poll.return_value = None
        child.wait.return_value = 143
        with patch.object(watcher.os, 'killpg') as kill:
            with self.assertRaisesRegex(RuntimeError, 'original peer response'):
                self.run_watch([self.response(bad)], child)
        kill.assert_called_once_with(12345, signal.SIGTERM)
        self.assertEqual(json.loads((self.watch.output/'0001-status.stdout').read_text()), bad)
        self.assertEqual(self.result()['phase'], 'health')
        self.assertIn(peer['node_id'], self.result()['error'])
        self.assertEqual(self.result()['outcome'], 'failed')
        self.assertTrue(self.result()['owns_workload'])
        self.assertTrue((self.watch.output/'workload-stop.json').exists())

    def test_timeout_and_malformed_output_remain_available_as_original_bytes(self):
        cases = [subprocess.TimeoutExpired(['fixture'], 90, output=b'partial-json', stderr=b'deadline'),
                 subprocess.CompletedProcess([], 0, '{invalid json', 'fixture warning')]
        for i, response in enumerate(cases):
            with self.subTest(i=i), patch.object(watcher.subprocess, 'run', side_effect=[response]):
                with self.assertRaises((RuntimeError, ValueError)):
                    self.watch.call('status', ['fixture'])
            prefix = self.watch.output/f'{i+1:04d}-status'
            self.assertTrue(prefix.with_suffix('.stdout').read_text())
            self.assertTrue(prefix.with_suffix('.stderr').read_text())
            self.assertTrue(prefix.with_suffix('.json').exists())

    def test_healthy_final_run_checks_first_middle_and_last_writes(self):
        self.write_info(**self.final)
        self.run_watch([self.response(self.status)] + [self.response(self.proof)]*3)
        result = self.result()
        self.assertEqual(result['outcome'], 'passed')
        self.assertFalse(result['owns_workload'])
        self.assertEqual([s['iteration'] for s in result['checks'][0]['samples']], [1,2,3])

    def test_wrong_store_missing_member_and_unreplicated_data_fail_closed(self):
        variants = [deepcopy(self.proof) for _ in range(3)]
        variants[0]['nodes'][0]['store_id'] = 'retired-store'
        variants[1]['nodes'].pop()
        for node in variants[2]['nodes'][3:]:
            node['held'] = False
        for i, proof in enumerate(variants):
            with self.subTest(i=i), patch.object(watcher.time, 'sleep'):
                with self.assertRaises(RuntimeError):
                    self.run_watch([self.response(self.status)] + [self.response(proof)]*3)
                self.assertEqual(self.result()['outcome'], 'failed')

    def test_non_geo_faults_and_final_geo_debt_still_rejected(self):
        changes = [lambda s: s['cluster'].update(root_quorum_confirmed=False),
                   lambda s: s['local_storage']['volumes'][0].update(health='missing'),
                   lambda s: s['cluster']['repair_observations'][3].update(repair_debt=1),
                   lambda s: s['cluster'].update(state='degraded', reasons=['Geo repair has 1 pending item(s)'])]
        for mutate in changes:
            bad = deepcopy(self.status)
            mutate(bad)
            self.write_info(**self.final)
            with self.subTest(status=bad), self.assertRaises(RuntimeError):
                self.run_watch([self.response(bad)])
            self.assertEqual(self.result()['outcome'], 'failed')

    def test_early_completion_and_failed_requests_cannot_pass_as_long_soak(self):
        self.write_info(**{**self.final, 'termination_reason':'iteration_limit'})
        with self.assertRaisesRegex(RuntimeError, 'full duration'):
            self.run_watch([])
        self.write_info(**self.final)
        with (self.run_dir/'metrics.tsv').open('a') as stream:
            stream.write(self.final['finished_utc']+'\tget\t3\tfail\n')
        with self.assertRaisesRegex(RuntimeError, 'failed operation'):
            self.run_watch([])

    def test_existing_evidence_is_never_reused_or_rewritten(self):
        (self.watch.output/'result.json').write_text('{"outcome":"failed"}')
        with self.assertRaises(FileExistsError):
            watcher.Watch(self.path)
        self.assertEqual(self.result()['outcome'], 'failed')

    def test_forced_child_cleanup_is_recorded_and_does_not_signal_other_pids(self):
        child = Mock(pid=54321)
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired(['workload'],45), -9]
        with patch.object(watcher.os, 'killpg') as kill:
            watcher.stop_workload(child, self.watch.output)
        self.assertEqual(kill.call_args_list, [call(54321,signal.SIGTERM), call(54321,signal.SIGKILL)])
        self.assertTrue(json.loads((self.watch.output/'workload-stop.json').read_text())['forced_kill'])

    def test_real_child_is_stopped_on_guard_failure_but_unrelated_process_survives(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'], start_new_session=True)
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'], start_new_session=True)
        try:
            bad = deepcopy(self.status)
            bad['cluster']['repair_observations'][5].update(reachable=False, error='fixture disconnect')
            with self.assertRaisesRegex(RuntimeError, 'fixture disconnect'):
                self.run_watch([self.response(bad)], child)
            self.assertIsNotNone(child.poll())
            self.assertIsNone(unrelated.poll())
            self.assertFalse(json.loads((self.watch.output/'workload-stop.json').read_text()).get('forced_kill', False))
        finally:
            for process in (child, unrelated):
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
