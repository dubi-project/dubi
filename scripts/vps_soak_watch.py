#!/usr/bin/env python3
"""Finite VPS physical sampling, with optional ownership of its request workload."""
import argparse
import csv
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from vps_cluster_tools import fingerprint, require_healthy, sha256
from vps_cluster_deploy import require_rollout_start
from vps_foundation_faults import physical_coverage


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def stop_workload(process, output):
    """Only stop the child we created; never signal an unverified PID from disk."""
    if process is None or process.poll() is not None:
        return
    result = {'pid': process.pid, 'signal': 'SIGTERM', 'started_utc': now()}
    os.killpg(process.pid, signal.SIGTERM)
    try:
        result['exit_code'] = process.wait(timeout=45)
    except subprocess.TimeoutExpired:
        result['forced_kill'] = True
        os.killpg(process.pid, signal.SIGKILL)
        result['exit_code'] = process.wait(timeout=10)
    atomic_json(output / 'workload-stop.json', result)


class Watch:
    def __init__(self, config_path):
        self.config_path = Path(config_path)
        self.config = json.loads(self.config_path.read_text())
        self.output = Path(self.config['output'])
        # Refuse old evidence before creating children or writing reports.
        self.output.mkdir(mode=0o700)
        self.run_dir = Path(self.config['run_dir'])
        self.expected = {n['identity']['node_id']: n for n in self.config['nodes']}
        if len(self.expected) != len(self.config['nodes']):
            raise ValueError('duplicate configured Node identity')
        self.report = {'started_utc': now(), 'outcome': 'running', 'checks': [],
                       'config_sha256': sha256(self.config_path), 'grace_seconds': 180,
                       'scope': 'physical sampling; standalone checks do not replace continuous acceptance',
                       'owns_workload': False}
        self.command_number = 0

    def save(self):
        atomic_json(self.output / 'result.json', self.report)

    def call(self, label, args, payload=None, timeout=90):
        """Save the original response before parsing it or applying a gate."""
        self.command_number += 1
        prefix = self.output / f'{self.command_number:04d}-{label}'
        record = {'started_utc': now(), 'args': [str(a) for a in args]}
        try:
            response = subprocess.run(args, input=json.dumps(payload) if payload is not None else None,
                                      capture_output=True, text=True, timeout=timeout)
            record['exit_code'] = response.returncode
            stdout, stderr = response.stdout, response.stderr
        except subprocess.TimeoutExpired as error:
            record['timeout_seconds'] = timeout
            stdout, stderr = error.stdout or '', error.stderr or ''
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors='replace')
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors='replace')
        (prefix.with_suffix('.stdout')).write_text(stdout)
        (prefix.with_suffix('.stderr')).write_text(stderr)
        record['finished_utc'] = now()
        atomic_json(prefix.with_suffix('.json'), record)
        self.report['last_command'] = str(prefix.name)
        self.save()
        if record.get('exit_code') != 0:
            raise RuntimeError(f'{label} failed; see {prefix.name}: {stderr[-2000:]}')
        return json.loads(stdout)

    def verify_binaries(self):
        c = self.config
        if sha256(c.get('binary', '/usr/local/bin/dubi')) != c['sha256'] or sha256(c['probe']) != c['probe_sha256']:
            raise RuntimeError('workload/probe binary changed during acceptance')
        pid = subprocess.check_output(['systemctl', 'show', 'dubi', '-p', 'MainPID', '--value'], text=True).strip()
        if not pid.isdigit() or int(pid) == 0 or sha256('/proc/' + pid + '/exe') != c['sha256']:
            raise RuntimeError('running server differs from the accepted candidate')

    def sample(self, info, writes, final):
        c = self.config
        self.report.update(phase='status', successful_writes=len(writes))
        self.save()
        status = self.call('status', [c.get('binary', '/usr/local/bin/dubi'), 'status', '--store', c['store']])
        if fingerprint(status) != c['topology_sha256']:
            raise RuntimeError('cluster topology changed during physical sampling')
        self.report['phase'] = 'health'
        self.save()
        require_rollout_start({'nodes': [{**c['entry'], 'status': status}]}, True)
        if final:
            require_healthy(status)
        cutoff = datetime.now(timezone.utc).timestamp() - 180
        eligible = [r for r in writes if datetime.fromisoformat(r['utc'].replace('Z', '+00:00')).timestamp() <= cutoff]
        indices = [0, len(eligible)-1] if not final else [0, len(eligible)//2, len(eligible)-1]
        samples = []
        for index in sorted(set(indices)):
            row = eligible[index]
            iteration = int(row['iteration'])
            fixture = {'status': status, 'tenant': c['tenant'],
                       'key': f"soak-{info['key_epoch']}-{iteration}",
                       'value': f"dubi-cross-region-soak:{info['key_epoch']}:{iteration}",
                       'owner_group_id': c['owner_group_id']}
            self.report.update(phase='holding', iteration=iteration)
            self.save()
            for attempt in range(3):
                proof = self.call(f'holding-{iteration}-{attempt}', [c['probe'], '--store', c['store']], fixture)
                if len(proof['nodes']) != len(self.expected) or {n['node_id'] for n in proof['nodes']} != set(self.expected):
                    raise RuntimeError('physical observations omit/duplicate a configured Node')
                for node in proof['nodes']:
                    if node['held'] and node['store_id'] != self.expected[node['node_id']]['store_id']:
                        raise RuntimeError('holding belongs to an unexpected Store')
                    if not node['held'] and any(x in node.get('error', '').lower() for x in
                                               ('permissiondenied', 'unauthenticated', 'identity mismatch')):
                        raise RuntimeError('physical probe authorization or identity failed')
                covered, required = physical_coverage(proof, status)
                detail = {'utc': now(), 'iteration': iteration, 'covered_regions': covered,
                          'required_regions': required, 'proof': proof, 'final': bool(final)}
                atomic_json(self.output / f'holding-{iteration}-{self.command_number:04d}.json', detail)
                if len(covered) >= required:
                    samples.append({k: detail[k] for k in ('iteration', 'covered_regions', 'required_regions')})
                    break
                if attempt == 2:
                    raise RuntimeError(f'write {iteration} older than three minutes lacks a physical Region majority')
                time.sleep(15)
        check = {'utc': now(), 'successful_writes': len(writes), 'eligible_writes': len(eligible),
                 'cluster_state': status['cluster']['state'], 'samples': samples, 'final': final}
        self.report['checks'].append(check)
        self.report['phase'] = 'sampling'
        self.save()
        print(json.dumps(check), flush=True)

    def run(self, workload=None):
        deadline = time.monotonic() + 25200
        next_sample = 0
        self.report['owns_workload'] = workload is not None
        self.save()
        try:
            while time.monotonic() < deadline:
                self.report['phase'] = 'workload'
                info_path = self.run_dir / 'run.info'
                if not info_path.exists():
                    if workload is not None and workload.poll() is not None:
                        raise RuntimeError('workload exited before creating run.info')
                    if time.monotonic() > deadline - 25200 + 60:
                        raise RuntimeError('workload did not initialize within one minute')
                    time.sleep(1)
                    continue
                info = dict(line.split('=', 1) for line in info_path.read_text().splitlines() if '=' in line)
                if any(info.get(k) != self.config[k] for k in ('store', 'tenant')):
                    raise RuntimeError('workload identity or Keyspace differs from the accepted configuration')
                self.verify_binaries()
                with (self.run_dir / 'metrics.tsv').open() as stream:
                    rows = list(csv.DictReader(stream, delimiter='\t'))
                if any(r.get('result') == 'fail' for r in rows):
                    raise RuntimeError('request workload recorded a failed operation')
                writes = [r for r in rows if r.get('event') == 'put' and r.get('result') == 'ok']
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(info['started_utc'].replace('Z', '+00:00'))).total_seconds()
                if age > 120 and not writes:
                    raise RuntimeError('no successful writes after two minutes')
                self.report['successful_writes'] = len(writes)
                finished = 'finished_utc' in info and 'exit_code' in info
                if finished and info['exit_code'] != '0':
                    raise RuntimeError('request workload did not finish successfully')
                if workload is not None and workload.poll() is not None and not finished:
                    raise RuntimeError('workload exited without a final report')
                if finished:
                    elapsed = (datetime.fromisoformat(info['finished_utc'].replace('Z', '+00:00')) -
                               datetime.fromisoformat(info['started_utc'].replace('Z', '+00:00'))).total_seconds()
                    if info.get('termination_reason') != 'duration' or elapsed < int(info['duration_seconds']):
                        raise RuntimeError('request workload finished before its full duration')
                    if workload is not None and workload.poll() is None:
                        # The shell may still be generating its final summary.
                        time.sleep(2)
                        continue
                    if workload is not None and workload.returncode != 0:
                        raise RuntimeError('workload process returned failure after its final report')
                cutoff = datetime.now(timezone.utc).timestamp() - 180
                eligible = [r for r in writes if datetime.fromisoformat(r['utc'].replace('Z', '+00:00')).timestamp() <= cutoff]
                final = bool(finished and writes and len(eligible) == len(writes))
                if eligible and (final or time.monotonic() >= next_sample):
                    self.sample(info, writes, final)
                    next_sample = time.monotonic() + 300
                if final:
                    self.report.update(outcome='passed', phase='finished')
                    return
                self.save()
                time.sleep(2 if finished else 5)
            raise RuntimeError('finite physical sampling deadline elapsed')
        except BaseException as error:
            self.report.update(outcome='failed', error=str(error), error_type=type(error).__name__)
            self.save()
            raise
        finally:
            self.report['finished_utc'] = now()
            self.save()
            if self.report['outcome'] != 'passed':
                stop_workload(workload, self.output)


def start_workload(watch):
    c, tools = watch.config, watch.config_path.parent
    if watch.run_dir.exists():
        raise FileExistsError('workload evidence already exists')
    # Bind this new run to the same preflight and immutable staged tool set.
    accepted = json.loads((tools / 'acceptance-passed.json').read_text())
    if (accepted['outcome'] != 'passed' or accepted['binary_sha256'] != c['sha256'] or
            len(accepted['fixtures']) != 9 or accepted.get('logical_reads') != 90 or
            len(accepted.get('coverage', {})) != 9 or len(accepted.get('fault_cases', [])) != 6 or
            any(case.get('state') != 'passed' for case in accepted['fault_cases'])):
        raise RuntimeError('exact-candidate functional and nine-entry preflight evidence is required')
    for name, expected in json.loads((tools / 'soak-tools-sha256.json').read_text()).items():
        if Path(name).name != name or sha256(tools / name) != expected:
            raise RuntimeError('test tool checksum mismatch: ' + name)
    watch.verify_binaries()
    status = watch.call('baseline-status', [c.get('binary', '/usr/local/bin/dubi'), 'status', '--store', c['store']])
    require_healthy(status)
    if fingerprint(status) != c['topology_sha256']:
        raise RuntimeError('topology changed after acceptance')
    env = {**os.environ, 'DUBI_BINARY': c.get('binary', '/usr/local/bin/dubi'), 'DUBI_STORE': c['store'],
           'DUBI_SOAK_TENANT': c['tenant'],
           'DUBI_SOAK_DURATION_SECONDS': '21600', 'DUBI_SOAK_INTERVAL_SECONDS': '30',
           'DUBI_SOAK_STATUS_INTERVAL_SECONDS': '90', 'DUBI_SOAK_ALLOW_GEO_DEGRADED': '1',
           'DUBI_SOAK_MAX_ITERATIONS': '0', 'DUBI_SOAK_DIR': c['run_dir'],
           'DUBI_SOAK_DIAGNOSTICS': str(tools / 'vps_soak_diagnostics.py')}
    with (watch.output / 'workload-launch.log').open('xb') as log:
        child = subprocess.Popen(['bash', str(tools / 'vps_cross_region_soak.sh')], env=env, cwd=tools,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, close_fds=True)
    return child


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('--start-workload', action='store_true', help='own one new six-hour workload; stop it if physical checks fail')
    args = parser.parse_args()
    lock = None
    if args.start_workload:
        lock = open('/run/dubi-vps-soak.lock', 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    watch = Watch(args.config)
    child = None
    def interrupted(signum, frame):
        raise RuntimeError(f'physical observer received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        watch.save()
        if args.start_workload:
            child = start_workload(watch)
            atomic_json(watch.config_path.parent / 'phase.json', {
                'stage': 'running', 'started_utc': now(), 'workload_pid': child.pid,
                'physical_pid': os.getpid(), 'run_dir': str(watch.run_dir), 'physical_output': str(watch.output)})
        watch.run(child)
    except BaseException as error:
        if watch.report['outcome'] != 'failed':
            watch.report.update(outcome='failed', error=str(error), error_type=type(error).__name__, finished_utc=now())
            watch.save()
        stop_workload(child, watch.output)
        raise
    finally:
        if lock is not None:
            lock.close()


if __name__ == '__main__':
    main()
