#!/usr/bin/env python3
"""Finite data-addressing migration soak. Run on a VPS, never the developer host.

This is a stability run, not the six-fault production qualification workflow.
No SSH keys, service changes, membership changes or fault injection are used.
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from vps_cluster_tools import fingerprint, require_healthy, sha256
from vps_foundation_faults import physical_coverage
from vps_soak_diagnostics import geo_wait_health, utc_now
from vps_soak_watch import atomic_json


def verify_value(reply, value):
    if reply.get('found') is not True or reply.get('value') != base64.b64encode(value.encode()).decode():
        raise ValueError('read value differs from the exact Tenant/Key fixture')


def verify_holding(proof, status, nodes):
    expected = {n['identity']['node_id']: n for n in nodes}
    seen = [n['node_id'] for n in proof['nodes']]
    if len(seen) != len(set(seen)) or set(seen) != set(expected):
        raise ValueError('physical observations omit or duplicate Nodes')
    for item in proof['nodes']:
        n = expected[item['node_id']]
        if item['held']:
            volumes = {v['volume_id'] for v in n['status']['local_storage']['volumes'] if v['state'] == 'active'}
            if item['store_id'] != n['store_id'] or item['volume_id'] not in volumes:
                raise ValueError('physical holding belongs to another Store/Volume')
        elif item.get('error') != 'rpc error: code = NotFound desc = record is not held on this Node':
            raise ValueError('physical observation failed: ' + item.get('error', 'missing error'))
    covered, required = physical_coverage(proof, status)
    if len(covered) < required:
        raise ValueError('record older than three minutes lacks a physical Region majority')
    return covered


class Soak:
    def __init__(self, path):
        self.path = Path(path)
        self.c = json.loads(self.path.read_text())
        c = self.c
        if c['duration_seconds'] < 180 or c['duration_seconds'] > 21600:
            raise ValueError('duration must be 180..21600 seconds')
        if c['interval_seconds'] < 15 or c['new_scope_seconds'] < 180:
            raise ValueError('workload rate exceeds this low-rate acceptance scope')
        if len(c['nodes']) != 9 or len({n['region'] for n in c['nodes']}) != 3:
            raise ValueError('requires nine Nodes in three Regions')
        self.out = Path(c['output'])
        self.out.mkdir(mode=0o700)  # Never overwrite an earlier run.
        self.number = 0
        self.pending = []
        self.sentinels = []
        self.scopes = []
        self.report = {'outcome': 'running', 'started_utc': utc_now(), 'phase': 'preflight',
                       'binary_sha256': c['sha256'], 'config_sha256': sha256(path),
                       'duration_seconds': c['duration_seconds'], 'writes': 0, 'reads': 0,
                       'tenants': 0, 'ranges': 0, 'idempotent_retries': 0,
                       'physical_checks': 0, 'cycles': 0,
                       'qualification': 'migration stability only; no fault injection or backup/restore'}
        self.save()

    def save(self):
        atomic_json(self.out/'result.json', self.report)

    def call(self, label, args, payload=None, timeout=100):
        self.number += 1
        prefix = self.out/f'{self.number:06d}-{label}'
        started = time.monotonic()
        row = {'utc': utc_now(), 'operation': label, 'args': args}
        try:
            p = subprocess.run(args, input=json.dumps(payload) if payload is not None else None,
                               capture_output=True, text=True, timeout=timeout)
            stdout, stderr = p.stdout, p.stderr
            row['exit_code'] = p.returncode
        except subprocess.TimeoutExpired as e:
            stdout, stderr = e.stdout or '', e.stderr or ''
            stdout = stdout.decode(errors='replace') if isinstance(stdout, bytes) else stdout
            stderr = stderr.decode(errors='replace') if isinstance(stderr, bytes) else stderr
            row['timeout_seconds'] = timeout
        row['elapsed_ms'] = round((time.monotonic()-started)*1000)
        prefix.with_suffix('.stdout').write_text(stdout)
        prefix.with_suffix('.stderr').write_text(stderr)
        atomic_json(prefix.with_suffix('.json'), row)
        with (self.out/'metrics.jsonl').open('a') as f:
            f.write(json.dumps(row)+'\n')
        self.report['last_command'] = prefix.name
        self.save()
        if row.get('exit_code') != 0:
            raise RuntimeError(f'{label} failed; original output: {prefix.name}')
        return json.loads(stdout)

    def cli(self, label, node, *args):
        # Flags precede positional arguments in Dubi's CLI.
        cmd = [self.c['binary'], *args[:2], '--store', self.c['store'], '--server', node['address'], *args[2:]]
        return self.call(label, cmd, timeout=200)

    def binaries(self):
        c = self.c
        for path, expected in [(c['binary'], c['sha256']), (c['probe'], c['probe_sha256'])]:
            if sha256(path) != expected:
                raise ValueError('accepted executable changed: ' + path)
        for name, expected in c['tools_sha256'].items():
            if Path(name).name != name or sha256(self.path.parent/name) != expected:
                raise ValueError('accepted test tool changed: ' + name)
        pid = subprocess.check_output(['systemctl', 'show', 'dubi', '-p', 'MainPID', '--value'], text=True).strip()
        if not pid.isdigit() or int(pid) <= 0 or sha256('/proc/'+pid+'/exe') != c['sha256']:
            raise ValueError('running local server differs from fixed candidate')
        mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        available = int(mem['MemAvailable'].split()[0])*1024
        stat = os.statvfs(c['store'])
        resource = {'utc': utc_now(), 'mem_available_bytes': available,
                    'disk_available_bytes': stat.f_bavail*stat.f_frsize,
                    'server_status': Path('/proc/'+pid+'/status').read_text()}
        with (self.out/'resources.jsonl').open('a') as f:
            f.write(json.dumps(resource)+'\n')
        if available < 180*1024*1024 or resource['disk_available_bytes'] < 1024**3:
            raise ValueError('acceptance stopped for low memory or disk capacity')

    def health(self, final=False):
        self.binaries()
        deadline = time.monotonic()+180
        while True:
            s = self.call('status', [self.c['binary'], 'status', '--store', self.c['store'], '--server', self.c['entry']['address']])
            if fingerprint(s) != self.c['topology_sha256']:
                raise ValueError('cluster topology changed during fixed-topology soak')
            pending = geo_wait_health(s, self.c['entry']['status'])
            if not pending:
                require_healthy(s)
                return s
            # Pause our writes while already committed Geo work settles. Every
            # intermediate response is retained; hard faults are never retried.
            if time.monotonic() >= deadline:
                raise ValueError('Geo debt did not settle within 180 seconds')
            self.report['phase'] = 'geo-settling'
            self.save()
            time.sleep(10)

    def write(self, node, tenant, key, value, request, label):
        result = self.cli(label, node, 'record', 'put', '--request-id', request, tenant, key, value)
        if result.get('state') not in ('local_ready', 'geo_ready') or not result.get('applied_index') or not result.get('range_id') or not result.get('owner_group_id'):
            raise ValueError('write did not return confirmed Range/Owner publication')
        self.report['writes'] += 1
        return {'tenant': tenant, 'key': key, 'value': value, 'put': result,
                'written_monotonic': time.monotonic(), 'written_utc': utc_now()}

    def read(self, node, fixture, label='get'):
        value = self.cli(label, node, 'record', 'get', fixture['tenant'], fixture['key'])
        verify_value(value, fixture['value'])
        self.report['reads'] += 1

    def create_scopes(self, generation):
        c = self.c
        fresh = []
        for region in sorted({n['region'] for n in c['nodes']}):
            local = sorted([n for n in c['nodes'] if n['region'] == region], key=lambda n:n['name'])
            node = local[generation % len(local)]
            tenant = f"{c['run_id']}-{generation}-{region}"
            self.cli('tenant-create', node, 'tenant', 'create', '--request-id', 'create-'+tenant, tenant)
            self.report['tenants'] += 1
            # Same Key and caller RequestID across scopes, distinct payloads.
            f = self.write(node, tenant, 'same-key', 'value:'+tenant, 'same-request', 'first-put')
            other = next(n for n in c['nodes'] if n['region'] != region)
            retry = self.cli('idempotent-retry', other, 'record', 'put', '--request-id', 'same-request', tenant, f['key'], f['value'])
            if any(retry.get(k) != f['put'][k] for k in ('applied_index', 'range_id', 'owner_group_id')):
                raise ValueError('cross-node retry changed original publication')
            self.report['idempotent_retries'] += 1
            fresh.append(f)
            self.pending.append(f)
            self.sentinels.append(f)
            self.report['ranges'] += 1
            atomic_json(self.out/'fixtures.json', self.sentinels)
            self.save()
        if len({f['put']['range_id'] for f in fresh}) != 3 or len({f['put']['owner_group_id'] for f in fresh}) != 3:
            raise ValueError('new Tenant scopes share a Range or fail to select regional Owners')
        if not self.scopes:
            self.scopes = fresh

    def physical(self, status, final=False):
        # Dedicated sentinels have never been logically read. Probe first so
        # read-through fetches cannot be mistaken for background replication.
        due = [f for f in self.pending if time.monotonic()-f['written_monotonic'] >= 180]
        for f in due:
            payload = {k:f[k] for k in ('tenant','key','value')}
            payload.update(status=status, owner_group_id=f['put']['owner_group_id'])
            proof = self.call('physical', [self.c['probe'], '--store', self.c['store']], payload)
            covered = verify_holding(proof, status, self.c['nodes'])
            with (self.out/'coverage.jsonl').open('a') as out:
                out.write(json.dumps({'utc':utc_now(), 'tenant':f['tenant'], 'covered_regions':covered,
                                      'record_id':proof['record_id'], 'final':final})+'\n')
            self.report['physical_checks'] += 1
            for region in sorted({n['region'] for n in self.c['nodes']}):
                self.read(next(n for n in self.c['nodes'] if n['region']==region), f, 'sentinel-get')
            self.pending.remove(f)

    def run(self):
        started = time.monotonic()
        deadline = started+self.c['duration_seconds']
        try:
            self.health()
            self.create_scopes(0)
            generation = 1
            next_scope = started+self.c['new_scope_seconds']
            next_health = time.monotonic()+60
            i = 0
            while time.monotonic() < deadline:
                cycle = time.monotonic()
                if cycle >= next_scope:
                    self.create_scopes(generation);generation += 1
                    next_scope = time.monotonic()+self.c['new_scope_seconds']
                if time.monotonic() >= next_health:
                    s = self.health();self.physical(s)
                    # Check historical keys as well as newest writes.
                    eligible = [f for f in self.sentinels if f not in self.pending]
                    if eligible:self.read(self.c['nodes'][i % 9], eligible[i % len(eligible)], 'historical-get')
                    next_health = time.monotonic()+60
                self.report['phase'] = 'traffic'
                node = self.c['nodes'][i % 9]
                scope = self.scopes[(i//9) % 3]
                f = self.write(node, scope['tenant'], 'row-'+str(i), self.c['run_id']+':'+str(i), 'row-'+str(i), 'put')
                for region in sorted({n['region'] for n in self.c['nodes']}):
                    peers = [n for n in self.c['nodes'] if n['region']==region]
                    self.read(peers[i % len(peers)], f)
                i += 1
                self.report.update(cycles=i, elapsed_seconds=round(time.monotonic()-started,1))
                self.save()
                time.sleep(max(0,min(deadline-time.monotonic(),self.c['interval_seconds']-(time.monotonic()-cycle))))
            self.report['phase'] = 'final-drain';self.save()
            while self.pending:
                time.sleep(max(0,180-(time.monotonic()-min(f['written_monotonic'] for f in self.pending))))
                self.physical(self.health(),final=True)
            self.health(final=True)
            if i < 9 or len(self.scopes)!=3 or self.report['physical_checks']!=self.report['ranges']:
                raise ValueError('incomplete entry/scope/physical coverage')
            self.report.update(outcome='passed', phase='finished')
        except BaseException as e:
            self.report.update(outcome='failed', error=str(e), error_type=type(e).__name__)
            # Preserve a bounded journal without changing the server.
            try:
                p=subprocess.run(['journalctl','-u','dubi','-n','800','--no-pager'],capture_output=True,text=True,timeout=15)
                (self.out/'incident-journal.txt').write_text(p.stdout+p.stderr)
            except Exception:pass
            raise
        finally:
            self.report.update(finished_utc=utc_now(),elapsed_seconds=round(time.monotonic()-started,1))
            self.save()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('config');args=p.parse_args()
    with open('/run/dubi-vps-soak.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        soak=Soak(args.config)
        def stop(signum, frame):raise RuntimeError('soak interrupted by signal '+str(signum))
        signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
        soak.run()


if __name__=='__main__':main()
