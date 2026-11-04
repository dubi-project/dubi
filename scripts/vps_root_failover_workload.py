#!/usr/bin/env python3
"""Finite low-rate VPS workload; retain every result across a planned Root outage."""
import base64
import concurrent.futures
import datetime
import hashlib
import json
import pathlib
import signal
import subprocess
import sys
import threading
import time


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def check_value(output, expected):
    value = json.loads(output)
    if not isinstance(value, dict):
        raise ValueError('invalid read reply')
    if value.get('found') is not True:
        raise ValueError('acknowledged record is missing')
    if base64.b64decode(value.get('value', ''), validate=True) != expected.encode():
        raise ValueError('record bytes mismatch')


def transient(error):
    text = error.lower()
    if any(word in text for word in ('permission', 'unauthenticated', 'unauthorized', 'corrupt', 'data loss', 'dataloss', 'certificate', 'mismatch')):
        return False
    return any(word in text for word in ('deadline', 'timeout', 'timed out', 'unavailable', 'connection refused', 'connection reset', 'no known leader', 'no leader'))


def run(config_path):
    c = json.loads(pathlib.Path(config_path).read_text())
    output = pathlib.Path(c['output']); output.mkdir(mode=0o700, exist_ok=False)
    if not 1 <= c['duration'] <= 900 or len(c['targets']) != 3 or len({n['name'] for n in c['targets']}) != 3:
        raise ValueError('invalid finite workload bounds')
    binary = pathlib.Path(c['binary'])
    if hashlib.sha256(binary.read_bytes()).hexdigest() != c['sha256']:
        raise ValueError('runtime binary changed')
    identity = json.loads((pathlib.Path(c['store'])/'DUBI_NODE_IDENTITY').read_text())
    if any(identity.get(k) != v for k, v in c['identity'].items()):
        raise ValueError('workload identity changed')
    stop = threading.Event(); lock = threading.Lock(); rows = []
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    report = {'started_utc': now(), 'outcome': 'running', 'completed_cycles': {}, 'fatal': []}
    deadline = time.monotonic() + c['duration']

    def save():
        report['utc'] = now()
        tmp = output/'state.tmp'; tmp.write_text(json.dumps(report, indent=2)+'\n'); tmp.replace(output/'state.json')

    def request(node, operation, key, value=None):
        args = [str(binary), 'record', operation, '--store', c['store'], '--server', node['address']]
        if operation == 'put': args += ['--request-id', c['run_id']+'-'+key]
        args += [c['tenant'], c['dataset'], key]
        if operation == 'put': args += [value]
        row = {'started_utc': now(), 'node': node['name'], 'operation': operation, 'key': key, 'expected': value}
        with lock:
            with (output/'intent.jsonl').open('a') as f: f.write(json.dumps(row)+'\n')
        started = time.monotonic()
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=15)
            row.update(code=p.returncode, stdout=p.stdout, stderr=p.stderr)
        except subprocess.TimeoutExpired:
            row.update(code=124, stdout='', stderr='application deadline exceeded after 15 seconds')
        row.update(finished_utc=now(), elapsed_ms=round((time.monotonic()-started)*1000, 3))
        if row['code'] == 0:
            try:
                if operation == 'get': check_value(row['stdout'], value)
                else:
                    if not isinstance(json.loads(row['stdout']), dict): raise ValueError('invalid write reply')
                row['result'] = 'ok'
            except (ValueError, TypeError) as error:
                row.update(result='fatal', validation_error=str(error)); stop.set()
        else:
            row['result'] = 'transient' if transient(row['stderr']) else 'fatal'
            if row['result'] == 'fatal': stop.set()
        with lock:
            rows.append(row)
            with (output/'operations.jsonl').open('a') as f: f.write(json.dumps(row)+'\n')
            if row['result'] == 'fatal': report['fatal'].append(row)
            save()
        return row['result'] == 'ok'

    def worker(node):
        for i in range(1, 61):
            if stop.is_set() or time.monotonic() >= deadline or (output/'stop').exists(): break
            available = next(int(s.split()[1]) for s in pathlib.Path('/proc/meminfo').read_text().splitlines() if s.startswith('MemAvailable:'))
            if available < 180*1024:
                with lock: report['fatal'].append({'error':'MemAvailable below 180 MiB'}); save()
                stop.set(); break
            if request(node, 'get', c['canary']['key'], c['canary']['value']):
                key = node['name']+'-'+str(i); value = 'dubi-root-failover:'+c['run_id']+':'+key
                if request(node, 'put', key, value) and request(node, 'get', key, value):
                    with lock:
                        report['completed_cycles'][node['name']] = report['completed_cycles'].get(node['name'], 0)+1
                        save()
            stop.wait(4)
    save()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for future in [executor.submit(worker, n) for n in c['targets']]: future.result()
        report.update(outcome='failed' if report['fatal'] else 'completed', finished_utc=now(), operations=len(rows))
    except BaseException as error:
        report.update(outcome='failed', error=str(error), finished_utc=now()); raise
    finally:
        save()
    return 1 if report['outcome'] != 'completed' else 0


if __name__ == '__main__':
    raise SystemExit(run(sys.argv[1]))
