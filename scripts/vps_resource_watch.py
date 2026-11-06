#!/usr/bin/env python3
"""Finite, local read-only resource/version samples for one VPS soak Node."""
import argparse
import hashlib
import http.client
import socket
import ssl
import json
from pathlib import Path
import signal
import subprocess
import time
from datetime import datetime, timezone


def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()


def utc():return datetime.now(timezone.utc).isoformat()


def system_counters():
    """Cumulative host counters distinguish CPU steal, I/O stalls and TCP loss.

    No packet payloads, environment variables or credentials are collected.
    Missing kernel interfaces are evidence, not a reason to lose the sample.
    """
    values={}
    for name in ('stat','diskstats','net/dev','net/snmp','net/netstat',
                 'pressure/cpu','pressure/io','pressure/memory'):
        try:values[name]=Path('/proc',name).read_text()
        except OSError as e:values[name]={'error':str(e)}
    return values


def memory_metrics(store, port):
    """Read the existing local HTTPS endpoint without exposing heap contents."""
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError('invalid local metrics port')
    context=ssl.create_default_context(cafile=str(Path(store)/'dubi-node-pki/node-ca.crt'))
    connection=http.client.HTTPSConnection('dubi.internal',port,context=context,timeout=10)
    try:
        connection.sock=context.wrap_socket(socket.create_connection(('127.0.0.1',port),timeout=10),server_hostname='dubi.internal')
        connection.request('GET','/metrics');response=connection.getresponse()
        body=response.read(1024*1024+1)
        if response.status!=200 or len(body)>1024*1024:raise RuntimeError('local metrics unavailable or oversized')
        values={line.split()[0]:int(line.split()[1]) for line in body.decode().splitlines() if line.startswith('dubi_go_')}
        if len(values)!=10:raise RuntimeError('incomplete Go memory metrics')
        return values
    finally:connection.close()


def run(config):
    c=json.loads(Path(config).read_text());out=Path(c['output']);out.mkdir(mode=0o700)
    if not 180 <= c['duration_seconds'] <= 25200:raise ValueError('invalid bounded duration')
    report={'outcome':'running','started_utc':utc(),'node':c['name'],'samples':0}
    def save():
        p=out/'result.tmp';p.write_text(json.dumps(report,indent=2));p.replace(out/'result.json')
    def stop(signum,frame):raise RuntimeError('sampling interrupted: '+str(signum))
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    deadline=time.monotonic()+c['duration_seconds'];save()
    try:
        while True:
            sample_started=time.monotonic()
            p=subprocess.run(['systemctl','show','dubi','-p','MainPID','-p','NRestarts','-p','MemoryCurrent','-p','ActiveState'],capture_output=True,text=True,timeout=10)
            if p.returncode:raise RuntimeError(p.stderr)
            unit=dict(line.split('=',1) for line in p.stdout.splitlines())
            pid=unit['MainPID']
            sample={'utc':utc(),'monotonic_seconds':sample_started,'unit':unit,'meminfo':Path('/proc/meminfo').read_text(),'loadavg':Path('/proc/loadavg').read_text(),'system_counters':system_counters()}
            if int(pid)>0:
                sample['process_status']=Path('/proc/'+pid+'/status').read_text()
                sample['process_io']=Path('/proc/'+pid+'/io').read_text()
                sample['binary_sha256']=digest('/proc/'+pid+'/exe')
                if c.get('metrics_port') is not None:
                    metrics_started=time.monotonic()
                    try:sample['go_memory']=memory_metrics(c['store'],c['metrics_port'])
                    except Exception as e:sample['metrics_error']=str(e)
                    sample['metrics_seconds']=time.monotonic()-metrics_started
            sample['sample_seconds']=time.monotonic()-sample_started
            with (out/'samples.jsonl').open('a') as f:f.write(json.dumps(sample)+'\n')
            report['samples']+=1;report['last_sample_utc']=sample['utc'];save()
            if 'metrics_error' in sample:
                raise RuntimeError('local metrics failed: '+sample['metrics_error'])
            if unit['ActiveState']!='active' or pid!=str(c['pid']) or sample.get('binary_sha256')!=c['sha256']:
                raise RuntimeError('server unavailable, restarted or fixed binary changed')
            identity=json.loads((Path(c['store'])/'DUBI_NODE_IDENTITY').read_text())
            if identity['node_id']!=c['node_id']:raise RuntimeError('Node identity changed')
            if time.monotonic()>=deadline:break
            time.sleep(min(30,max(0,deadline-time.monotonic())))
        report['outcome']='completed'
    except BaseException as e:
        report.update(outcome='failed',error=str(e));raise
    finally:
        report['finished_utc']=utc();save()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('config');run(p.parse_args().config)
