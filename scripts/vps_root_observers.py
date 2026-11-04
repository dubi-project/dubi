"""Bounded read-only Root phase observers for one explicitly scoped VPS run."""
import collections, hashlib, json, pathlib
from vps_cluster_tools import sha256
from vps_soak_diagnostics import ssh_json

def observer_work_directory(evidence, node):
    # Independent runs commonly use the same child directory basename.
    # Include the complete local run identity and the durable Node identity;
    # preserve exclusive creation so retries cannot overwrite old evidence.
    run = hashlib.sha256(str(pathlib.Path(evidence).resolve()).encode()).hexdigest()
    member = hashlib.sha256(node['identity']['node_id'].encode()).hexdigest()
    return '/var/tmp/dubi-root-observer-' + run[:24] + '-' + member[:24]

class ObserverSet:
    def __init__(self, fleet, evidence, helper):
        self.fleet, self.evidence, self.helper = fleet, evidence / 'root-observers', helper
        self.evidence.mkdir(exist_ok=False)
        self.started = []
        self.checksum = sha256(helper)

    def start(self, node):
        if any(x['node_id'] == node['identity']['node_id'] for x in self.started):
            return
        work = observer_work_directory(self.evidence, node)
        # Never overwrite a shared executable that another observer may be
        # running (Linux rejects that with ETXTBSY). Isolate the binary too.
        remote = work + '-probe'
        self.fleet.copy(self.helper, node['host'], remote)
        self.fleet.request(node['host'], 'stage-check', expected=node, staged=remote, sha256=self.checksum)
        request = {'helper': remote, 'sha256': self.checksum, 'store': node['store'],
                   'node_id': node['identity']['node_id'], 'work': work, 'status': node['status']}
        program = r'''import subprocess,pathlib,json,hashlib,datetime
r=REQUEST
assert hashlib.sha256(pathlib.Path(r['helper']).read_bytes()).hexdigest()==r['sha256']
assert json.loads((pathlib.Path(r['store'])/'DUBI_NODE_IDENTITY').read_text())['node_id']==r['node_id']
work=pathlib.Path(r['work']);work.mkdir(mode=0o700,exist_ok=False)
(work/'input.json').write_text(json.dumps({'status':r['status']}))
with (work/'input.json').open()as inp,(work/'samples.jsonl').open('x')as out,(work/'stderr.txt').open('x')as err:
 p=subprocess.Popen([r['helper'],'--store',r['store'],'--workers','2','--duration','60m'],stdin=inp,stdout=out,stderr=err,start_new_session=True,close_fds=True)
v={'pid':p.pid,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'work':r['work'],'helper':r['helper'],'store':r['store'],'node_id':r['node_id'],'finite':True,'duration_seconds':3600}
(work/'process.json').write_text(json.dumps(v));print(json.dumps(v))
'''.replace('r=REQUEST', 'r=' + repr(request))
        result = ssh_json(node['host'], self.fleet.key, program)
        result.update(host=node['host'], name=node['name'])
        self.started.append(result)
        (self.evidence / 'processes.json').write_text(json.dumps(self.started, indent=2) + '\n')

    def finish(self):
        summaries = []
        for item in self.started:
            program = r'''import pathlib,json,os,signal,time
r=REQUEST
p=pathlib.Path('/proc')/str(r['pid'])/'cmdline'
if p.exists() and p.read_bytes():
 args=p.read_bytes().decode().strip('\0').split('\0')
 if args[0]!=r['helper'] or '--store'not in args or args[args.index('--store')+1]!=r['store']:raise RuntimeError('observer PID changed; refuse signal')
 os.kill(r['pid'],signal.SIGTERM)
 deadline=time.monotonic()+12
 while p.exists() and p.read_bytes() and time.monotonic()<deadline:time.sleep(.1)
 if p.exists() and p.read_bytes():raise RuntimeError('observer did not drain within its request deadline')
work=pathlib.Path(r['work']);samples=[];invalid=[]
raw=(work/'samples.jsonl').read_text()
if len(raw)>24*1024*1024:raise RuntimeError('observer output exceeds finite bound')
for i,line in enumerate(raw.splitlines()):
 try:samples.append(json.loads(line))
 except ValueError:invalid.append(i+1)
print(json.dumps({'samples':samples,'invalid_lines':invalid,'stderr':(work/'stderr.txt').read_text()[-2000:]}))
'''.replace('r=REQUEST', 'r=' + repr(item))
            try:
                result = ssh_json(item['host'], self.fleet.key, program)
                (self.evidence / (item['name'] + '.json')).write_text(json.dumps(result, indent=2) + '\n')
                rows = result['samples']
                errors = [r for r in rows if r.get('error')]
                summaries.append({'node': item['name'], 'samples': len(rows),
                                  'phases': dict(collections.Counter(r['phase'] for r in rows)),
                                  'errors': errors,
                                  'max_rpc_ms': max((r['rpc_ms'] for r in rows), default=0),
                                  'max_apply_ms': max((r['apply_ms'] for r in rows), default=0),
                                  'invalid_lines': result['invalid_lines']})
            except Exception as error:
                summaries.append({'node': item['name'], 'collection_error': str(error)})
        (self.evidence / 'summary.json').write_text(json.dumps(summaries, indent=2) + '\n')
        return summaries
