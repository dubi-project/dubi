#!/usr/bin/env python3
"""Bounded notification acceptance on the disposable three-process console only.
Uses existing credentials in memory; never prints tokens. Does not touch SSH config.
"""
import json, ssl, time, uuid, urllib.request, urllib.error, urllib.parse, subprocess
from pathlib import Path
base=Path('/var/tmp/dubi-http-demo-20261106')
admin=json.loads((base/'admin.json').read_text())['token'];ctx=ssl.create_default_context()
urls=[f'https://209.200.246.159:{p}' for p in (18444,18446,18448)]
created=[]
def rid():return str(uuid.uuid4())
def call(path,method='GET',body=None,node=0,token=admin,want=200,extra=None):
 headers={'Content-Type':'application/json'}
 if token:headers['Authorization']='Bearer '+token
 if extra:headers.update(extra)
 q=urllib.request.Request(urls[node]+path,method=method,headers=headers,data=json.dumps(body).encode() if body is not None else None)
 try:r=urllib.request.urlopen(q,context=ctx,timeout=20)
 except urllib.error.HTTPError as e:r=e
 data=json.loads(r.read())
 if r.status!=want:raise AssertionError((path,r.status,data.get('message')))
 return data.get('data',data),dict(r.headers)
api='/api/v1/admin/'
def notices(node=0,token=admin):return call(api+'notifications?limit=200',node=node,token=token)[0]
def wait_for(label,predicate,seconds=100):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  try:
   result=predicate()
   if result:print('PASS '+label,flush=True);return result
  except (urllib.error.URLError,TimeoutError,AssertionError):pass
  time.sleep(3)
 raise AssertionError('timed out: '+label)
def event_for(kind,resource,level=None):
 return next((n for n in notices()['items'] if n['kind']==kind and n['resource_id']==resource and (level is None or n['level']==level)),None)
def issue(kind,name,ttl):
 data,_=call(api+'credentials','POST',{'request_id':rid(),'kind':kind,'name':name,'ttl_seconds':ttl},node=1);created.append(data['credential_id']);return data
try:
 for n in range(3):call(api+'notifications',node=n)
 a=issue('admin','消息验收管理员 A',30*86400);b=issue('admin','消息验收管理员 B',30*86400);m=issue('monitor','消息验收到期提醒',3600)
 notice=wait_for('automatic credential expiry event without browser',lambda:event_for('credential',m['credential_id'],'warning'))
 event_id=notice['id']
 call(api+'notifications',token=m['token'],want=403)
 mutation={'request_id':rid(),'id':event_id,'read':True}
 call(api+'notifications/read','POST',mutation,node=0,token=a['token']);call(api+'notifications/read','POST',mutation,node=2,token=a['token'])
 for n in range(3):
  view,_=call(api+'notifications/'+event_id,node=n,token=a['token']);assert view['read']
  other,_=call(api+'notifications/'+event_id,node=n,token=b['token']);assert other['read']
 print('PASS cross-node read state, cluster-wide sharing and exact retry',flush=True)
 session,h=call('/api/v1/auth/login','POST',{'token':a['token']},node=1,token=None,extra={'Origin':urls[1]})
 cookie=h['Set-Cookie'].split(';')[0];mutation={'request_id':rid(),'id':event_id,'read':False}
 call(api+'notifications/read','POST',mutation,node=1,token=None,want=403,extra={'Cookie':cookie,'Origin':urls[1]})
 call(api+'notifications/read','POST',mutation,node=1,token=None,extra={'Cookie':cookie,'Origin':urls[1],'X-Dubi-CSRF':session['csrf_token']})
 view,_=call(api+'notifications/'+event_id,node=2,token=a['token']);assert not view['read']
 print('PASS browser session uses cluster read state and enforces CSRF',flush=True)
 target,_=call(api+'gateway',node=2);target_id=target['node_id']
 subprocess.run(['systemctl','stop','dubi-gateway-demo-3'],check=True)
 try:
  failure=wait_for('automatic unavailable-node message',lambda:event_for('node',target_id,'error'),120)
 finally:subprocess.run(['systemctl','start','dubi-gateway-demo-3'],check=True)
 wait_for('automatic node recovery message',lambda:event_for('node',target_id,'success'),120)
 # Wait a full detector cycle to verify stable problems are not re-announced.
 time.sleep(35)
 same=[n for n in notices()['items'] if n['kind']=='credential' and n['resource_id']==m['credential_id'] and n['level']=='warning']
 assert len(same)==1
 print('PASS stable alert deduplication across detector cycles',flush=True)
 repos,_=call(api+'repositories');repo=next((r['name'] for r in repos if r['type']=='file'),None)
 if repo:
  job,_=call(api+'backups','POST',{'request_id':rid(),'repository':repo})
  # Operations returns the cluster-owned job metadata.
  job_id=job.get('job_id') or job.get('id')
  if not job_id:raise AssertionError('missing backup job identity')
  wait_for('automatic backup terminal notification',lambda:event_for('backup',job_id),150)
 print('PASS notifications acceptance completed',flush=True)
finally:
 subprocess.run(['systemctl','start','dubi-gateway-demo-3'],check=False)
 for cid in created:
  try:call(api+'credentials/'+cid+'/revoke','POST',{'request_id':rid()})
  except Exception:print('cleanup needed for temporary credential '+cid,flush=True)
