#!/usr/bin/env python3
"""Exercise the disposable three-node HTTP console. Never prints tokens/keys."""
import argparse,json,ssl,time,uuid,urllib.request,urllib.error,urllib.parse
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,required=True);p.add_argument('--host',required=True);a=p.parse_args();root=a.directory
admin=json.loads((root/'admin.json').read_text())['token'];monitor=json.loads((root/'monitor.json').read_text())['token'];ctx=ssl.create_default_context();urls=[f'https://{a.host}:{p}' for p in (18444,18446,18448)];results=[]
def rid():return str(uuid.uuid4())
def call(node,path,method='GET',body=None,token=admin,cookie=None,csrf=None,origin=None):
 h={}
 if token:h['Authorization']='Bearer '+token
 if cookie:h['Cookie']=cookie
 if csrf:h['X-Dubi-CSRF']=csrf
 if origin:h['Origin']=origin
 if body is not None:h['Content-Type']='application/json'
 req=urllib.request.Request(urls[node]+path,headers=h,method=method,data=None if body is None else json.dumps(body).encode())
 try:r=urllib.request.urlopen(req,context=ctx,timeout=30)
 except urllib.error.HTTPError as e:r=e
 raw=r.read()
 try:b=json.loads(raw)
 except ValueError:b={}
 return r.status,dict(r.headers),b

def check(label,want,node,path,**kw):
 start=time.monotonic();status,h,b=call(node,path,**kw)
 if status!=want:raise AssertionError((label,status,b.get('code'),b.get('message')))
 results.append({'check':label,'status':status,'ms':round((time.monotonic()-start)*1000,2)})
 return b.get('data',b),h
api='/api/v1/admin/'
for n in range(3):
 for resource in ('credentials','repositories','backups'):
  check(f'{n} reads {resource}',200,n,api+resource)
  check(f'{n} anonymous denied {resource}',401,n,api+resource,token=None)
  check(f'{n} monitor denied {resource}',403,n,api+resource,token=monitor)
# Only inspect the actor's metadata, never persist its secret.
cs,_=check('credential metadata',200,0,api+'credentials');assert 'token_sha256' not in json.dumps(cs)
self_id=next(c['credential_id'] for c in cs['items'] if c['current'])
check('current credential protected',409,1,api+'credentials/'+self_id+'/revoke',method='POST',body={'request_id':rid()})
name='Console QA '+uuid.uuid4().hex[:6]
inp={'request_id':rid(),'name':name,'kind':'admin','ttl_seconds':3600}
created,_=check('issue administrator',200,0,api+'credentials',method='POST',body=inp)
issued=created['token'];cid=created['credential_id']
check('one-time issue cannot replay token',409,2,api+'credentials',method='POST',body=inp)
for n in range(3):
 c,_=check('new name visible across nodes',200,n,api+'credentials/'+cid);assert c['name']==name and c['request_id']==inp['request_id'];assert issued not in json.dumps(c)
 check('new administrator works',200,n,api+'repositories',token=issued)
me,h=check('new administrator login',200,1,'/api/v1/auth/login',token=None,method='POST',body={'token':issued},origin=urls[1]);cookie=h['Set-Cookie'].split(';')[0];csrf=me['csrf_token']
check('cookie mutation needs CSRF',403,2,api+'credentials',token=None,cookie=cookie,origin=urls[2],method='POST',body={'request_id':rid(),'name':'bad-csrf','kind':'monitor','ttl_seconds':3600})
check('cross origin mutation denied',403,2,api+'credentials',token=None,cookie=cookie,csrf=csrf,origin='https://wrong.example',method='POST',body={'request_id':rid(),'name':'bad-origin','kind':'monitor','ttl_seconds':3600})
check('parent revoke through different node',200,2,api+'credentials/'+cid+'/revoke',method='POST',body={'request_id':rid()})
for n in range(3):
 check('revoked admin bearer rejected',401,n,api+'credentials',token=issued)
 check('revoked parent invalidates session',401,n,api+'credentials',token=None,cookie=cookie)
for kind in ['monitor','tenant']:
 tenant=''
 if kind=='tenant':
  tenant='console-qa-'+uuid.uuid4().hex[:8];check('create scoped tenant',200,0,api+'tenants',method='POST',body={'request_id':rid(),'tenant_id':tenant,'name':'控制台接口验收','modules':['config'],'storage_limit_bytes':0})
 token,_=check('issue '+kind,200,1,api+'credentials',method='POST',body={'request_id':rid(),'name':'验收 '+kind,'kind':kind,'tenant_id':tenant,'ttl_seconds':3600})
 check(kind+' cannot administer',403,2,api+'repositories',token=token['token'])
 check(kind+' allowed scope',200,2,'/metrics' if kind=='monitor' else '/api/v1/me',token=token['token'])
 check('revoke '+kind,200,0,api+'credentials/'+token['credential_id']+'/revoke',method='POST',body={'request_id':rid()})
 check('revoked '+kind+' denied',401,2,'/metrics' if kind=='monitor' else '/api/v1/me',token=token['token'])
page,_=check('credential pagination',200,0,api+'credentials?limit=1');assert len(page['items'])==1 and page['next_cursor']
check('cursor binds filter',400,1,api+'credentials?kind=monitor&cursor='+urllib.parse.quote(page['next_cursor']))
seen=[]
order=[]
while True:
 seen += [v['credential_id'] for v in page['items']]
 order += [({'active':0,'expired':1,'revoked':2}[v['status']],-v['created_unix'],-v.get('created_index',0),v['credential_id']) for v in page['items']]
 if not page['next_cursor']:break
 page,_=check('next credential page',200,1,api+'credentials?limit=1&cursor='+urllib.parse.quote(page['next_cursor']))
assert len(seen)==len(set(seen)) and order==sorted(order)
# Persist a useful local repository, with a dedicated directory owned by the demo user.
repos,_=check('repository listing',200,0,api+'repositories')
repo='local';path=str(root/'backups')
if not any(r['name']==repo for r in repos):
 body={'request_id':rid(),'name':repo,'path':path}
 check('default file repository create',200,1,api+'repositories',method='POST',body=body)
 check('file create exact replay',200,2,api+'repositories',method='POST',body=body)
for n in range(3):
 r,_=check('file repository visible',200,n,api+'repositories/'+repo);assert r['path']==path and r['type']=='file'
check('set default repository',200,2,api+'repositories/local/default',method='POST',body={'request_id':rid()})
r,_=check('default persists cross node',200,0,api+'repositories/local');assert r['default']
# S3 encrypted-config path, without inventing access to a cloud account.
s3='qa-s3-'+uuid.uuid4().hex[:6]
body={'request_id':rid(),'name':s3,'type':'s3','bucket':'qa-bucket','endpoint':'https://example.invalid','access_key':'test-access-'+rid(),'secret_key':'test-secret-'+rid()}
check('save encrypted S3 credentials',200,2,api+'repositories',method='POST',body=body)
config,_=check('S3 secrets redacted',200,1,api+'repositories/'+s3);assert config['credentials_configured'] and not any(s in json.dumps(config) for s in [body['access_key'],body['secret_key'],'credentials_nonce','encrypted_credentials'])
update={**body,'request_id':rid(),'revision':config['revision'],'prefix':'updated/','access_key':'','secret_key':''}
check('S3 update preserves credentials',200,0,api+'repositories/'+s3,method='PUT',body=update)
check('stale S3 update rejected',409,1,api+'repositories/'+s3,method='PUT',body={**update,'request_id':rid(),'prefix':'stale/'})
updated,_=check('updated S3 revision',200,2,api+'repositories/'+s3);assert updated['credentials_configured'] and updated['revision']!=config['revision']
# Two physical backups establish a usable recovery point and incremental reuse.
job_ids=[]
for iteration in range(2):
 request={'request_id':rid(),'repository':'local'}
 job,_=check('start cluster backup',200,iteration,api+'backups',method='POST',body=request);jid=job['job_id'];job_ids.append(jid)
 again,_=check('backup exact replay',200,2,api+'backups',method='POST',body=request);assert again['job_id']==jid
 end=time.monotonic()+300
 while True:
  status,_,b=call(2,api+'backups/'+jid);j=b.get('data',{})
  if status!=200:raise AssertionError(('backup polling',status,b.get('code')))
  if j['state']=='completed':break
  if j['state']=='failed' or time.monotonic()>end:raise AssertionError(('backup not complete',j['state'],[(w['node_id'],w['state']) for w in j['workers']]))
  time.sleep(2)
 assert j['completed_nodes']==j['total_nodes']==3 and j['created_unix']>0
 results.append({'check':'cluster backup completed','job_id':jid,'nodes':j['completed_nodes'],'bytes':j['bytes'],'objects':j['objects']})
 prep,_=check('restore preparation',200,1,api+'backups/'+jid+'/restore-preparation');assert prep['target']=='new-empty-cluster' and len(prep['backup']['workers'])==3
r,_=check('repository completed count',200,0,api+'repositories/local');assert r['completed_backups']>=2
check('cannot redirect recovery point destination',409,0,api+'repositories/local',method='PUT',body={'request_id':rid(),'name':'local','type':'file','path':str(root/'other'),'revision':r['revision']})
first,_=check('newest backup first',200,0,api+'backups?repository=local&limit=1');assert first['items'][0]['job_id']==job_ids[-1]
second,_=check('backup next page',200,2,api+'backups?repository=local&limit=1&cursor='+urllib.parse.quote(first['next_cursor']));assert second['items'][0]['job_id']==job_ids[-2]
(root/'operations-acceptance.json').write_text(json.dumps({'checks':results,'s3_config_test_repository':s3,'jobs':job_ids},ensure_ascii=False,indent=2))
print(json.dumps({'passed':len(results),'backup_jobs':job_ids,'s3_config_test_repository':s3}))
