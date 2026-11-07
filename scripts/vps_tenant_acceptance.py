#!/usr/bin/env python3
"""Tenant/quota acceptance on the existing disposable three-node Gateway only.
Secrets stay in memory and are never printed or included in the report.
"""
import argparse,base64,concurrent.futures,json,ssl,subprocess,time,urllib.error,urllib.parse,urllib.request,uuid
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,required=True);p.add_argument('--host',required=True);p.add_argument('--restart',action='store_true');args=p.parse_args()
root=args.directory;admin=json.loads((root/'admin.json').read_text())['token'];monitor=json.loads((root/'monitor.json').read_text())['token'];context=ssl.create_default_context();urls=[f'https://{args.host}:{port}' for port in (18444,18446,18448)];results=[]
def rid():return str(uuid.uuid4())
def call(node,path,method='GET',body=None,token=admin,cookie=None,csrf=None,origin=None):
 headers={}
 if token:headers['Authorization']='Bearer '+token
 if cookie:headers['Cookie']=cookie
 if csrf:headers['X-Dubi-CSRF']=csrf
 if origin:headers['Origin']=origin
 if body is not None:headers['Content-Type']='application/json'
 req=urllib.request.Request(urls[node]+path,method=method,headers=headers,data=None if body is None else json.dumps(body).encode())
 try:res=urllib.request.urlopen(req,context=context,timeout=70)
 except urllib.error.HTTPError as e:res=e
 data=res.read();return res.status,dict(res.headers),json.loads(data) if data else {}
def check(label,want,node,path,**kwargs):
 start=time.monotonic();status,headers,body=call(node,path,**kwargs)
 if status!=want:raise AssertionError(f'{label}: expected {want}, got {status}, code={body.get("code")}, message={body.get("message")}')
 results.append({'check':label,'status':status,'elapsed_ms':round((time.monotonic()-start)*1000,2)})
 return body.get('data',body),headers
suffix=uuid.uuid4().hex[:8];tenant='acceptance-'+suffix;path='/api/v1/admin/tenants';detail=path+'/'+tenant
for node in range(3):
 check(f'node {node} tenant list',200,node,path)
 check(f'node {node} anonymous denied',401,node,path,token=None)
 check(f'node {node} monitor denied',403,node,path,token=monitor)
mods,_=check('available modules',200,0,'/api/v1/admin/tenant-modules');assert mods==[{'id':'config','name':'配置中心'}]
create={'request_id':rid(),'tenant_id':tenant,'name':'验收租户 '+suffix,'modules':['config'],'storage_limit_bytes':100}
t,_=check('create tenant with quota',200,0,path,method='POST',body=create);assert t['used_bytes']==0 and t['reserved_bytes']==0 and t['accounting']
check('create exact retry',200,2,path,method='POST',body=create)
check('duplicate tenant',409,1,path,method='POST',body={**create,'request_id':rid()})
for bad in ({**create,'request_id':rid(),'tenant_id':tenant+'-bad','modules':['cache']},{**create,'request_id':rid(),'tenant_id':tenant+'-bad','name':''},{**create,'request_id':rid(),'tenant_id':tenant+'-bad','module_id':'config'}):check('invalid create',400,0,path,method='POST',body=bad)
for node in range(3):
 got,_=check(f'node {node} sees tenant settings',200,node,detail);assert got['name']==create['name'] and got['storage_limit_bytes']==100
save={**create,'request_id':rid(),'revision':t['revision'],'name':'名称已更新 '+suffix}
t,_=check('update name',200,1,detail,method='PUT',body=save)
check('update exact retry',200,2,detail,method='PUT',body=save)
check('stale version rejected',409,0,detail,method='PUT',body={**save,'request_id':rid(),'name':'stale'})
issued,_=check('issue tenant credential',200,0,detail+'/credentials',method='POST',body={'request_id':rid(),'ttl_seconds':3600});application=issued['token'];credential=issued['credential_id'];assert issued['expires_unix']>time.time()
check('Tenant cannot administer',403,1,path,token=application)
for node in range(3):
 got,_=check('credential verifier metadata only',200,node,detail);assert got['active_credentials']==1 and 'token' not in json.dumps(got) and 'sha256' not in json.dumps(got)
def business(key,method='GET',value=None,request_id=None,version=None,node=0):
 body=None
 if method!='GET':
  body={'request_id':request_id or rid()}
  if method=='PUT':body['value']=base64.b64encode(value).decode()
  if version is not None:body['version']=version
 return call(node,'/api/v1/config/entry?key='+urllib.parse.quote(key),method,body,application)
def usage(want,label='logical usage',reserved=0):
 deadline=time.monotonic()+45
 for node in range(3):
  while True:
   status,_,body=call(node,detail);got=body.get('data',body)
   if status==200 and got.get('used_bytes')==want and got.get('reserved_bytes')==reserved:break
   if time.monotonic()>deadline:raise AssertionError((label,node,got.get('used_bytes'),want))
   time.sleep(.25)
  results.append({'check':label+f' node {node}','used_bytes':want,'reserved_bytes':reserved})
 time.sleep(1.1) # Let each entry's local cache observe the acknowledged total.
def write(key,value,expected=200,request_id=None,node=0,version=None):
 start=time.monotonic();status,_,b=business(key,'PUT',value,request_id,version,node)
 if status!=expected:raise AssertionError(f'write {key}: {status} {b.get("code")} {b.get("message")}')
 results.append({'check':'Config write '+key,'status':status,'elapsed_ms':round((time.monotonic()-start)*1000,2)})
 return b
request=rid();first=write('a',b'123456789',request_id=request);usage(10)
write('a',b'123456789',request_id=request,node=2);usage(10,'same request not charged twice')
write('a',b'changed',expected=409,request_id=request,node=1)
write('a',b'larger-than-old-value',expected=409,version='0'*64);usage(10,'failed CAS does not change usage')
write('a',b'1',node=1);usage(2,'overwrite shrinks usage')
# Eight independent keys, each 20 logical bytes, compete for 98 remaining bytes.
def concurrent_write(i):
 key='k'+str(i);request=rid();status,_,body=business(key,'PUT',b'x'*18,request_id=request,node=i%3)
 # Preserve operation identity on any uncertain transport/server result.
 for attempt in range(3):
  if status!=503:break
  time.sleep(1);status,_,body=business(key,'PUT',b'x'*18,request_id=request,node=(i+attempt)%3)
 return key,status,body.get('code')
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:out=list(pool.map(concurrent_write,range(8)))
assert all(status==200 or status==409 and code=='quota_exceeded' for _,status,code in out),out
successful=[key for key,status,_ in out if status==200];assert 4<=len(successful)<=8,out
results.append({'check':'concurrent soft quota permits bounded test burst','accepted':len(successful),'rejected':len(out)-len(successful)})
used=2+len(successful)*20;usage(used,'concurrent quota converges to exact usage')
write('post-sync',b'x'*20,expected=409)
current,_=check('read latest settings',200,0,detail)
def update(**fields):
 global current
 body={'request_id':rid(),'tenant_id':tenant,'name':current['name'],'modules':current['modules'],'storage_limit_bytes':current['storage_limit_bytes'],'revision':current['revision'],**fields}
 current,_=check('update tenant policy',200,1,detail,method='PUT',body=body)
 time.sleep(1.1)
update(storage_limit_bytes=1)
write('new',b'x',expected=409)
status,_,value=business('a',node=2);assert status==200 and value['value']==base64.b64encode(b'1').decode()
for key in successful+['a']:
 status,_,value=business(key,'DELETE',node=2);assert status==200,(status,value.get('code'))
usage(0,'delete allowed above quota and frees usage')
update(storage_limit_bytes=128)
write('persistent',b'persisted',node=2);usage(19)
update(modules=[])
status,_,value=business('persistent');assert status==403,value
results.append({'check':'existing credential obeys module revocation','status':status})
update(modules=['config'])
status,_,value=business('persistent',node=1);assert status==200
# Browser writes require CSRF and the same origin.
login,headers=check('browser login',200,0,'/api/v1/auth/login',method='POST',token=None,body={'token':admin},origin=urls[0]);cookie=headers['Set-Cookie'].split(';',1)[0];csrf=login['csrf_token']
check('cookie write without CSRF',403,0,path,method='POST',body=create,token=None,cookie=cookie,origin=urls[0])
check('cookie write wrong origin',403,0,path,method='POST',body=create,token=None,cookie=cookie,csrf=csrf,origin='https://wrong.example')
check('cookie authorized exact retry',200,0,path,method='POST',body=create,token=None,cookie=cookie,csrf=csrf,origin=urls[0])
check('logout',200,0,'/api/v1/auth/logout',method='POST',token=None,cookie=cookie,csrf=csrf,origin=urls[0])
# Filtered cursor cannot be reused after changing its query.
page,_=check('name search',200,0,path+'?q='+urllib.parse.quote(current['name'])+'&limit=1');assert page['items'][0]['tenant_id']==tenant
check('duplicate filter rejected',400,0,path+'?status=active&status=inactive')
check('bad cursor rejected',400,0,path+'?cursor=invalid')
if args.restart:
 subprocess.run(['systemctl','restart','dubi-gateway-demo-1','dubi-gateway-demo-2','dubi-gateway-demo-3'],check=True)
 deadline=time.monotonic()+60
 while time.monotonic()<deadline:
  try:
   if all(call(i,detail)[0]==200 for i in range(3)):break
  except Exception:pass
  time.sleep(2)
 else:raise AssertionError('demo cluster did not recover')
 usage(19,'quota and tenant policy survive restart')
 status,_,value=business('persistent',node=1);assert status==200 and value['value']==base64.b64encode(b'persisted').decode()
 results.append({'check':'Config data survives all demo nodes restart','status':status})
check('revoke application credential',200,2,detail+'/credentials/'+credential,method='DELETE',body={'request_id':rid()})
status,_,value=business('persistent');assert status==401
check('repeat revocation',200,0,detail+'/credentials/'+credential,method='DELETE',body={'request_id':rid()})
report={'tenant_id':tenant,'checks':len(results),'results':results,'completed_unix':int(time.time())};print(json.dumps(report,ensure_ascii=False,indent=2))
