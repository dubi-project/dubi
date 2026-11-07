#!/usr/bin/env python3
"""Run only on the disposable TT01 console (three HTTP entries, three-region cluster).
Creates two isolated fixture tenants and four accounts. Preserves the user's admin.
Passwords stay in a mode-0600 file on TT01; the report never contains credentials.
Leave fixtures for browser QA; Config and issued business credential records are removed.
Run serially before browser QA: password/role checks deliberately invalidate sessions.
"""
import json,secrets,time,uuid,urllib.request,urllib.error,urllib.parse,base64,os
from pathlib import Path
base=Path('/var/tmp/dubi-http-demo-20261106');operator=json.loads((base/'admin.json').read_text())['token']
urls=[f'https://209.200.246.159:{p}' for p in (18444,18446,18448)]
report=[]
def rid():return str(uuid.uuid4())
def call(path,method='GET',body=None,node=0,token=None,session=None,want=200):
 h={'Content-Type':'application/json','Origin':urls[node]}
 if token:h['Authorization']='Bearer '+token
 if session:h.update(Cookie=session['cookie'],**{'X-Dubi-CSRF':session['csrf_token']})
 req=urllib.request.Request(urls[node]+path,method=method,headers=h,data=None if body is None else json.dumps(body).encode())
 start=time.monotonic()
 try:r=urllib.request.urlopen(req,timeout=45)
 except urllib.error.HTTPError as e:r=e
 raw=r.read().decode();d=json.loads(raw)
 if r.status!=want:raise AssertionError((method,path,r.status,d.get('code'),d.get('message')))
 assert 'password_hash' not in raw and 'password_salt' not in raw
 report.append({'method':method,'path':path,'status':r.status,'ms':round((time.monotonic()-start)*1000)})
 return d.get('data',d),dict(r.headers)
def login(a,node=0):
 d,h=call('/api/v1/auth/login','POST',{'username':a['username'],'password':a['password']},node=node);d['cookie']=h['Set-Cookie'].split(';')[0];return d
def account(tenant,role,name):
 pwd=secrets.token_urlsafe(18);a,_=call('/api/v1/admin/accounts','POST',{'request_id':rid(),'tenant_id':tenant,'username':name,'name':{'admin':'租户管理员','readwrite':'读写成员','readonly':'只读成员'}[role],'role':role,'password':pwd},token=operator);a['password']=pwd;return a
def update(a,changes,want=200):
 fresh,_=call('/api/v1/admin/accounts/'+a['id'],token=operator)
 return call('/api/v1/admin/accounts/'+a['id'],'PATCH',{'request_id':rid(),'revision':fresh['revision'],**changes},token=operator,want=want)[0]
suffix=secrets.token_hex(3);tenant='portal-'+suffix;other=tenant+'-other'
for t in (tenant,other):call('/api/v1/admin/tenants','POST',{'request_id':rid(),'tenant_id':t,'name':'租户控制台验收'+(' B' if t==other else ' A'),'modules':['config'],'storage_limit_bytes':10485760},token=operator)
a=account(tenant,'admin','tenant-admin-'+suffix);b=account(other,'admin','tenant-other-'+suffix);w=account(tenant,'readwrite','tenant-writer-'+suffix);v=account(tenant,'readonly','tenant-reader-'+suffix)
# Persist credentials privately so browser QA and later user preview can use the same fixtures.
private={'url':urls[0],'tenant':tenant,'other_tenant':other,'accounts':{'admin':a,'other':b,'readwrite':w,'readonly':v}}
fd=os.open('/var/tmp/dubi-tenant-console-acceptance.json',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:json.dump(private,f)
sa=login(a);sb=login(b,1);sw=login(w,2);sv=login(v,1)
for i in range(3):
 me,_=call('/api/v1/me',session=sa,node=i);assert me['tenant_id']==tenant and me['role']=='admin'
for s in (sa,sw,sv):
 for path in ('/api/v1/admin/accounts','/api/v1/admin/cluster','/api/v1/admin/notifications','/metrics'):call(path,session=s,want=403)
 d,_=call('/api/v1/tenant/overview',session=s);assert d['tenant']['tenant_id']==tenant
for s in (sw,sv):
 for p in ('accounts','credentials'):call('/api/v1/tenant/'+p,session=s,want=403)
for path in ('accounts','credentials'):
 call('/api/v1/tenant/'+path+'?tenant_id='+other,session=sa,want=400)
call('/api/v1/tenant/accounts/'+b['id'],session=sa,want=404)
call('/api/v1/tenant/accounts/'+b['id'],'DELETE',{'request_id':rid(),'revision':b['revision']},session=sa,want=404)
call('/api/v1/admin/accounts','POST',{'request_id':rid(),'tenant_id':other,'username':a['username'],'name':'冲突','role':'admin','password':secrets.token_urlsafe(18)},token=operator,want=409)
update(a,{'disabled':True},409);update(a,{'tenant_id':other},400)
print('PASS shared login, three entry nodes, tenant isolation, roles and last-admin protection',flush=True)
key='portal.verification';cfg='/api/v1/tenant/config/entry?key='+key
create={'request_id':rid(),'if_absent':True,'value':base64.b64encode('租户配置内容'.encode()).decode()}
x,_=call(cfg,'PUT',create,session=sw,node=2)
x,_=call(cfg,session=sv,node=1);assert x['value']==create['value']
call(cfg,session=sb,want=404)
call(cfg,'PUT',{**create,'request_id':rid()},session=sv,want=403)
call(cfg+'&tenant='+other,session=sa,want=400)
y,_=call(cfg,'PUT',{'request_id':rid(),'version':x['version'],'value':base64.b64encode(b'updated').decode()},session=sw)
z,_=call(cfg,session=sv);assert z['value']==base64.b64encode(b'updated').decode()
creds='/api/v1/tenant/credentials'
c,_=call(creds,'POST',{'request_id':rid(),'name':'只读业务验收','permission':'readonly','modules':['config'],'never_expires':True},session=sa)
call('/api/v1/config/entry?key='+key,token=c['token'])
call('/api/v1/config/entry?key='+key,'PUT',{'request_id':rid(),'value':base64.b64encode(b'forbidden').decode()},token=c['token'],want=403)
call(creds+'/'+c['credential_id'],session=sb,want=404)
call(creds+'/'+c['credential_id']+'/revoke','POST',{'request_id':rid()},session=sa)
call('/api/v1/config/entry?key='+key,token=c['token'],want=401)
call(creds+'/'+c['credential_id'],'DELETE',{'request_id':rid()},session=sa)
call(creds,'POST',{'request_id':rid(),'name':'越权模块','permission':'readwrite','modules':['search'],'never_expires':True},session=sa,want=400)
call(cfg,'DELETE',{'request_id':rid(),'version':z['version']},session=sw,node=1)
call(cfg,session=sv,want=404)
print('PASS actual Config CRUD, cross-tenant isolation, readonly business token, revocation and deletion',flush=True)
update(w,{'role':'readonly'});call('/api/v1/me',session=sw,node=2,want=401)
update(w,{'role':'readwrite'})
profile,_=call('/api/v1/tenant/profile',session=sv)
profile,_=call('/api/v1/tenant/profile','PATCH',{'request_id':rid(),'revision':profile['revision'],'name':'只读成员'},session=sv)
newpwd=secrets.token_urlsafe(18)
call('/api/v1/tenant/password','POST',{'request_id':rid(),'revision':profile['revision'],'current_password':v['password'],'password':newpwd},session=sv)
call('/api/v1/me',session=sv,node=2,want=401)
v['password']=newpwd;private['accounts']['readonly']=v
with open('/var/tmp/dubi-tenant-console-acceptance.json','w') as f:json.dump(private,f)
sv=login(v)
update(v,{'disabled':True});call('/api/v1/me',session=sv,want=401);update(v,{'disabled':False})
for scope in ('system','tenant'):
 page,_=call('/api/v1/admin/accounts?scope='+scope,token=operator);assert all(bool(i['tenant_id'])==(scope=='tenant') for i in page['items'])
print('PASS role/password/disable invalidates sessions; profile and system account classification',flush=True)
Path('/var/tmp/dubi-tenant-console-report.json').write_text(json.dumps({'checks':report,'count':len(report),'tenant':tenant},ensure_ascii=False,indent=2))
print(json.dumps({'passed':True,'checks':len(report),'tenant':tenant}),flush=True)
