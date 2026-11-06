#!/usr/bin/env python3
import argparse,concurrent.futures,json,ssl,subprocess,urllib.request,urllib.error
from pathlib import Path
parser=argparse.ArgumentParser(description="Verify three existing disposable VPS Gateway nodes; creates and revokes one temporary admin token.")
parser.add_argument("--directory",type=Path,required=True,help="private demo directory with binary and credential files")
parser.add_argument("--host",required=True,help="certificate-covered public host")
parser.add_argument("--user",default="dubi-http-demo",help="existing service user")
args=parser.parse_args()
base=args.directory
urls=['https://'+args.host+':'+str(p) for p in [18444,18446,18448]]
admin=json.loads((base/'admin.json').read_text())
monitor=json.loads((base/'monitor.json').read_text())
tenant=json.loads((base/'credential.json').read_text())
ctx=ssl.create_default_context()
results=[]
def call(node,path,token=None,method='GET',body=None,cookie=None,origin=None,csrf=None):
 headers={}
 if token:headers['Authorization']='Bearer '+token
 if cookie:headers['Cookie']=cookie
 if origin:headers['Origin']=origin
 if csrf:headers['X-Dubi-CSRF']=csrf
 if body is not None:headers['Content-Type']='application/json';body=json.dumps(body).encode()
 r=urllib.request.Request(urls[node]+path,data=body,headers=headers,method=method)
 try:r=urllib.request.urlopen(r,context=ctx,timeout=25)
 except urllib.error.HTTPError as e:r=e
 b=r.read();return r.status,dict(r.headers),b

def check(label,expected,*args,**kwargs):
 status,headers,body=call(*args,**kwargs)
 if status!=expected:raise AssertionError(f'{label}: expected {expected}, got {status}: {body[:300]!r}')
 results.append({'check':label,'status':status});return headers,body
for i in range(3):
 check('node%d health'%i,200,i,'/healthz')
 check('node%d ready'%i,200,i,'/readyz')
 check('node%d UI'%i,200,i,'/')
 check('node%d admin'%i,200,i,'/api/v1/admin/gateway',token=admin['token'])
 check('node%d anonymous metrics'%i,401,i,'/metrics')
 check('node%d monitor'%i,200,i,'/metrics',token=monitor['token'])
 check('node%d monitor cannot administer'%i,403,i,'/api/v1/admin/gateway',token=monitor['token'])
 check('node%d tenant cannot administer'%i,401,i,'/api/v1/admin/gateway',token=tenant['token'])
 check('node%d admin cannot read tenant'%i,401,i,'/api/v1/config/entry?key=demo',token=admin['token'])
 _,b=check('node%d tenant reads Config'%i,200,i,'/api/v1/config/entry?key=demo',token=tenant['token'])
 assert json.loads(b)['value']=='SGVsbG8gRFVCSQ=='
check('cross-origin login refused',403,0,'/api/v1/auth/login',method='POST',body={'token':admin['token']},origin='https://wrong.example')
h,b=check('admin login',200,0,'/api/v1/auth/login',method='POST',body={'token':admin['token']},origin=urls[0])
cookie=h['Set-Cookie'].split(';',1)[0];csrf=json.loads(b)['csrf_token']
assert 'HttpOnly' in h['Set-Cookie'] and 'Secure' in h['Set-Cookie'] and 'SameSite=Strict' in h['Set-Cookie']
for i in range(3):check('shared session node%d'%i,200,i,'/api/v1/admin/gateway',cookie=cookie)
check('CSRF missing refused',403,1,'/api/v1/auth/logout',method='POST',cookie=cookie,origin=urls[1])
check('logout another node',200,2,'/api/v1/auth/logout',method='POST',cookie=cookie,origin=urls[2],csrf=csrf)
for i in range(3):check('logout revoked on node%d'%i,401,i,'/api/v1/admin/gateway',cookie=cookie)
h,b=check('unknown API is JSON',404,0,'/api/v1/missing');assert h['Content-Type']=='application/json'
check('internal RPC inaccessible',404,0,'/dubi.raft.GroupService/ReadIndex',method='POST')
# Create a disposable operator credential; retain the user's demonstration admin.
cmd=['runuser','-u',args.user,'--',str(base/'dubi-gateway'),'access']
r=subprocess.run(cmd+['create','--store',str(base/'n3'),'--server','127.0.0.1:18447','--ttl','1h'],check=True,capture_output=True,text=True)
other=json.loads(r.stdout)
h,b=check('temporary admin login',200,1,'/api/v1/auth/login',method='POST',body={'token':other['token']},origin=urls[1]);othercookie=h['Set-Cookie'].split(';',1)[0]
subprocess.run(cmd+['revoke','--store',str(base/'n2'),'--server','127.0.0.1:18445',other['credential_id']],check=True,capture_output=True)
for i in range(3):
 check('parent revoked node%d'%i,401,i,'/api/v1/me',token=other['token'])
 check('parent session revoked node%d'%i,401,i,'/api/v1/me',cookie=othercookie)
# Concurrent, bounded API requests, with full TLS verification.
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 statuses=list(pool.map(lambda i:call(i%3,'/api/v1/admin/gateway',token=admin['token'])[0],range(18)))
assert all(s==200 for s in statuses),statuses
results.append({'check':'18 concurrent authenticated reads','status':200})
(base/'gateway-acceptance.json').write_text(json.dumps({'passed':len(results),'checks':results},indent=2))
print(json.dumps({'passed':len(results),'all_tls_verified':True,'nodes':3,'result':str(base/'gateway-acceptance.json')}))
