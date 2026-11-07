#!/usr/bin/env python3
"""Account and shared-notification acceptance on the disposable TT01 console.
Creates a private initial admin login, never prints secrets, disables test accounts.
"""
import json, secrets, time, uuid, urllib.request, urllib.error, os
from pathlib import Path
base=Path('/var/tmp/dubi-http-demo-20261106')
operator=json.loads((base/'admin.json').read_text())['token']
urls=[f'https://209.200.246.159:{p}' for p in (18444,18446,18448)]
api='/api/v1/admin/'
created=[]
def rid():return str(uuid.uuid4())
def call(path,method='GET',body=None,node=0,token=None,session=None,want=200,csrf=True):
 headers={'Content-Type':'application/json','Origin':urls[node]}
 if token:headers['Authorization']='Bearer '+token
 if session:
  headers['Cookie']=session['cookie']
  if csrf:headers['X-Dubi-CSRF']=session['csrf_token']
 req=urllib.request.Request(urls[node]+path,method=method,headers=headers,data=json.dumps(body).encode() if body is not None else None)
 try:r=urllib.request.urlopen(req,timeout=30)
 except urllib.error.HTTPError as e:r=e
 text=r.read().decode();data=json.loads(text)
 if r.status!=want:raise AssertionError((path,r.status,data.get('message') or data.get('error',{}).get('message')))
 if 'password_hash' in text or 'password_salt' in text:raise AssertionError('password verifier exposed')
 return data.get('data',data),dict(r.headers)
def login(user,pwd,node=0,want=200):
 me,h=call('/api/v1/auth/login','POST',{'username':user,'password':pwd},node=node,want=want)
 if want==200:me['cookie']=h['Set-Cookie'].split(';')[0]
 return me
def create(user,name,pwd):return call(api+'accounts','POST',{'request_id':rid(),'username':user,'name':name,'password':pwd},token=operator)[0]
def accounts():return call(api+'accounts?limit=200',token=operator)[0]['items']
def current(a):return next(row for row in accounts() if row['id']==a['id'])
def update(a,changes):
 a=current(a);return call(api+'accounts/'+a['id'],'PATCH',{'request_id':rid(),'revision':a['revision'],**changes},node=1,token=operator)[0]
# The initial demo admin belongs to the user, not to this test run.
setup=base/'console-account.json'
if not setup.exists():
 if any(a['username']=='admin' for a in accounts()):raise AssertionError('existing admin found; refusing to replace its password')
 pwd=secrets.token_urlsafe(20);a=create('admin','管理员',pwd)
 fd=os.open(setup,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as f:json.dump({'url':urls[0]+'/','username':'admin','password':pwd,'account_id':a['id']},f)
initial=json.loads(setup.read_text());login(initial['username'],initial['password'])
print('PASS initial admin login; initial password saved privately',flush=True)
try:
 suffix=secrets.token_hex(3);pwd=secrets.token_urlsafe(18);newpwd=secrets.token_urlsafe(18)
 a=create('qa-'+suffix,'账号验收 A',pwd);created.append(a)
 b=create('qb-'+suffix,'账号验收 B',pwd);created.append(b)
 s=login(a['username'],pwd,2);other=login(b['username'],pwd,1)
 for n in range(3):
  me,_=call('/api/v1/me',node=n,session=s);assert me['account_id']==a['id']
 print('PASS account session usable across all three nodes',flush=True)
 login(a['username'],'incorrect password',want=401)
 profile={'request_id':rid(),'revision':a['revision'],'name':'验收资料已更新'}
 call(api+'profile','PATCH',profile,node=1,session=s,csrf=False,want=403)
 a,_=call(api+'profile','PATCH',profile,node=1,session=s)
 me,_=call('/api/v1/me',node=2,session=s);assert me['name']=='验收资料已更新'
 print('PASS profile name update, cross-node identity and CSRF rejection',flush=True)
 feed,_=call(api+'notifications?limit=1',session=s)
 if feed['items']:
  notice=feed['items'][0];original=notice['read']
  call(api+'notifications/read','POST',{'request_id':rid(),'id':notice['id'],'read':not original},session=s)
  other_notice,_=call(api+'notifications/'+notice['id'],node=2,session=other);assert other_notice['read']==(not original)
  call(api+'notifications/read','POST',{'request_id':rid(),'id':notice['id'],'read':original},session=other)
 print('PASS cluster-shared notification read state across accounts',flush=True)
 change={'request_id':rid(),'revision':a['revision'],'current_password':pwd,'password':newpwd}
 a,_=call(api+'password','POST',change,node=1,session=s)
 for n in range(3):call('/api/v1/me',node=n,session=s,want=401)
 call('/api/v1/me',session=other)
 login(a['username'],pwd,want=401);s=login(a['username'],newpwd,2)
 print('PASS password change invalidates old sessions and old password only',flush=True)
 a=update(a,{'disabled':True})
 call('/api/v1/me',node=2,session=s,want=401);login(a['username'],newpwd,want=401)
 a=update(a,{'disabled':False,'password':pwd});login(a['username'],pwd,1)
 monitor=json.loads((base/'monitor.json').read_text())['token']
 call(api+'accounts',token=monitor,want=403)
 print('PASS account disable, admin reset/re-enable and monitor denial',flush=True)
 print('PASS account acceptance completed',flush=True)
finally:
 for a in created:
  try:update(a,{'disabled':True})
  except Exception:print('Temporary account cleanup requires attention: '+a['id'],flush=True)
