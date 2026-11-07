import json, secrets, uuid, urllib.request, urllib.error, re
from pathlib import Path
base=Path('/var/tmp/dubi-http-demo-20261106');operator=json.loads((base/'admin.json').read_text())['token'];urls=[f'https://209.200.246.159:{p}' for p in (18444,18446,18448)]
created_accounts=[];created_credentials=[];checks=0
def rid():return str(uuid.uuid4())
def call(path,method='GET',body=None,node=0,token=operator,session=None,want=200):
 global checks
 headers={'Content-Type':'application/json','Origin':urls[node]}
 if token:headers['Authorization']='Bearer '+token
 if session:headers.update({'Cookie':session['cookie'],'X-Dubi-CSRF':session['csrf_token']})
 req=urllib.request.Request(urls[node]+path,method=method,headers=headers,data=None if body is None else json.dumps(body).encode())
 try:r=urllib.request.urlopen(req,timeout=30)
 except urllib.error.HTTPError as e:r=e
 raw=r.read();value=json.loads(raw) if raw else {}
 assert r.status==want,(path,r.status,value.get('message'))
 checks+=1;return value.get('data',value),r.headers
api='/api/v1/admin/'
def ordered(resource,key):
 cursor='';rows=[]
 while True:
  data,_=call(api+resource+'?limit=5'+('&cursor='+cursor if cursor else ''));rows+=data['items'];cursor=data.get('next_cursor','')
  if not cursor:break
 assert len({key(x)[-1] for x in rows})==len(rows)
 assert [key(x) for x in rows]==sorted(key(x) for x in rows)
try:
 pwd=secrets.token_urlsafe(24)
 a,_=call(api+'accounts','POST',{'request_id':rid(),'username':'verify-'+secrets.token_hex(4),'name':'临时界面验收','password':pwd});created_accounts.append(a)
 assert re.fullmatch('account_[0-9a-f]{12}',a['id'])
 login,h=call('/api/v1/auth/login','POST',{'username':a['username'],'password':pwd},token=None);login['cookie']=h['Set-Cookie'].split(';')[0]
 for n in range(3):
  detail,_=call(api+'accounts/'+a['id'],node=n);assert detail['name']==a['name']
  me,_=call('/api/v1/me',token=None,session=login,node=n);assert me['account_id']==a['id']
 call(api+'accounts/'+a['id'],'DELETE',{'request_id':rid(),'revision':a['revision']},token=None,session=login,want=409)
 ordered('accounts',lambda x:(int(x['disabled']),-x['created_index'],x['id']))
 for kind in ['admin','monitor']:
  c,_=call(api+'credentials','POST',{'request_id':rid(),'name':'临时永久凭证验收','kind':kind,'ttl_seconds':0,'never_expires':True});created_credentials.append(c)
  assert re.fullmatch('credential_[0-9a-f]{12}',c['credential_id']) and c['expires_unix']==0
  for n in range(3):
   d,_=call(api+'credentials/'+c['credential_id'],node=n);assert d['status']=='active' and d['expires_unix']==0
  call('/api/v1/me',token=c['token'],node=2)
  call(api+'credentials/'+c['credential_id'],'DELETE',{'request_id':rid()},want=409)
  call(api+'credentials/'+c['credential_id']+'/revoke','POST',{'request_id':rid()},node=1)
  call('/api/v1/me',token=c['token'],node=2,want=401)
 ordered('credentials',lambda x:({'active':0,'expired':1,'revoked':2}[x['status']],-x['created_unix'],-x.get('created_index',0),x['credential_id']))
 for c in created_credentials:
  body={'request_id':rid()};path=api+'credentials/'+c['credential_id'];call(path,'DELETE',body,node=1);call(path,'DELETE',body,node=2)
  for n in range(3):call(path,node=n,want=404)
 created_credentials.clear()
 body={'request_id':rid(),'revision':a['revision']};call(api+'accounts/'+a['id'],'DELETE',body,node=1);call(api+'accounts/'+a['id'],'DELETE',body,node=2)
 for n in range(3):
  call(api+'accounts/'+a['id'],node=n,want=404);call('/api/v1/me',token=None,session=login,node=n,want=401)
 created_accounts.clear();print(json.dumps({'passed':True,'checks':checks,'nodes':3,'verified':['account ID/detail/delete/session invalidation','permanent credentials/revoke/delete','enabled-first account pagination','credential status-first pagination']},ensure_ascii=False))
finally:
 for c in created_credentials:
  path=api+'credentials/'+c['credential_id'];call(path+'/revoke','POST',{'request_id':rid()});call(path,'DELETE',{'request_id':rid()})
 for a in created_accounts:call(api+'accounts/'+a['id'],'DELETE',{'request_id':rid(),'revision':a['revision']})
