#!/usr/bin/env python3
"""Run only against the temporary three-process HTTP demo on the VPS.
No secrets are printed or saved in the report. The failure exercise restarts
only dubi-gateway-demo-3, and always attempts to restore it in finally.
"""
import base64, json, ssl, subprocess, time, urllib.request, urllib.error, urllib.parse, uuid
from pathlib import Path

base = Path('/var/tmp/dubi-http-demo-20261106')
urls = [f'https://209.200.246.159:{p}' for p in (18444,18446,18448)]
admin = json.loads((base/'admin.json').read_text())['token']
context = ssl.create_default_context()
checks = []
def rid(): return str(uuid.uuid4())
def request(n, path, method='GET', body=None, token=admin):
    headers={'Content-Type':'application/json'}
    if token: headers['Authorization']='Bearer '+token
    req=urllib.request.Request(urls[n]+path, headers=headers, method=method, data=None if body is None else json.dumps(body).encode())
    try: response=urllib.request.urlopen(req,context=context,timeout=35)
    except urllib.error.HTTPError as e: response=e
    with response:
        raw=response.read().decode()
        try: data=json.loads(raw)
        except ValueError: data=raw
        return response.status,data

def check(label,n,path,want=200,**kw):
    code,data=request(n,path,**kw)
    if code!=want: raise AssertionError((label,code,data.get('code') if isinstance(data,dict) else 'non-json'))
    checks.append(label)
    return data.get('data',data) if isinstance(data,dict) else data

def ready(n):
    for _ in range(45):
        try:
            if request(n,'/readyz',token=None)[0]==200: return
        except OSError: pass
        time.sleep(1)
    raise AssertionError('demo node not ready')

a='/api/v1/admin/'
for n in range(3): ready(n)
monitor=check('issue temporary monitor',0,a+'credentials',method='POST',body={'request_id':rid(),'name':'监控诊断接口验收','kind':'monitor','ttl_seconds':3600})
tenant_token=None
try:
    time.sleep(12)
    for n in range(3):
        d=check(f'{n} real node sampling',n,a+'monitoring?window=900')
        assert len(d['nodes'])==3 and d['summary']['complete'], 'incomplete healthy sample'
        assert all(x['metrics']['sample_unix'] and x['metrics']['rss_bytes']>0 and x['metrics']['cpu_percent'] is not None and x['metrics']['ready'] for x in d['nodes'])
        assert d['summary']['observed_nodes']==3 and len(d['points'])<=121
        check(f'{n} anonymous denied',n,a+'monitoring',401,token=None)
        check(f'{n} monitor cannot administer',n,a+'monitoring',403,token=monitor['token'])
        metrics=check(f'{n} monitor can scrape',n,'/metrics',token=monitor['token'])
        assert metrics.count('# TYPE dubi_http_business_requests_total counter')==1
        assert 'dubi_process_resident_memory_bytes ' in metrics
        for seconds in (3600,21600):
            history=check(f'{n} {seconds}s query bounded',n,a+f'monitoring?window={seconds}')
            assert len(history['points'])<=121
    node=d['nodes'][0]
    scoped=check('region and node scope',1,a+'monitoring?'+urllib.parse.urlencode({'region':node['region_id'],'node_id':node['node_id']}))
    assert len(scoped['nodes'])==1
    for query in ('window=1','window=900&window=3600','unknown=1','window='):
        check('invalid query '+query,0,a+'monitoring?'+query,400)
    check('absent scope',0,a+'monitoring?node_id=absent',404)
    check('monitoring immutable method',0,a+'monitoring',405,method='DELETE')
    before=check('metrics before management queries',0,'/metrics')
    for _ in range(2):check('management collection',0,a+'monitoring')
    after=check('metrics after management queries',0,'/metrics')
    counter=lambda text:[v for v in text.splitlines() if v.startswith('dubi_http_business_requests_total{')]
    assert counter(before)==counter(after),'management counted as business'
    tenant='monitor-qa-'+uuid.uuid4().hex[:8]
    check('create isolated test tenant',0,a+'tenants',method='POST',body={'request_id':rid(),'tenant_id':tenant,'name':'监控业务流量验收','modules':['config'],'storage_limit_bytes':0})
    tenant_token=check('issue business credential',1,a+'credentials',method='POST',body={'request_id':rid(),'name':'监控采样业务流量','kind':'tenant','tenant_id':tenant,'ttl_seconds':3600})
    check('tenant cannot read monitoring',0,a+'monitoring',403,token=tenant_token['token'])
    for n in range(3):
        path='/api/v1/config/entry?key=monitor-check-'+str(n)
        value=base64.b64encode(b'monitoring-real-business').decode()
        check(f'{n} config write',n,path,method='PUT',token=tenant_token['token'],body={'request_id':rid(),'value':value})
        read=check(f'{n} config read',n,path,token=tenant_token['token']);assert read['value']==value
        check(f'{n} config delete',n,path,method='DELETE',token=tenant_token['token'],body={'request_id':rid()})
    time.sleep(12)
    d=check('completed traffic in histogram',0,a+'monitoring?window=900')
    assert any(p['read_rate']>0 and p['read_p95_ms'] is not None for p in d['points'])
    assert any(p['write_rate']>0 and p['write_p95_ms'] is not None for p in d['points'])
    records=[]
    for n in (0,2):
        payload={'request_id':rid()}
        record=check('save diagnostic',n,a+'diagnostics',method='POST',body=payload);records.append(record)
        assert len(record['nodes'])==3 and all(len(x['checks'])==4 for x in record['nodes'])
        again=check('retry at different entry',1,a+'diagnostics',method='POST',body=payload)
        assert again['created_index']==record['created_index']
        for peer in range(3):
            visible=check(f'{peer} shared diagnostic',peer,a+'diagnostics/'+record['id']);assert visible==record
    check('same ID different scope conflict',0,a+'diagnostics',409,method='POST',body={'request_id':records[-1]['id'],'node_id':node['node_id']})
    page=check('newest record first',1,a+'diagnostics?limit=1');assert page['items'][0]['id']==records[-1]['id']
    nextpage=check('record cursor page',2,a+'diagnostics?limit=1&cursor='+urllib.parse.quote(page['next_cursor']));assert nextpage['items'][0]['id']==records[-2]['id']
    check('cursor binds filter',0,a+'diagnostics?status=passed&cursor='+urllib.parse.quote(page['next_cursor']),400)
    check('unknown diagnostic status',0,a+'diagnostics?status=healthy',400)
    check('no matching records',0,a+'diagnostics?q=never-matches-xyz')
    # Only this named temporary demo unit is interrupted, never a fleet node.
    try:
        subprocess.run(['systemctl','stop','dubi-gateway-demo-3'],check=True)
        # A stopped Root leader can temporarily make authorization/directory
        # unavailable. Wait for quorum election, never bypass authorization.
        for attempt in range(15):
            code,body=request(0,a+'monitoring?window=900')
            if code==200: break
            assert code==503, ('unexpected failure observation status',code)
            time.sleep(2)
        assert code==200, 'Root did not recover after demo peer stop'
        partial=body['data'];checks.append('unreachable node is partial after election')
        unavailable=[n for n in partial['nodes'] if n['status']=='unavailable']
        assert len(unavailable)==1 and not partial['summary']['complete']
        assert all(v is None for v in unavailable[0]['metrics'].values()),'missing metrics became zero'
        assert all(p['observed_nodes']<=2 for p in partial['points'])
        r=check('persist partial diagnostic',0,a+'diagnostics',method='POST',body={'request_id':rid()})
        missing=next(n for n in r['nodes'] if n['node_id']==unavailable[0]['node_id'])
        assert all(c['state']=='unknown' for c in missing['checks'])
    finally:
        subprocess.run(['systemctl','start','dubi-gateway-demo-3'],check=True)
        ready(2)
    time.sleep(12)
    recovered=check('node sampling resumes',0,a+'monitoring?window=900');assert recovered['summary']['complete']
    assert check('diagnostic survives node restart',2,a+'diagnostics/'+records[0]['id'])==records[0]
    checks.append('failure exercise recovered all demo nodes')
finally:
    for c in (monitor,tenant_token):
        if c: check('revoke acceptance credential',0,a+'credentials/'+c['credential_id']+'/revoke',method='POST',body={'request_id':rid()})
(base/'monitoring-acceptance.json').write_text(json.dumps({'checks':checks,'count':len(checks),'completed_unix':int(time.time())},ensure_ascii=False,indent=2))
print('PASS',len(checks),'monitoring/diagnostic checks on the three temporary VPS processes')
