#!/usr/bin/env python3
"""Migration-soak correctness guards; run on the acceptance VPS."""
import base64
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from vps_cluster_tools_test import inventory
import vps_range_soak as soak


class RangeSoakTests(unittest.TestCase):
    def setUp(self):
        self.nodes=inventory()['nodes']
        for n in self.nodes:
            n['status']['local_storage']['volumes']=[{'volume_id':'v-'+n['name'],'state':'active'}]
        self.status=self.nodes[0]['status']
        self.proof={'nodes':[{'node_id':n['identity']['node_id'],'held':True,'store_id':n['store_id'],
                              'volume_id':'v-'+n['name']} for n in self.nodes]}

    def test_exact_value_and_absent_key_cannot_silently_pass(self):
        expected=base64.b64encode(b'config').decode()
        soak.verify_value({'found':True,'value':expected},'config')
        for r in [{'found':False,'value':expected},{'found':True,'value':base64.b64encode(b'session').decode()},{}]:
            with self.assertRaises(ValueError):soak.verify_value(r,'config')

    def test_majorities_not_every_node_or_every_region(self):
        regions=sorted({n['region'] for n in self.nodes})
        held=set()
        for region in regions[:2]:
            held.update(n['identity']['node_id'] for n in [n for n in self.nodes if n['region']==region][:2])
        for n in self.proof['nodes']:
            n['held']=n['node_id'] in held
            if not n['held']:n['error']='rpc error: code = NotFound desc = record is not held on this Node'
        self.assertEqual(len(soak.verify_holding(self.proof,self.status,self.nodes)),2)
        next(n for n in self.proof['nodes'] if n['held']).update(held=False,error='rpc error: code = NotFound desc = record is not held on this Node')
        with self.assertRaises(ValueError):soak.verify_holding(self.proof,self.status,self.nodes)

    def test_identity_and_unreachable_observations_fail_even_with_majority(self):
        for mutate in [lambda p:p['nodes'].pop(),lambda p:p['nodes'].append(p['nodes'][0]),
                       lambda p:p['nodes'][0].update(store_id='old-store'),
                       lambda p:p['nodes'][0].update(volume_id='old-volume'),
                       lambda p:p['nodes'][0].update(held=False,error='rpc error: Unavailable')]:
            p=deepcopy(self.proof);mutate(p)
            with self.assertRaises(ValueError):soak.verify_holding(p,self.status,self.nodes)

    def test_failed_timeout_and_malformed_replies_are_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            runner=soak.Soak.__new__(soak.Soak);runner.out=Path(d);runner.number=0;runner.report={}
            replies=[subprocess.CompletedProcess([],1,'partial','failure'),
                     subprocess.TimeoutExpired([],1,output=b'partial',stderr=b'timeout'),
                     subprocess.CompletedProcess([],0,'{broken','parse-warning')]
            for reply in replies:
                with patch.object(soak.subprocess,'run',side_effect=[reply]):
                    with self.assertRaises((RuntimeError,ValueError)):runner.call('put',['fixture'])
            self.assertEqual(len(list(Path(d).glob('*.stdout'))),3)
            self.assertEqual(len(list(Path(d).glob('*.stderr'))),3)
            rows=[json.loads(s) for s in (Path(d)/'metrics.jsonl').read_text().splitlines()]
            self.assertEqual(rows[0]['exit_code'],1)
            self.assertIn('timeout_seconds',rows[1])


if __name__=='__main__':unittest.main()
