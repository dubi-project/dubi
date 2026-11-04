import base64
import json
import unittest
from vps_root_failover_workload import check_value, transient

class WorkloadEvidenceTests(unittest.TestCase):
    def test_exact_bytes_are_required(self):
        check_value(json.dumps({'found': True, 'value': base64.b64encode(b'expected').decode()}), 'expected')
        for response in ({'found': False}, {'found': True, 'value': '!!!'}, {'found': True, 'value': 'd3Jvbmc='}):
            with self.assertRaises(ValueError): check_value(json.dumps(response), 'expected')
    def test_transient_text_never_hides_security_or_corruption(self):
        for text in ('context deadline exceeded', 'code = Unavailable', 'connection refused'):
            self.assertTrue(transient(text))
        for text in ('permission denied: Unavailable', 'certificate timeout', 'data loss', 'bytes mismatch', 'unknown failure'):
            self.assertFalse(transient(text))

if __name__ == '__main__': unittest.main()
