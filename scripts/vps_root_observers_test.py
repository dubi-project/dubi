import pathlib
import unittest

from vps_root_observers import observer_work_directory


class ObserverWorkDirectoryTests(unittest.TestCase):
    def test_equal_basenames_in_distinct_runs_are_isolated(self):
        node = {'identity': {'node_id': 'node_one'}, 'name': 'sg01'}
        a = observer_work_directory('/tmp/run-a/candidate-holding-join/root-observers', node)
        b = observer_work_directory('/tmp/run-b/candidate-holding-join/root-observers', node)
        self.assertNotEqual(a, b)
        self.assertEqual(a, observer_work_directory('/tmp/run-a/candidate-holding-join/root-observers', node))
        self.assertEqual(pathlib.Path(a).parent, pathlib.Path('/var/tmp'))

    def test_node_identity_isolates_rejoin_and_name_is_not_a_path(self):
        evidence = '/tmp/run/candidate-holding-join/root-observers'
        old = {'identity': {'node_id': 'node_old'}, 'name': 'sg03'}
        new = {'identity': {'node_id': 'node_new'}, 'name': 'sg03'}
        self.assertNotEqual(observer_work_directory(evidence, old), observer_work_directory(evidence, new))
        old['name'] = '../../untrusted'
        path = observer_work_directory(evidence, old)
        self.assertEqual(pathlib.Path(path).parent, pathlib.Path('/var/tmp'))
        self.assertNotIn('untrusted', path)


if __name__ == '__main__':
    unittest.main()
