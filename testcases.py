import unittest, os, glob
from os.path import join
from w1data.rollup import do_rollup

class FakeArgs:
    @classmethod
    def fix_location(cls, location):
        if not os.path.is_abs(location):
            location = os.path.normpath(
                os.path.expanduser(
                    join(
                        os.path.dirname(__file__), location)))
        return location

    def __init__(self, raw_location, rollup_location, debug=False):
        self.raw_location = self.fix_location(raw_location)
        self.rollup_location = self.fix_location(rollup_location)
        self.debug = debug

class TestCases(unittest.TestCase):
    output = None

    def setUp(self):
        self.output = join(os.path.dirname(__file__), 't', 'output')
        self.t = join(os.path.dirname(__file__), 't')
        try:
            os.makedirs(self.output)
        except FileExistsError:
            pass
        with os.scandir(self.output) as s:
            for entry in s:
                if entry.is_dir:
                    for fn in glob.glob(join(self.output, entry.name, '*')):
                        os.unlink(fn)
                    os.rmdir(join(self.output, entry.name))
                    continue
                os.unlink(join(self.output, entry.name))

    def _output_files_exist(self):
        found = False
        with os.scandir(self.output) as s:
            for entry in s:
                found=True
                break
        return found

    def test_all_empty(self):
        self.assertEqual(do_rollup(self.output, join(self.t, 'empty_ok')), None)
        self.assertFalse(self._output_files_exist())

    def test_top_metadata_only_ok(self):
        self.assertEqual(do_rollup(self.output, join(self.t, 'top_metadata_only_ok')), None)
        self.assertFalse(self._output_files_exist())

    def test_one_observation(self):
        self.assertEqual(do_rollup(self.output, join(self.t, 'one_observation')), None)
        self.assertTrue(self._output_files_exist())

if os.environ.get('DEBUG', None) is not None:
    import logging
    logging.basicConfig(format="%(levelname)s:%(filename)s:%(lineno)d:%(message)s", level=logging.DEBUG)
\
if __name__ == '__main__':
    unittest.main()
