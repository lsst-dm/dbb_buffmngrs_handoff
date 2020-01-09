import os
import queue
import shutil
import tempfile
import unittest
from dbb_buffer_mngr.porter import Porter


class PorterTestCase(unittest.TestCase):
    """Test file porter.
    """

    def setUp(self):
        self.src = tempfile.mkdtemp()
        self.dst = tempfile.mkdtemp()
        self.hld = tempfile.mkdtemp()
        self.loc = "localhost:{}".format(self.dst)

        sub = tempfile.mkdtemp(dir=self.src)
        src = dict([tempfile.mkstemp(dir=self.src)])
        src.update(dict(tempfile.mkstemp(dir=sub) for _ in range(2)))
        for fd in src:
            os.close(fd)

        self.queue = queue.Queue()
        for path in src.values():
            tail = path.replace(self.src, "").lstrip(os.sep)
            dn, fn = os.path.split(tail)
            self.queue.put((self.src, dn, fn))

    def tearDown(self):
        shutil.rmtree(self.src)
        shutil.rmtree(self.dst)
        shutil.rmtree(self.hld)

    def testInvalidHoldingArea(self):
        args = [self.loc, self.queue]
        kwargs = {"holding_area": "/not/a/path"}
        self.assertRaises(ValueError, Porter, *args, **kwargs)

    def testWithoutHoldingArea(self):
        p = Porter(self.loc, self.queue)
        p.run()

        src = []
        for top, sub, names in os.walk(self.src):
            for name in names:
                src.append(os.path.join(top, name))
        src = set([p.replace(self.src, "").lstrip(os.sep) for p in src])
        dst = []
        for top, sub, names in os.walk(self.dst):
            for name in names:
                dst.append(os.path.join(top, name))
        dst = set([p.replace(self.dst, "").lstrip(os.sep) for p in dst])
        self.assertEqual(src, dst)
        self.assertEqual(self.queue.qsize(), 0)

    def testWithHoldingArea(self):
        p = Porter(self.loc, self.queue, holding_area=self.hld)
        p.run()

        src = []
        for top, sub, names in os.walk(self.src):
            for name in names:
                src.append(os.path.join(top, name))
        src = set([p.replace(self.src, "").lstrip(os.sep) for p in src])
        dst = []
        for top, sub, names in os.walk(self.dst):
            for name in names:
                dst.append(os.path.join(top, name))
        dst = set([p.replace(self.dst, "").lstrip(os.sep) for p in dst])
        hld = []
        for top, sub, names in os.walk(self.hld):
            for name in names:
                hld.append(os.path.join(top, name))
        hld = set([p.replace(self.hld, "").lstrip(os.sep) for p in hld])
        self.assertEqual(dst, hld)
        self.assertEqual(src, set())
        self.assertEqual(self.queue.qsize(), 0)
