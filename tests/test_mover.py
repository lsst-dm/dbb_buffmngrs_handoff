# This file is part of dbb_buffer_mngr.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import queue
import shutil
import tempfile
import unittest
from lsst.dbb.buffmngrs.handoff import Mover
from lsst.dbb.buffmngrs.handoff.messages import FileMsg


class MoverTestCase(unittest.TestCase):
    """Test the command moving files from the buffer to holding area.
    """

    def setUp(self):
        self.src = tempfile.mkdtemp()
        self.dst = tempfile.mkdtemp()
        files = dict([tempfile.mkstemp(dir=self.src)])
        for fd in files:
            os.close(fd)
        self.files = files.values()

        self.inp = queue.Queue()
        self.out = queue.Queue()
        for path in self.files:
            path = os.path.relpath(path, start=self.src)
            tail, name = os.path.split(path)
            msg = FileMsg(head=self.src, tail=tail, name=name)
            self.inp.put(msg)

    def tearDown(self):
        shutil.rmtree(self.src)
        shutil.rmtree(self.dst)

    def testInvalidConfig(self):
        """Test if Mover complains about an invalid configuration.
        """
        config = dict()
        args = [config, self.inp, self.out]
        self.assertRaises(ValueError, Mover, *args)

    def testInvalidBuffer(self):
        """Test if Mover complains about a non-existing buffer.
        """
        config = dict(holding="/not/a/path")
        args = [config, self.inp, self.out]
        self.assertRaises(ValueError, Mover, *args)

    def testRun(self):
        """Test if Mover moves files as expected.
        """
        config = dict(holding=self.dst)
        cmd = Mover(config, self.inp, self.out)
        cmd.run()

        src = set()
        for top, subs, files in os.walk(self.src):
            for f in files:
                src.add(f)
        dst = set()
        for top, subs, files in os.walk(self.dst):
            for f in files:
                dst.add(f)
        ref = set([os.path.relpath(p, start=self.src) for p in self.files])
        self.assertEqual(len(src), 0)
        self.assertEqual(len(dst), 1)
        self.assertEqual(ref, dst)
