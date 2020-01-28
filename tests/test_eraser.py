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
import shutil
import tempfile
import time
import unittest
from lsst.dbb.buffer.mngr import Eraser


class EraserTestCase(unittest.TestCase):
    """Test the command cleaning up the buffer on the handoff site.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def testInvalidConfig(self):
        """Test if Eraser complains about invalid configuration.
        """
        config = dict()
        args = [config]
        self.assertRaises(ValueError, Eraser, *args)

    def testInvalidBuffer(self):
        """Test if Eraser complains about a non-existing buffer.
        """
        config = dict(buffer="/not/a/path")
        args = [config]
        self.assertRaises(ValueError, Eraser, *args)

    def testNonExpiredDir(self):
        """Test if Eraser does not remove a non-expired directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)

        config = dict(buffer=self.dir)
        cmd = Eraser(config)
        cmd.run()

        dirs = []
        for topdir, subdirs, files in os.walk(self.dir):
            for d in subdirs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)

    def testExpiredDir(self):
        """Test if Eraser removes an expired directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)
        time.sleep(2)

        config = dict(buffer=self.dir)
        cmd = Eraser(config, exp_time=1)
        cmd.run()

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 0)

    def testNonEmptyDir(self):
        """Test if Eraser does not remove a non-empty directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)
        files = dict([tempfile.mkstemp(dir=subdir) for _ in range(2)])
        for fd in files:
            os.close(fd)
        time.sleep(2)

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)

