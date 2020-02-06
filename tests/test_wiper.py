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

import getpass
import os
import shutil
import tempfile
import unittest
from lsst.dbb.buffmngrs.handoff import Wiper


class WiperTestCase(unittest.TestCase):
    """Test the command cleaning up the staging area on the endpoint site.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.user = getpass.getuser()
        self.host = "localhost"

    def tearDown(self):
        shutil.rmtree(self.dir)

    def testInvalidConfig(self):
        """Test if Wiper complains about an invalid configuration.
        """
        config = dict()
        args = [config]
        self.assertRaises(ValueError, Wiper, *args)

    def testEmpty(self):
        """Test if Wiper removes empty directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)

        config = dict(user=self.user, host=self.host, staging=self.dir)
        cmd = Wiper(config)
        cmd.run()

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 0)

    def testNonEmpty(self):
        """Test if Wiper does not remove non-empty directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)
        fd, fn = tempfile.mkstemp(dir=os.path.join(self.dir, subdir))
        os.close(fd)

        config = dict(user=self.user, host=self.host, staging=self.dir)
        cmd = Wiper(config)
        cmd.run()

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)
