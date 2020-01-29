"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.dbb.buffer.mngr


_g = globals()
_g.update(build_package_configs(
    project_name='dbb_buffer_mngr',
    version=lsst.dbb.buffer.mngr.version.__version__))
