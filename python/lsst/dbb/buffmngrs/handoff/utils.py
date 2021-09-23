# This file is part of dbb_buffmngrs_handoff.
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
"""A collection of general purpose functions.
"""
import hashlib
import importlib
import logging
import queue
import time
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

from sqlalchemy import create_engine


__all__ = [
    "get_checksum",
    "get_chunk",
    "run_continuously",
    "setup_db_conn",
    "setup_logging"
]


def get_checksum(path, method='blake2', block_size=4096):
    """Calculate checksum for a file using BLAKE2 cryptographic hash function.

    Parameters
    ----------
    path : `str`
        Path to the file.
    method : `str`
        An algorithm to use for calculating file's hash. Supported algorithms
        include:
        * _blake2_: BLAKE2 cryptographic hash,
        * _md5_: traditional MD5 algorithm,
        * _sha1_: SHA-1 cryptographic hash.
        By default or if unsupported method is provided, BLAKE2 algorithm wil
        be used.
    block_size : `int`, optional
        Size of the block

    Returns
    -------
    `str`
        File's hash calculated using a given method.
    """
    methods = {
        'blake2': hashlib.blake2b,
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
    }
    hasher = methods.get(method, hashlib.blake2b)()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_chunk(q, size=10):
    """Grab a number of items from a queue.

    Parameters
    ----------
    q : queue.Queue
        The queue to grab items from.
    size : int, optional
        Number of elements to grab from the queue, default to 10.

    Returns
    -------
    `list`
        Items grabbed from the queue.
    """
    chunk = []
    for _ in range(size):
        try:
            item = q.get(block=False)
        except queue.Empty:
            break
        else:
            chunk.append(item)
    return chunk


def run_continuously(cmd, pause=1):
    """Run a command continuously.

    Parameters
    ----------
    cmd : Command
        The command to run in the background
    pause : int, optional
        Amount of seconds to pause between consecutive executions of the
        command.
    """
    while True:
        cmd.run()
        time.sleep(pause)


def setup_db_conn(config):
    """Create a database connection.

    Parameters
    ----------
    config : `dict`
        Database connection configuration.

    Returns
    -------
    `sqlalchemy.engine.Engine`
        SQLAlchemy object which describes how to talk to a specific database.
    """
    pool_name = config.get("pool_class", "QueuePool")
    module = importlib.import_module("sqlalchemy.pool")
    try:
        class_ = getattr(module, pool_name)
    except AttributeError:
        raise RuntimeError(f"unknown connection pool type: {pool_name}")
    engine = create_engine(config["engine"],
                           echo=config.get("echo", False),
                           poolclass=class_)
    return engine


def setup_logging(options=None):
    """Configure logger.

    Parameters
    ----------
    options : dict, optional
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.

    Returns
    -------
    `logging.Logger`
        A root logger to use.
    """
    # Define default settings for the logger. They will be overridden with
    # values found in 'options', if specified.
    settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "INFO",
        "rotate": None,
        "when": 'H',
        "interval": 1,
        "maxbytes": 0,
        "backup_count": 0
    }
    if options is not None:
        settings.update(options)

    kwargs = {"format": settings["format"]}

    level_name = settings["level"]
    level = getattr(logging, level_name.upper(), logging.WARNING)
    kwargs["level"] = level

    logfile = settings["file"]
    if logfile is not None:
        handler = logging.FileHandler
        opts = {}

        rotate = settings["rotate"]
        if rotate is not None:
            opts.update({"backupCount": settings["backup_count"]})
            if rotate.upper() == "SIZE":
                opts.update({
                    "maxBytes": settings["maxbytes"],
                })
                handler = logging.handlers.RotatingFileHandler
            elif rotate.upper() == "TIME":
                opts.update({
                    "when": settings["when"],
                    "interval": settings["interval"],
                })
                handler = logging.handlers.TimedRotatingFileHandler
            else:
                raise RuntimeError(f"unknown log rotate method '{rotate}'")

        kwargs["handlers"] = [handler(logfile, **opts)]

    logging.basicConfig(**kwargs)
