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

import argparse
import jsonschema
import logging
import os
import queue
import threading
import time
import yaml
import lsst.dbb.buffmngrs.handoff as mgr

__all__ = ["main"]


logger = logging.getLogger("lsst.dbb.buffmngrs.handoff")


def parse_args():
    """Parse command line arguments.

    Returns
    -------
    argparse.Namespace
        A namespace populated with arguments and their values.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default=None,
                        help="configuration file in YAML format")
    parser.add_argument("-v", "--validate", action="store_true", default=False,
                        help="validate configuration file")
    return parser.parse_args()


def set_logger(options=None):
    """Configure logger.

    Parameters
    ----------
    options : dict, optional
       Logger settings. The key/value pairs it contains will be used to
       override corresponding default settings.  If empty or None (default),
       logger will be set up with default settings.
    """
    # Define default settings for the logger. They will be overridden with
    # values found in 'options', if specified.
    settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "WARNING",
    }
    if options is not None:
        settings.update(options)

    level_name = settings["level"]
    level = getattr(logging, level_name.upper(), logging.WARNING)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    logfile = settings["file"]
    if logfile is not None:
        handler = logging.FileHandler(logfile)
    logger.addHandler(handler)

    fmt = settings["format"]
    formatter = logging.Formatter(fmt=fmt, datefmt=None)
    handler.setFormatter(formatter)


def background_thread(cmd, pause=1):
    """Run a command in the background, repeatedly.

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


def main():
    """Application entry point.
    """
    args = parse_args()

    # Read provided configuration or use the default one.
    if args.config is None:
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        filename = os.path.normpath(os.path.join(root, "etc/transd.yml"))
    else:
        filename = args.config
    with open(filename, "r") as f:
        config = yaml.safe_load(f)

    # Validate configuration, if requested.
    if args.validate:
        try:
            jsonschema.validate(instance=config, schema=mgr.SCHEMA)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"Configuration error: {ex.message}.")

    # Set up a logger.
    logger_options = config.get("logging", None)
    set_logger(options=logger_options)
    logger.info(f"Configuration read from '{filename}'.")

    # Initialize runtime settings.
    #
    # chunk_size
    #     Maximal number of files being processes by a transfer thread during
    #     a single session.
    #
    # timeout
    #     Number of seconds after which the subprocess executing a shell
    #     command # will be terminated.
    #
    # pause
    #     Number of seconds both the main thread and any daemon thread will
    #     spent idling after their session finished.
    #
    # transfer_pool
    #     Number of transfer threads to run concurrently.
    #
    # expiration_time
    #     Number of seconds that need to pass from their last modification
    #     before empty directories in the handoff buffer can be removed.
    settings = {
        "chunk_size": 1,
        "timeout": None,
        "pause": 1,
        "transfer_pool": 1,
        "expiration_time": 86400
    }
    general_options = config.get("general", None)
    if general_options is not None:
        settings.update(general_options)
    delay = settings["pause"]
    chunk_size = settings["chunk_size"]
    timeout = settings["timeout"]
    pool_size = settings["transfer_pool"]
    exp_time = settings["expiration_time"]

    # Initialize task queues.
    awaiting = queue.Queue()
    completed = queue.Queue()

    # Define tasks related to managing the buffer.
    handoff = config["handoff"]
    scanner = mgr.Scanner(handoff, awaiting)
    mover = mgr.Mover(handoff, completed)
    eraser = mgr.Eraser(handoff, exp_time=exp_time)
    cleaner = mgr.Macro()
    cleaner.add(mover)
    cleaner.add(eraser)

    # Define tasks related to file transfer.
    endpoint = config["endpoint"]
    porter = mgr.Porter(endpoint, awaiting, completed,
                        chunk_size=chunk_size, timeout=timeout)
    wiper = mgr.Wiper(endpoint)

    logger.info("Starting cleaner daemon...")
    daemon = threading.Thread(target=background_thread,
                              args=(cleaner,), kwargs=dict(pause=delay),
                              daemon=True)
    daemon.start()
    logger.info("Done.")

    logger.info("Starting buffer monitoring...")
    while True:
        # Scan source location for files.
        start = time.time()
        scanner.run()
        end = time.time()
        duration = end - start
        logger.info(f"Scan completed: {awaiting.qsize()} file(s) found.")
        logger.debug(f"Scan completed in {duration:.2f} sec.")

        # Copy files to a remote location.
        if awaiting.qsize() != 0:
            start = time.time()
            threads = []
            for _ in range(pool_size):
                t = threading.Thread(target=porter.run)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            wiper.run()
            end = time.time()
            duration = end - start
            logger.debug(f"Processing completed in {duration:.2f} sec.")

        # Go to slumber for a given time interval.
        logger.info(f"Next scan in {delay} sec.")
        time.sleep(delay)
