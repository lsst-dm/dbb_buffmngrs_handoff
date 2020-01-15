#!/usr/bin/env python

import argparse
import jsonschema
import logging
import os
import queue
import threading
import time
import yaml
import dbb_buffer_mngr as mgr


logger = logging.getLogger("dbb_buffer_mngr")


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


def set_logger(settings=None):
    """Configure logger.

    Parameters
    ----------
    settings : dict, optional
       Logger settings. If None (default), default settings will be used.
    """
    default_settings = {
        "file": None,
        "format": "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        "level": "WARNING",
    }
    if settings is None:
        settings = default_settings

    level_name = settings.get("level", default_settings["level"])
    level = getattr(logging, level_name.upper(), logging.WARNING)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    logfile = settings.get("file", default_settings["file"])
    if logfile is not None:
        handler = logging.FileHandler(logfile)
    logger.addHandler(handler)

    fmt = settings.get("format", default_settings["format"])
    formatter = logging.Formatter(fmt=fmt, datefmt=None)
    handler.setFormatter(formatter)


def background_thread(cmd, pause=1):
    """Run command in the background.

    Parameters
    ----------
    cmd :
        Command to run in the background
    pause : int, optional
        Amount of seconds to pause between consecutive executions of the command.
    """
    while True:
        cmd.run()
        time.sleep(pause)


if __name__ == "__main__":
    args = parse_args()

    # Read provided configuration or use the default one.
    if args.config is None:
        root = os.getcwd()
        filename = os.path.join(root, "etc/syncd.yml")
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
    logger_settings = config.get("logging", None)
    set_logger(settings=logger_settings)
    logger.info(f"Configuration read from '{filename}'.")

    logger.info("Starting...")

    handoff = config["handoff"]
    endpoint = config["endpoint"]

    default_options = {
        "chunk_size": 1,
        "timeout": None,
        "pause": 1,
        "transfer_pool": 1,
    }
    options = config.get("general", None)
    if options is None:
        options = default_options
    delay = options.get("pause", default_options["pause"])
    chunk_size = options.get("chunk_size", default_options["chunk_size"])
    timeout = options.get("timeout", default_options["timeout"])
    pool_size = options.get("transfer_pool", default_options["transfer_pool"])

    awaiting = queue.Queue()
    completed = queue.Queue()

    scanner = mgr.Scanner(handoff, awaiting)
    cleaner = mgr.Cleaner(handoff, completed)
    porter = mgr.Porter(endpoint, awaiting, completed,
                        chunk_size=chunk_size, timeout=timeout)
    wiper = mgr.Wiper(endpoint)

    daemon = threading.Thread(target=background_thread,
                              args=(cleaner,), kwargs=dict(pause=delay),
                              daemon=True)
    daemon.start()

    while True:
        # Scan source location for files.
        start = time.time()
        scanner.run()
        end = time.time()
        eta = end - start
        logger.info(f"Scan completed in {eta:.2f} sec.")
        logger.debug(f"Number of files found: {awaiting.qsize()}.")

        # Copy files to a remote location.
        start = time.time()
        threads = []
        for _ in range(pool_size):
            t = threading.Thread(target=porter.run)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if completed.qsize() != 0:
            wiper.run()
        end = time.time()
        eta = end - start
        logger.info(f"File transfer completed in {eta:.2f} sec.")
        logger.debug(f"Number of files transferred: {completed.qsize()}.")

        # Go to slumber for a given time interval.
        logger.info(f"Next scan in {delay} sec.")
        time.sleep(delay)
