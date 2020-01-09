import argparse
import jsonschema
import logging
import os
import queue
import threading
import time
import yaml
from dbb_buffer_mngr import Porter, Scanner, SCHEMA


logger = logging.getLogger("syncd")


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
       Logging options. If None, default logging configuration will be used.
    """
    formatter = logging.Formatter(fmt="%(asctime)s:%(levelname)s:%(message)s")
    handler = logging.StreamHandler()
    level = logging.WARN

    if options is not None:
        loglevel = options.get("loglevel", "WARNING")
        lvl = getattr(logging, loglevel.upper(), None)
        if isinstance(lvl, int):
            level = lvl

        logfile = options.get("logfile", None)
        if logfile is not None:
            handler = logging.FileHandler(logfile)

    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    args = parse_args()

    if args.config is None:
        root = os.getcwd()
        filename = os.path.join(root, "etc/syncd.yml")
    else:
        filename = args.config
    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    logger.info(f"Configuration read from '{filename}'.")

    if args.validate:
        try:
            jsonschema.validate(instance=config, schema=SCHEMA)
        except jsonschema.ValidationError as ex:
            msg = f"Configuration error: {ex.message}."
            logging.critical(msg)
            raise ValueError(msg)
        logger.info("Configuration validated successfully.")

    logging_opts = None
    try:
        logging_opts = config["logging"]
    except KeyError:
        pass
    set_logger(options=logging_opts)

    logger.info("Starting...")

    local = config["local"]
    buffer = local["buffer"]
    storage = local["storage"]

    remote = config["remote"]
    user = remote["user"]
    host = remote["host"]
    path = remote["path"]
    destination = f"{user}@{host}:{path}"

    options = config["general"]
    delay = options.get("delay", 1)
    size = options.get("chunk_size", 10)

    # Initialize the transfer queue.
    files = queue.Queue()

    scanner = Scanner(buffer, files)
    porter = Porter(destination, files, chunk_size=size, holding_area=storage)
    while True:
        # Scan source location for files.
        start = time.time()
        scanner.run()
        end = time.time()
        eta = end - start
        logger.info(f"Scan of {buffer} completed in {eta:.2f} sec.")
        logger.debug(f"Number of files found: {files.qsize()}.")

        # Transfer new files to designated location.
        start = time.time()
        threads = []
        for _ in range(options["porters"]):
            t = threading.Thread(target=porter.run)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        end = time.time()
        eta = end - start
        logger.info(f"File transfer completed in {eta:.2f} sec.")

        # Go to slumber for a given time interval.
        logger.info(f"Next scan in {delay} sec.")
        time.sleep(delay)
