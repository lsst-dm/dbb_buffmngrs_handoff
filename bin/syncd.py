import argparse
import os
import logging
import queue
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

    if args.validate:
        pass

    logging_opts = None
    try:
        logging_opts = config["logging"]
    except KeyError:
        pass
    set_logger(options=logging_opts)
    logger.info("Configuration read from '%s'." % filename)

    options = config["options"]
    delay = options["delay"]

    # Initialize the transfer queue.
    files = queue.Queue()

    logger.info("Starting...")

    host = config["host"]
    source = host["path"].rstrip(os.sep)
    num = options["scanners"]

    remote = config["remote"]
    target = "%s@%s:%s" % (remote["user"], remote["host"], remote["path"])

    previous = dict()
    while True:
        paths = [os.path.join(source, p) for p in os.listdir(source)]
        if not paths:
            time.sleep(delay)
            continue
        dirs = [p for p in paths if os.path.isdir(p)]

        # Create current snapshots of existing directories.
        snapshots = queue.Queue()

        start = time.time()
        batches = [dirs[i:i+num] for i in range(0, len(dirs), num)]
        for batch in batches:
            scanners = []
            for path in batch:
                s = Scanner(os.path.join(source, path), snapshots)
                s.start()
                scanners.append(s)
            for s in scanners:
                s.join()

        current = dict()
        while not snapshots.empty():
            s = snapshots.get()
            current[s.root] = s
        end = time.time()
        eta = end - start

        msg = "Scan completed; "
        msg += "no. of directories. : {num}, "
        msg += "duration: {eta:.2f} sec."
        logger.info(msg.format(num=len(current), eta=eta))

        # Initialize file transfer queue.
        files = queue.Queue()

        # Process snapshots corresponding to completely new directories.
        start = time.time()
        new = set(current) - set(previous)
        for head in new:
            for path in current[head].paths:
                tail = path.replace(head, "").lstrip("/")
                files.put((head, tail))

        # Compare snapshots of already existing directories.
        old = set(current) & set(previous)
        for head in old:
            for path in current[head].diff(previous[head]):
                tail = path.replace(head, "").lstrip("/")
                files.put((head, tail))
        end = time.time()
        eta = end - start

        msg = "Snapshots processed; "
        msg += "no. of changed files : {num}, "
        msg += "duration: {eta:.2f} sec."
        logger.info(msg.format(num=files.qsize(), eta=eta))

        # Transfer new files to designated location.
        start = time.time()
        porters = []
        for _ in range(options["porters"]):
            p = Porter(target, files, chunk_size=options["chunk_size"])
            p.start()
            porters.append(p)
        for p in porters:
            p.join()

        previous = current
        end = time.time()
        eta = end - start

        msg = "Transfer completed; "
        msg += "duration: {eta:.2f} sec."
        logger.info(msg.format(eta=eta))

        # Go to slumber for a given time interval.
        logger.info("Next scan in {} sec.".format(delay))
        time.sleep(delay)
