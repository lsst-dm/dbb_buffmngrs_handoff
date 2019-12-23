import argparse
import jsonschema
import logging
import os
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
    logger.info("Configuration read from '{fn}'.".format(fn=filename))

    if args.validate:
        try:
            jsonschema.validate(instance=config, schema=SCHEMA)
        except jsonschema.ValidationError as ex:
            msg = "Configuration error: {err}.".format(err=ex.message)
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
    target = "{u}@{h}:{p}".format(u=remote["user"], h=remote["host"], p=remote["path"])

    options = config["general"]
    delay = options.get("delay", 1)
    size = options.get("chunk_size", 10)

    # Initialize the transfer queue.
    files = queue.Queue()

    while True:
        # Scan source location for files.
        start = time.time()
        s = Scanner(buffer, files)
        s.start()
        s.join()
        end = time.time()
        eta = end - start
        msg = "Scan of {loc} completed in {eta:.2f} sec.: {num} files found."
        logger.info(msg.format(loc=buffer, eta=eta, num=files.qsize()))

        # Transfer new files to designated location.
        start = time.time()
        porters = []
        for _ in range(options["porters"]):
            p = Porter(target, files, chunk_size=size, holding_area=storage)
            p.start()
            porters.append(p)
        for p in porters:
            p.join()
        end = time.time()
        eta = end - start
        msg = "File transfer completed in {eta:.2f} sec."
        logger.info(msg.format(eta=eta))

        # Go to slumber for a given time interval.
        logger.info("Next scan in {} sec.".format(delay))
        time.sleep(delay)
