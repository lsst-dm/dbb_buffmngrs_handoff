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
"""Definitions of commands recognized by the handoff buffer manager.
"""

import click
import jsonschema
import logging
import yaml
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from .declaratives import Base
from .manager import Manager
from .utils import setup_dbb_conn, setup_logging
from .validation import SCHEMA


logger = logging.getLogger("lsst.dbb.buffmngrs.handoff")


@click.group()
def cli():
    pass


@cli.command()
@click.option("--validate/--no-validate", default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def initdb(filename, validate):
    """Create database tables required by the manager.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(SCHEMA)
        try:
            jsonschema.validate(instance=configuration, schema=schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"configuration error: {ex.message}.")
        except jsonschema.SchemaError as ex:
            raise ValueError(f"schema error: {ex.message}.")
        return

    config = configuration.get("logging", None)
    setup_logging(options=config)

    config = configuration["database"]
    engine = setup_dbb_conn(config)
    try:
        Base.metadata.create_all(engine)
    except (DBAPIError, SQLAlchemyError) as ex:
        msg = f"cannot create tables: {ex.message}"
        logger.error(msg)
        raise RuntimeError(msg)


@cli.command()
@click.option("--validate/--no-validate", default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def dropdb(filename, validate):
    """Remove existing database tables.
    """
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(SCHEMA)
        try:
            jsonschema.validate(instance=configuration, schema=schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"configuration error: {ex.message}.")
        except jsonschema.SchemaError as ex:
            raise ValueError(f"schema error: {ex.message}.")
        return

    config = configuration.get("logging", None)
    setup_logging(options=config)

    config = configuration["database"]
    engine = setup_dbb_conn(config)
    try:
        Base.metadata.drop_all(engine)
    except (DBAPIError, SQLAlchemyError) as ex:
        msg = f"cannot remove tables: {ex.message}"
        logger.error(msg)
        raise RuntimeError(msg)


@cli.command()
@click.option("--validate/--no-validate", default=False,
              help="Validate configuration before starting the service.")
@click.argument("filename", type=click.Path(exists=True))
def run(filename, validate):
    with open(filename) as f:
        configuration = yaml.safe_load(f)
    if validate:
        schema = yaml.safe_load(SCHEMA)
        try:
            jsonschema.validate(instance=configuration, schema=schema)
        except jsonschema.ValidationError as ex:
            raise ValueError(f"configuration error: {ex.message}.")
        except jsonschema.SchemaError as ex:
            raise ValueError(f"schema error: {ex.message}.")
        return

    config = configuration.get("logging", None)
    setup_logging(options=config)

    mgr = Manager(configuration)
    mgr.run()


def main():
    """Start microservices for DBB endpoint manager.
    """
    return cli()
