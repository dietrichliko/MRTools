"""Just a demonstration of clicklog

Usage:
    example_clicklog.py --log-level DEGUG

"""

import logging

import click

import mrtools.clicklog as clicklog

clicklog.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%y-%m-%d %H:%M:%S"
)
log = logging.getLogger()


@click.command()
@clicklog.log_level_option(log)
def main():

    log.debug("This is a debug message")
    log.info("This is an info message")
    log.warning("This is a warning message")
    log.error("This is an error message")
    log.fatal("This is a fatal message")
