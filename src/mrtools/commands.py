"""Click based cli for Analyzers

Usage:

    def main():

        config.init()
        analyzer = EX02Analyzer()

        cli = mrtools.AnalyzerCli(analyzer)
        cli.option(
            "--year",
            type=click.Choice(['2016', '2017', '2018']),
            default="2016",
            help="Use data from this year",
        )
        cli.run()


    if __name__ == "__main__":
        main()
"""
import logging
from os import readlink
import pathlib
from typing import Optional, Union, Any, List, Dict, Callable

import click

from mrtools.analyzer import Analyzer
from mrtools.samples import SampleGroup
from mrtools.samples_cache import SamplesCache
from mrtools.config import Configuration
import mrtools.clicklog as clicklog

log = logging.getLogger(__package__)

config = Configuration()


def sc_options(f: Callable) -> Callable:
    """Common decorator for sample cache options

    The options are passed as a dictionary in the contex opbject.
    """

    def callback(ctx, param, value):
        state = ctx.ensure_object(dict)
        state[param.name] = value
        return value

    f = click.option(
        "--remote/--no-remote",
        default=False,
        expose_value=False,
        callback=callback,
        help="Files can be on remote sites",
        show_default=True,
    )(f)
    f = click.option(
        "--refresh/--no-refresh",
        default=False,
        expose_value=False,
        callback=callback,
        help="Refresh the Sample Cache",
        show_default=True,
    )(f)
    f = click.option(
        "--root-threads",
        metavar="THREADS",
        type=int,
        default=None,
        expose_value=False,
        callback=callback,
        help="Number of threads for ROOT [default from config]",
    )(f)
    f = click.option(
        "--threads",
        type=int,
        metavar="THREADS",
        default=None,
        expose_value=False,
        callback=callback,
        help="Number of threads [default from config]",
        show_default=True,
    )(f)

    return f


class AnalyzerCli:
    """Click based cli for the Analyzer"""

    analyzer: Analyzer
    options: List[Callable]

    def __init__(self, analyzer: Analyzer) -> None:

        self.analyzer = analyzer
        self.options = []

    def option(self, *args, **kwargs) -> None:
        """Add user specific click options"""

        self.options.append(click.option(*args, **kwargs))

    def run(self) -> None:
        """Setup the commands with all options and run click"""

        @click.group()
        @sc_options
        @click.option(
            "--config-file",
            type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
            default=None,
            help="[default: ~/.config/mrtools.mrtools.toml]",
        )
        @click.option(
            "--site",
            metavar="SITE",
            default="",
            help="Force a site specific configuration",
        )
        @clicklog.log_level_option(log)
        def cli(config_file: Optional[Union[pathlib.Path, str]], site: str):
            """Modern ROOT Tools allow to run an analysis code on a sample"""

            config.init(config_file, site)

        @cli.command()
        def run(**user_options):
            """Run the analysis on selected samples"""
            if len(user_options):
                log.info("User Options: %s", str(user_options))
            log.fatal("Not implemented yet")

        @cli.command()
        def prun(**user_options):
            """Run the analysis on selected samples in parallel using dask"""

            if len(user_options):
                log.info("User Options: %s", str(user_options))
            log.fatal("Not implemented yet")

        @cli.command()
        def submit(**user_options):
            """Run the analysis by submitting jobs to the cluster"""

            if len(user_options):
                log.info("User Options: %s", str(user_options))
            log.fatal("Not implemented yet")

        @cli.command()
        @click.pass_obj
        def list(sc_options: Dict[str, Any], **user_options):
            """List the samples defined for the analysis"""

            with SamplesCache(**sc_options) as sc:

                samples = self.analyzer.samples(sc, user_options)

                for sample in samples:
                    click.echo(f"{sample!r}")
                    if isinstance(sample, SampleGroup):
                        for subsample in sample.samples:
                            click.echo(f"   {subsample}")

        @cli.command()
        @click.option("--entries/--no-entries", default=False)
        @click.pass_obj
        def verify(sc_options: Dict[str, Any], **user_options):
            """List the samples defined for the analysis"""

            with SamplesCache(**sc_options) as sc:

                samples = self.analyzer.samples(sc, user_options)

                for sample in samples:
                    click.echo(f"{sample}")
                    if isinstance(sample, SampleGroup):
                        for subsample in sample.samples:
                            click.echo(f"   {subsample!r}")

        # adding user specific parameters
        for option in self.options:
            run = option(run)
            list = option(list)
            verify = option(verify)

        cli()  # this starts the click parsing
