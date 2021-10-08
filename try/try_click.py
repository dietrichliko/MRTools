#!/usr/bin/env python

from typing import Any

import click


class Cli:
    def __init__(self) -> None:

        self.options: List[Any] = []

    def option(self, *args, **kwargs):

        self.options.append(click.option(*args, **kwargs))

    def run(self):
        @click.group()
        def cli(**opts):
            print(opts)
            pass

        @cli.command()
        def one(**opts):
            print(opts)
            click.echo("one")

        for opt in self.options:
            one = opt(one)

        @cli.command()
        def two():
            click.echo("two")

        cli()


def main():

    cli = Cli()
    cli.option("--list/--no-list")
    cli.option("--test")
    cli.run()


if __name__ == "__main__":

    main()
