#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio

import click

from click import Context, Option
from loguru import logger

from pramen_py import Transformation
from pramen_py.app import env
from pramen_py.app.logger import setup_logger
from pramen_py.runner.runner_transformation import TransformationsRunner
from pramen_py.utils import coro


CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "auto_envvar_prefix": "PRAMENPY",
}


def set_logger_callback(
    _: Context,
    __: Option,
    value: bool,
) -> bool:
    """Set debug on --verbose option.

    $ pramen-py --verbose ...
    """
    if value:
        logger.remove()
        setup_logger("DEBUG", env)

    logger.debug("DEBUG is on")
    return value


class RunGroup(click.Group):
    """Group of available transformations commands.

    We have to extend from click.Group in order to load at click.Group
    initialization available transformations dynamically from the
    transformations namespace packages.

    At this point all available transformations are initialized
    """

    def __init__(self, *args, **kwargs):  # type: ignore
        super().__init__(*args, **kwargs)
        TransformationsRunner(cli=self).activate()


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    is_eager=True,
    callback=set_logger_callback,
)
@click.version_option()
@click.pass_context
@coro
async def main(ctx: Context, verbose: bool) -> None:
    """Defining transformations for pramen in python.

    Usage examples:

    $ pramen-py transformations run
     ExampleTransformation1
     --info-date 2022-03-01
     --config tests/resources/real_config.yaml

    Application is configured via environment variables.

    $ pramen-py list-configuration-options
    # to see available options
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@main.group()
@click.pass_context
@coro
async def transformations(
    ctx: Context,
) -> None:
    """Defines available actions for transformations."""


@transformations.command()
@click.pass_context
@coro
async def ls(ctx: Context) -> None:
    """List available transformations."""
    for t_name in set(  # noqa: C401
        t.__name__ for t in Transformation.__subclasses__()
    ):
        click.echo(t_name)


@transformations.group(cls=RunGroup)
@click.pass_context
@coro
async def run(ctx: Context) -> None:
    """Run transformations."""


@main.group()
@coro
async def completions() -> None:
    """Get shell completions scripts.

    Use --help to see available shells.

    Example:
        $ source <(pramen-py completions zsh)
    """


@completions.command(help="zsh completions script")
@coro
async def zsh() -> None:
    proc = await asyncio.subprocess.create_subprocess_shell(
        "_pramen_py_COMPLETE=zsh_source pramen-py",
        stdout=asyncio.subprocess.PIPE,
    )
    res, _ = await proc.communicate()
    click.echo(res.decode())


@completions.command(help="bash completions script")
@coro
async def bash() -> None:
    proc = await asyncio.subprocess.create_subprocess_shell(
        "_pramen_py_COMPLETE=bash_source pramen-py",
        stdout=asyncio.subprocess.PIPE,
    )
    res, _ = await proc.communicate()
    click.echo(res.decode())


@main.command()
@coro
async def list_configuration_options() -> None:
    """List of available environment variables to configure the app."""

    async def example_coroutine_function() -> str:
        with open(".env.example") as config_options:
            read_config_options = config_options.read()
        return read_config_options

    example_config_options = await example_coroutine_function()

    click.echo(example_config_options)
