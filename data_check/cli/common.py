import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any, Callable, Optional, Union, cast

import click

from data_check.config import DataCheckConfig
from data_check.data_check import DataCheck

DEFAULT_VALUES: dict[str, Any] = {
    "connection": "",
    "workers": DataCheckConfig.parallel_workers,
    "use_process": DataCheckConfig.use_process,
    "config": DataCheckConfig.config_path,
    "verbose": False,
    "traceback": False,
    "quiet": False,
    "log": None,
}


def common_options(function: Callable[..., Any]) -> Callable[..., Any]:
    function = click.option(
        "--connection",
        "-c",
        type=str,
        help="connection to use",
        envvar="DATA_CHECK_CONNECTION",
    )(function)
    function = click.option("--verbose", is_flag=True, help="print verbose output")(
        function
    )
    function = click.option(
        "--traceback", is_flag=True, help="print traceback output for debugging"
    )(function)
    function = click.option("--quiet", is_flag=True, help="do not print any output")(
        function
    )
    function = click.option(
        "--log", type=click.Path(), help="write output to a log file"
    )(function)

    function = click.option(
        "--workers",
        "-n",
        type=int,
        default=DataCheckConfig.parallel_workers,
        help=(
            "parallel workers to run queries "
            f"(default: {DataCheckConfig.parallel_workers})"
        ),
    )(function)

    function = click.option(
        "--use-process",
        is_flag=True,
        help=("use processes instead of threads"),
    )(function)

    function = click.option(
        "--config",
        type=str,
        default=DataCheckConfig.config_path,
        help=f"config file to use (default: {DataCheckConfig.config_path})",
    )(function)

    function = click.version_option(version=cast(str, version("data-check")))(function)
    return function


def init_common(  # noqa: PLR0913
    ctx: click.Context,
    connection: str = DEFAULT_VALUES["connection"],
    workers: int = DEFAULT_VALUES["workers"],
    use_process: bool = DEFAULT_VALUES["use_process"],
    config: Union[str, Path] = DEFAULT_VALUES["config"],
    verbose: bool = DEFAULT_VALUES["verbose"],
    traceback: bool = DEFAULT_VALUES["traceback"],
    quiet: bool = DEFAULT_VALUES["quiet"],
    log: Optional[Union[str, Path]] = DEFAULT_VALUES["log"],
):
    ctx.ensure_object(dict)

    config = Path(config)
    set_once(ctx, "connection", connection)
    set_once(ctx, "workers", workers)
    set_once(ctx, "use_process", use_process)
    set_once(ctx, "config", config)
    set_once(ctx, "verbose", verbose)
    set_once(ctx, "traceback", traceback)
    set_once(ctx, "quiet", quiet)
    set_once(ctx, "log", log)


def get_config(ctx: click.Context) -> DataCheckConfig:
    if "dc_config" in ctx.obj:
        return ctx.obj["dc_config"]
    config = get_value(ctx, "config")
    connection = get_value(ctx, "connection")
    workers = get_value(ctx, "workers")
    use_process = get_value(ctx, "use_process")
    log = get_value(ctx, "log")
    dc_config = (
        DataCheckConfig(config_path=config).load_config().set_connection(connection)
    )
    dc_config.parallel_workers = workers
    dc_config.use_process = use_process
    if log:
        dc_config.log_path = Path(log)
    if not dc_config.connection:
        click.echo(f"unknown connection: {connection}")
        sys.exit(1)
    ctx.obj["dc_config"] = dc_config
    return dc_config


def set_once(ctx: click.Context, obj: str, value: Any):
    default_value = DEFAULT_VALUES[obj]
    if value != default_value and value is not None:
        ctx.obj[obj] = value


def get_value(ctx: click.Context, obj: str) -> Any:
    return ctx.obj.get(obj, DEFAULT_VALUES[obj])


def get_data_check(  # noqa: PLR0913
    ctx: click.Context,
    connection: str,
    workers: int,
    use_process: bool,
    config: Union[str, Path],
    verbose: bool,
    traceback: bool,
    quiet: bool,
    log: Optional[Union[str, Path]],
) -> DataCheck:
    init_common(
        ctx=ctx,
        connection=connection,
        workers=workers,
        use_process=use_process,
        config=config,
        verbose=verbose,
        traceback=traceback,
        quiet=quiet,
        log=log,
    )

    dc_config = get_config(ctx)

    verbose = get_value(ctx, "verbose")
    traceback = get_value(ctx, "traceback")
    quiet = get_value(ctx, "quiet")

    dc = DataCheck(config=dc_config)
    dc.output.configure_output(
        verbose=verbose,
        traceback=traceback,
        quiet=quiet,
        # set log_path from config, so we can also use it from the config file
        log_path=dc_config.log_path,
        printer=click.echo,
    )
    return dc
