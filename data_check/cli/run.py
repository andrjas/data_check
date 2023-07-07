from pathlib import Path
from typing import List, Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options, get_data_check


@click.command()
@common_options
@click.option("--print", "print_failed", is_flag=True, help="print failed results")
@click.option(
    "--print-format",
    "print_format",
    type=str,
    default=DataCheckConfig.default_print_format,
    help=(
        "format for printing failed results (pandas, csv, json); "
        f"default: {DataCheckConfig.default_print_format}"
    ),
)
@click.option(
    "--print-json",
    is_flag=True,
    help="shortcut for --print --print-format json",
)
@click.option(
    "--diff",
    "print_diffed",
    is_flag=True,
    help="print only the different columns for failed results",
)
@click.argument("files", nargs=-1, type=click.Path())
@click.pass_context
def run(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    use_process: bool = DataCheckConfig.use_process,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    print_failed: bool = False,
    print_format: str = DataCheckConfig.default_print_format,
    print_json: bool = False,
    print_diffed: bool = False,
    files: List[Union[str, Path]] = [],
):
    """Run checks (default command)."""
    dc = get_data_check(
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

    if print_json:
        print_failed = True
        print_format = "json"

    dc.output.configure_print(
        print_failed=print_failed,
        print_format=print_format,
        print_diffed=print_diffed,
    )

    dc.load_template()
    dc.load_lookups()

    if not files:
        files = [dc.config.checks_path]  # use default checks path if nothing is given
    path_list = [Path(f) for f in files]

    all_checks = dc.collect_checks(path_list)
    result = dc.run_checks(all_checks)
    overall_result = dc.get_overall_result(
        result, print_overall=False, print_summary=True
    )
    if not overall_result:
        ctx.exit(1)
