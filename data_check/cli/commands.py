import click
from pathlib import Path
from typing import Callable, Any
from functools import partial

from .params import RunParams


def ping_cmd(res: RunParams):
    test = res.data_check.sql.test_connection()
    if test:
        res.ctx.exit(0)
    else:
        res.ctx.exit(1)


def load_cmd(res: RunParams, load: Path):
    if not res.table:
        click.echo("--table must be specified")
        res.ctx.exit(1)
    else:
        res.data_check.sql.table_loader.load_table_from_file(
            table=res.table,
            file=load,
            load_mode=res.load_mode,
        )
        res.ctx.exit(0)


def load_tables_cmd(res: RunParams):
    path_list = [Path(f) for f in res.files]
    res.data_check.sql.table_loader.load_tables_from_files(
        path_list, load_mode=res.load_mode
    )
    res.ctx.exit(0)


def sql_files_cmd(res: RunParams):
    res.data_check.load_template()
    res.data_check.load_lookups()
    path_list = [Path(f) for f in res.files]
    res.data_check.run_sql_files(path_list)
    res.ctx.exit(0)


def sql_cmd(res: RunParams, sql: str):
    res.data_check.load_template()
    res.data_check.load_lookups()
    if res.write_check:
        result = res.data_check.write_check(sql, Path(res.write_check))
    else:
        result = res.data_check.run_sql_query(sql, output=res.output)
    if result:
        res.ctx.exit(0)
    else:
        res.ctx.exit(1)


def run_cmd(res: RunParams):
    if not res.files:
        res.files = [
            res.data_check.config.checks_path
        ]  # use default checks path if nothing is given
    path_list = [Path(f) for f in res.files]
    res.data_check.load_template()
    res.data_check.load_lookups()
    result = res.data_check.run(path_list)
    if not result:
        res.ctx.exit(1)


def get_callback(
    cmd: Callable[..., None], add_partial_arg: bool = False
) -> Callable[[click.Context, str, Any], None]:
    cmd_name = cmd.__name__.replace("_cmd", "")

    def cb(ctx: click.Context, _: str, value: Any):
        if not value or ctx.resilient_parsing:
            return
        if add_partial_arg:
            ctx.obj = partial(cmd, **{cmd_name: value})
        else:
            ctx.obj = cmd

    return cb
