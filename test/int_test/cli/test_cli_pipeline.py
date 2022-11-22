import inspect
from pathlib import Path
from typing import List, Tuple

import pytest

from data_check.checks.pipeline_check.pipeline_check import PipelineCheck
from data_check.cli.main import cli
from data_check.data_check import DataCheck


@pytest.fixture
def pc(dc_serial: DataCheck) -> PipelineCheck:
    _pc = PipelineCheck(dc_serial, Path("."))
    return _pc


def get_command_names() -> List[str]:
    return list(cli.commands.keys())


def get_pipeline_step_names(pc: PipelineCheck) -> List[str]:
    return list(pc.pipeline_steps.keys())


def get_common_params() -> List[str]:
    return [p.name for p in cli.params]


IGNORE_PARAMS = [
    ("run", "print_json"),
    ("run", "print_failed"),
    ("run", "print_format"),
    ("run", "print_diffed"),
]


def get_command_param_names() -> List[Tuple[str, str]]:
    return [
        (cmd, param.name)
        for cmd in cli.commands.keys()
        for param in cli.commands[cmd].params
        if param.name not in get_common_params()
        and (cmd, param.name) not in IGNORE_PARAMS
    ]


def get_pipeline_method(pc: PipelineCheck, cmd: str):
    return pc.pipeline_steps[cmd]["method"]


@pytest.mark.parametrize("command_name", get_command_names())
def test_cli_commands_have_pipeline_step(command_name, pc: PipelineCheck):
    assert command_name in get_pipeline_step_names(pc)


@pytest.mark.parametrize(["cmd", "param"], get_command_param_names())
def test_cli_param_match_pipeline_param(cmd, param, pc: PipelineCheck):
    method = get_pipeline_method(pc, cmd)
    argspec = inspect.getfullargspec(method)
    assert param in argspec.args
