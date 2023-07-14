import inspect
from pathlib import Path
from typing import List, Tuple

import pytest

from data_check.checks.pipeline_check.pipeline_check import PipelineCheck
from data_check.checks.pipeline_check.pipeline_model import STEP_TO_CLASS
from data_check.cli.main import cli
from data_check.data_check import DataCheck


@pytest.fixture
def pc(dc_serial: DataCheck) -> PipelineCheck:
    _pc = PipelineCheck(dc_serial, Path())
    return _pc


def get_command_names() -> List[str]:
    return [cmd for cmd in cli.commands.keys() if cmd not in IGNORE_COMMANDS]


def get_pipeline_step_names(pc: PipelineCheck) -> List[str]:
    return list(STEP_TO_CLASS.keys())


def get_common_params() -> List[str]:
    return [p.name for p in cli.params if p.name]


IGNORE_PARAMS = [
    ("run", "print_json"),
    ("run", "print_failed"),
    ("run", "print_format"),
    ("run", "print_diffed"),
]

IGNORE_COMMANDS = ["ping", "gen", "init"]


def get_command_param_names() -> List[Tuple[str, str]]:
    return [
        (cmd, param.name)
        for cmd in cli.commands.keys()
        for param in cli.commands[cmd].params
        if param.name not in get_common_params()
        and (cmd, param.name) not in IGNORE_PARAMS
        and cmd not in IGNORE_COMMANDS
        and param.name is not None
    ]


@pytest.mark.parametrize("command_name", get_command_names())
def test_cli_commands_have_pipeline_step(command_name, pc: PipelineCheck):
    assert command_name in get_pipeline_step_names(pc)


def get_pipeline_step_args(step_name: str) -> List[str]:
    step = STEP_TO_CLASS[step_name]
    return list(step.__fields__.keys())


@pytest.mark.parametrize(["cmd", "param"], get_command_param_names())
def test_cli_param_match_pipeline_param(cmd, param, pc: PipelineCheck):
    assert param in get_pipeline_step_args(cmd)
