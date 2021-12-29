from __future__ import annotations
from os import linesep
import pandas as pd
from typing import cast, TYPE_CHECKING

from data_check.result import DataCheckResult

if TYPE_CHECKING:
    from data_check.output.output import DataCheckOutput


def format_data_check_result(output: DataCheckOutput, result: DataCheckResult):
    if result.exception:
        _set_exception_message(output, result)
    elif result.message:
        pass
    elif result.passed:
        _set_passed_message(output, result)
    else:
        _set_failed_message(output, result)


def _set_exception_message(output: DataCheckOutput, result: DataCheckResult):
    fail = output.str_fail(f"FAILED (with exception in {result.source})")
    result.message = f"{result.source}: {fail}"
    result.log_message = (
        result.message
        + linesep
        + "".join(output.format_exception(cast(Exception, result.exception)))
    )
    if output.verbose:
        result.message += linesep + str(result.exception)
    if output.traceback:
        result.message = result.log_message


def _set_passed_message(output: DataCheckOutput, result: DataCheckResult):
    result.message = f"{result.source}: {output.passed_message}"
    if output.print_failed and output.verbose:
        result.message += linesep + output.pprint_result(result)


def _set_failed_message(output: DataCheckOutput, result: DataCheckResult):
    if result.source_message:
        result.message = f"{result.source}: {result.source_message}"
    else:
        result.message = f"{result.source}: {output.failed_message}"
    if result.extra_message:
        result.message += f" ({result.extra_message})"

    result.log_message = result.message
    if isinstance(result.result, pd.DataFrame):
        result.log_message += linesep + output.pprint_result(result)
    elif isinstance(result.result, list):
        failure_message = ""
        for failure in result.result:
            failure_message += (
                linesep
                + failure[0]
                + ":"
                + linesep
                + output.pprint_result(failure[1].copy())
            )
        result.log_message += failure_message
    if output.print_failed:
        result.message = result.log_message
