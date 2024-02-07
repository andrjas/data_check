import inspect
import sys
import warnings
from typing import Any, Optional


def showwarning(message, category, filename, lineno, file=None, line=None):
    _ = category
    _ = filename
    _ = lineno
    print(message, file=sys.stderr)


warnings.showwarning = showwarning


def retrieve_name(var):
    """Returns the name of a variable from 2 frames back
    (i.e. the method calling the caller of retrieve_name).
    """
    # modified from https://stackoverflow.com/questions/18425225/getting-the-name-of-a-variable-as-a-string
    cur_frame = inspect.currentframe()
    if cur_frame is not None:
        next_frame = cur_frame.f_back
        if next_frame is not None and next_frame.f_back is not None:
            callers_parents_vars = next_frame.f_back.f_locals.items()
            return next(
                iter(
                    var_name
                    for var_name, var_val in callers_parents_vars
                    if var_val is var
                ),
                None,
            )


def retrieve_caller() -> str:
    """Returns the name of the calling method from 2 frames back
    (i.e. the method calling the caller of retrieve_caller).
    """
    cur_frame = inspect.currentframe()
    if cur_frame and cur_frame.f_back and cur_frame.f_back.f_back:
        return cur_frame.f_back.f_back.f_code.co_name
    return "UNKNOWN CALLER"


def deprecated_method_argument(
    argument: Any,
    instead_argument: Optional[Any] = None,
    instead_default_value: Optional[Any] = None,
):
    argument_name = retrieve_name(argument)
    use_instead_argument = retrieve_name(instead_argument)
    if argument is not None:
        warn_msg = f"{argument_name} is deprecated"
        if use_instead_argument:
            warn_msg += f", use {use_instead_argument} instead"
        warnings.warn(warn_msg, FutureWarning, stacklevel=2)
        if instead_argument != instead_default_value:
            raise ValueError(
                f"{argument_name} and {use_instead_argument} "
                "cannot be used at the same time"
            )


def deprecated_method(
    deprecated_method_name: Optional[str] = None, instead_method: Optional[str] = None
):
    if not deprecated_method_name:
        deprecated_method_name = retrieve_caller()
    warn_msg = f"{deprecated_method_name} is deprecated"
    if instead_method:
        warn_msg += f", use {instead_method} instead"
    warnings.warn(warn_msg, FutureWarning, stacklevel=2)
