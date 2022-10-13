import inspect
import sys
import warnings
from typing import Any


def showwarning(message, category, filename, lineno, file=None, line=None):
    _ = category
    _ = filename
    _ = lineno
    print(message, file=sys.stderr)


warnings.showwarning = showwarning


def retrieve_name(var):
    """Returns the name of a variable from 2 frames back (i.e. the method calling the caller of retrieve_name)."""
    # modified from https://stackoverflow.com/questions/18425225/getting-the-name-of-a-variable-as-a-string
    callers_parents_vars = inspect.currentframe().f_back.f_back.f_locals.items()
    return next(
        iter(var_name for var_name, var_val in callers_parents_vars if var_val is var),
        None,
    )


def deprecated_method_argument(
    argument: Any, instead_argument: Any = None, instead_default_value: Any = None
):
    argument_name = retrieve_name(argument)
    use_instead_argument = retrieve_name(instead_argument)
    if argument is not None:
        warn_msg = f"{argument_name} is deprecated"
        if use_instead_argument:
            warn_msg += f", use {use_instead_argument} instead"
        warnings.warn(warn_msg, FutureWarning)
        if instead_argument != instead_default_value:
            raise ValueError(
                f"{argument_name} and {use_instead_argument} cannot be used at the same time"
            )
