from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from data_check.checks.base_check import BaseCheck


class DataCheckError(Exception):
    """
    Generic class for various exceptions that might occur in data_check.
    """

    pass


class ValidationError(Exception):
    """
    Wraps errors that occur during validation.
    """

    check: Optional[BaseCheck] = None
    original_exception: Optional[Exception] = None

    def __init__(
        self,
        msg: str,
        check: Optional[BaseCheck] = None,
        original_exception: Optional[Exception] = None,
    ):
        super().__init__(msg)
        self.check = check
        self.original_exception = original_exception


class TableDoesNotExistsError(Exception):
    """
    Thrown when a table does not exist.
    """

    pass
