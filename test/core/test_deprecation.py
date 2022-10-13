import pytest

from data_check.utils import deprecation


def test_retrieve_name():
    var = "test1"
    name = deprecation.retrieve_name(var)
    assert name is None


def test_retrieve_name_nested():
    var = "test1"

    def func(other_var):
        name = deprecation.retrieve_name(other_var)
        assert name == "var"

    func(var)


def test_deprecated_method_argument():
    with pytest.warns(FutureWarning, match=r"arg is deprecated$"):
        arg = "test"
        deprecation.deprecated_method_argument(arg)


def test_deprecated_method_argument_no_warning():
    arg = None
    deprecation.deprecated_method_argument(arg)


def test_deprecated_method_argument_instead_argument():
    with pytest.warns(FutureWarning, match=r"arg is deprecated, use arg2 instead$"):
        arg = "test"
        arg2 = None
        deprecation.deprecated_method_argument(arg, arg2)


def test_deprecated_method_argument_instead_argument_not_none():
    with pytest.raises(
        ValueError, match=r"arg and arg2 cannot be used at the same time$"
    ):
        arg = "test"
        arg2 = "not none"
        deprecation.deprecated_method_argument(arg, arg2)


def test_deprecated_method_argument_instead_argument_default_value():
    with pytest.warns(FutureWarning, match=r"arg is deprecated, use arg2 instead$"):
        arg = "test"
        arg2 = "test2"
        deprecation.deprecated_method_argument(arg, arg2, "test2")


def test_deprecated_method_argument_instead_argument_default_value_none():
    with pytest.warns(FutureWarning, match=r"arg is deprecated, use arg2 instead$"):
        arg = "test"
        arg2 = None
        deprecation.deprecated_method_argument(arg, arg2, None)
