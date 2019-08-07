"""param_val - static type checking for luigi parameters
To mypy, an IntParam looks like an instance of the class IntParam;
but at runtime, luigi actually picks an int value. So the type
of a class variable set to an IntParam should be int.
"""
from typing import Any, Callable, TypeVar, cast

import luigi


_T = TypeVar('_T')
_U = TypeVar('_U')


def _valueOf(example: _T, cls: Callable[..., _U]) -> Callable[..., _T]:

    def getValue(*args: Any, **kwargs: Any) -> _T:
        return cast(_T, cls(*args, **kwargs))
    return getValue


StrParam = _valueOf('s', luigi.Parameter)
IntParam = _valueOf(0, luigi.IntParameter)
BoolParam = _valueOf(True, luigi.BoolParameter)
DictParam = _valueOf({'k': 'v'}, luigi.DictParameter)
