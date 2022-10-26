#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import enum
import inspect

from typing import Any, Callable, Dict, Generic, Tuple, TypeVar
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture
from typing_extensions import ParamSpec, Protocol


class HasNameDunder(Protocol):
    __name__: str


_TargetClsType = TypeVar("_TargetClsType", bound=HasNameDunder)
_TargetMethodParams = ParamSpec("_TargetMethodParams")
_TargetMethodReturn = TypeVar("_TargetMethodReturn")
_TargetMethodKey = Tuple[str, str]

_TargetMethodArgs = Tuple[Any, ...]
_TargetMethodKwargs = Dict[str, Any]

_CallKeyParamDef = Tuple[str, Any]
_CallKey = Tuple[_CallKeyParamDef, ...]


class Markers(enum.Enum):
    """Markers for defining When.called_with arguments.

    Markers.any - means the argument could be anything
    """

    any: str = "any"


def create_call_key(
    original_callable_sig: inspect.Signature,
    *args: _TargetMethodArgs,
    **kwargs: _TargetMethodKwargs,
) -> _CallKey:
    """Create a call identification key for the given function.

    This key represents a certain call to the function. The order
    of kwargs is not important and will be allocated based on order
    of the kwargs in the function signature.

    Supports normal functions as well as class methods.
    """
    original_params = original_callable_sig.parameters
    args_key: _CallKey = tuple(
        zip(tuple(original_params)[: len(args)], args),
    )

    def get_from_kwargs_or_default(k: str) -> Any:
        may_be_result = kwargs.get(k, original_params[k].default)
        if may_be_result is inspect.Parameter.empty:
            raise ValueError(
                f"Not specified parameter {k}. "
                f"Incompatible call specification args={args}, kwargs={kwargs}"
                f" to the function with signature {original_callable_sig}"
            )
        else:
            return may_be_result

    kwargs_key: _CallKey = tuple(
        (k, get_from_kwargs_or_default(k))
        for k in tuple(original_params)[len(args) :]
    )
    return args_key + kwargs_key


def get_mocked_call_result(
    original_callable_sig: inspect.Signature,
    mocked_calls: Dict[
        _CallKey,
        _TargetMethodReturn,
    ],
    *args: _TargetMethodArgs,
    **kwargs: _TargetMethodKwargs,
) -> _TargetMethodReturn:
    call_key = create_call_key(
        original_callable_sig,
        *args,
        **kwargs,
    )

    def params_are_compatible(
        param_in_mocked_calls: _CallKeyParamDef,
        param_in_call: _CallKeyParamDef,
    ) -> bool:
        assert param_in_mocked_calls[0] == param_in_call[0]
        return (
            param_in_mocked_calls[1] is Markers.any
            or param_in_mocked_calls[1] == param_in_call[1]
        )

    def call_matched_call_key(mocked_call_key: _CallKey) -> bool:
        return all(
            params_are_compatible(param_1, param_2)
            for param_1, param_2 in zip(mocked_call_key, call_key)
        )

    for call in filter(call_matched_call_key, mocked_calls):
        return mocked_calls[call]
    else:
        raise KeyError(
            f"Call {call_key} is not in mocked_calls {mocked_calls}"
        )


def side_effect_factory(
    origin_callable: Callable[_TargetMethodParams, _TargetMethodReturn],
    mocked_calls: Dict[_CallKey, _TargetMethodReturn],
) -> Callable[_TargetMethodParams, _TargetMethodReturn]:
    def side_effect(
        *args: _TargetMethodParams.args,
        **kwargs: _TargetMethodParams.kwargs,
    ) -> _TargetMethodReturn:
        try:
            return get_mocked_call_result(
                inspect.signature(origin_callable),
                mocked_calls,
                # TODO investigate why mypy failing
                *args,  # type: ignore
                **kwargs,  # type: ignore
            )
        except KeyError:
            return origin_callable(*args, **kwargs)

    return side_effect


class MockedCalls(
    Generic[
        _TargetClsType,
        _TargetMethodParams,
        _TargetMethodReturn,
    ]
):
    mocked_calls_registry: Dict[
        _TargetMethodKey,
        Dict[_CallKey, _TargetMethodReturn],
    ] = {}

    def __init__(self, mocker: MockerFixture) -> None:
        self.mocker = mocker

    def add_call(
        self,
        cls: _TargetClsType,
        method: str,
        args: _TargetMethodArgs,
        kwargs: _TargetMethodKwargs,
        should_return: _TargetMethodReturn,
    ) -> MagicMock:
        self.mocked_calls_registry.setdefault(
            (cls.__name__, method),
            {},
        )
        self.mocked_calls_registry[(cls.__name__, method)][
            create_call_key(
                inspect.signature(getattr(cls, method)),
                *args,
                **kwargs,
            )
        ] = should_return

        return self.mocker.patch.object(
            cls,
            method,
            autospec=True,
            # it is important to send the origin target to the
            # side_effect_factory in order the result side_effect stores
            # the original target.
            side_effect=side_effect_factory(
                getattr(cls, method),
                self.mocked_calls_registry[(cls.__name__, method)],
            ),
        )


class When(
    Generic[
        _TargetClsType,
        _TargetMethodParams,
        _TargetMethodReturn,
    ]
):
    """Patching utility focused on readability.

    Example:

    >>> class Klass1:
    >>>     def some_method(
    >>>         self,
    >>>         arg1: str,
    >>>         arg2: int,
    >>>         *,
    >>>         kwarg1: str,
    >>>         kwarg2: str,
    >>>     ) -> str:
    >>>         return "Not mocked"


    >>> def test_should_properly_patch_calls(when):
    >>>     p = when(Klass1, "some_method").called_with(
    >>>         "a",
    >>>         Markers.any,
    >>>         kwarg1="b",
    >>>         kwarg2=Markers.any,
    >>>     ).then_return("Mocked")
    >>>
    >>>     assert (
    >>>         Klass1().some_method(
    >>>             "a",
    >>>             1,
    >>>             kwarg1="b",
    >>>             kwarg2="c",
    >>>         )
    >>>         == "Mocked"
    >>>     )
    >>>     assert (
    >>>         Klass1().some_method(
    >>>             "not mocked param",
    >>>             1,
    >>>             kwarg1="b",
    >>>             kwarg2="c",
    >>>         )
    >>>         == "Not mocked"
    >>>     )
    >>>     p.assert_called()

    It is possible to use 'when' with class methods and standalone functions
    (in this case cls parameter will become the python module).

    You can patch multiple times the same object with different "called_with"
    parameters in a single test.

    You can also patch multiple targets (cls, method)
    """

    cls: _TargetClsType
    method: str

    args: _TargetMethodArgs
    kwargs: _TargetMethodKwargs

    def __init__(self, mocker: MockerFixture):
        self.mocker = mocker
        self.mocked_calls = MockedCalls[
            _TargetClsType,
            _TargetMethodParams,
            _TargetMethodReturn,
        ](self.mocker)

    def __call__(
        self,
        cls: _TargetClsType,
        method: str,
    ) -> "When":  # type: ignore

        if inspect.ismethod(getattr(cls, method)):
            raise NotImplementedError(
                "See https://github.com/python/cpython/issues/67267 "
                "for more information"
            )

        def match_current_obj(patch) -> bool:  # type: ignore
            return patch.target is cls and patch.attribute == method

        # if current object was already patched, we have to re-patch it again
        for mocked_obj in filter(
            match_current_obj,
            self.mocker._patches,
        ):
            mocked_obj.stop()
            self.mocker._patches.remove(mocked_obj)

        self.cls = cls
        self.method = method
        return self

    def called_with(  # type: ignore
        self,
        *args,
        **kwargs,
    ) -> "When":  # type: ignore
        is_instance_method = (
            tuple(
                inspect.signature(
                    getattr(
                        self.cls,
                        self.method,
                    )
                ).parameters
            )[0]
            == "self"
        )
        # prepend Markers.any in case of a method (for self arg)
        self.args = (Markers.any,) + args if is_instance_method else args
        self.kwargs = kwargs
        return self

    def then_return(self, value: _TargetMethodReturn) -> MagicMock:
        return self.mocked_calls.add_call(
            self.cls,
            self.method,
            self.args,
            self.kwargs,
            value,
        )


@pytest.fixture
def when(mocker: MockerFixture) -> When:  # type: ignore
    """Patching utility focused on readability.

    Example:

    >>> class Klass1:
    >>>     def some_method(
    >>>         self,
    >>>         arg1: str,
    >>>         arg2: int,
    >>>         *,
    >>>         kwarg1: str,
    >>>         kwarg2: str,
    >>>     ) -> str:
    >>>         return "Not mocked"


    >>> def test_should_properly_patch_calls(when):
    >>>     when(Klass1, "some_method").called_with(
    >>>         "a",
    >>>         Markers.any,
    >>>         kwarg1="b",
    >>>         kwarg2=Markers.any,
    >>>     ).then_return("Mocked")

    >>>     assert (
    >>>         Klass1().some_method(
    >>>             "a",
    >>>             1,
    >>>             kwarg1="b",
    >>>             kwarg2="c",
    >>>         )
    >>>         == "Mocked"
    >>>     )
    >>>     assert (
    >>>         Klass1().some_method(
    >>>             "not mocked param",
    >>>             1,
    >>>             kwarg1="b",
    >>>             kwarg2="c",
    >>>         )
    >>>         == "Not mocked"
    >>>     )

    It is possible to use 'when' with class methods and standalone functions
    (in this case cls parameter will become the python module).

    You can patch multiple times the same object with different "called_with"
    parameters in a single test.

    You can also patch multiple targets (cls, method)
    """
    return When(mocker)
