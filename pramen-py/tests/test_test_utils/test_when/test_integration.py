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

import pytest

import tests.test_test_utils.test_when

from pramen_py.test_utils.when import Markers


class Klass1:
    def some_method(
        self,
        arg1: str,
        arg2: int,
        *,
        kwarg1: str,
        kwarg2: str,
    ) -> str:
        return "Not mocked"

    def some_method_with_defaults(
        self,
        arg1: str,
        arg2: int,
        *,
        kwarg1: str,
        kwarg2: str = "some default string",
    ) -> str:
        return "Not mocked"

    @classmethod
    def some_class_method(
        cls,
        arg1: str,
        arg2: int,
        *,
        kwarg1: str,
        kwarg2: str,
    ) -> str:
        return "Not mocked"


class Klass2:
    def some_method(
        self,
        arg1: str,
        arg2: int,
        *,
        kwarg1: str,
        kwarg2: str,
    ) -> str:
        return "Not mocked"


def test_should_properly_patch_calls(when):
    when(Klass1, "some_method").called_with(
        "a",
        Markers.any,
        kwarg1="b",
        kwarg2=Markers.any,
    ).then_return("Mocked")

    assert (
        Klass1().some_method(
            "a",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Mocked"
    )
    assert (
        Klass1().some_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )


@pytest.mark.skip(
    reason=(
        "bug in unittest, "
        "see https://github.com/python/cpython/issues/67267"
    )
)
def test_should_work_with_classmethods(when):
    when(Klass1, "some_class_method").called_with(
        "a",
        Markers.any,
        kwarg1="b",
        kwarg2=Markers.any,
    ).then_return("Mocked")

    assert (
        Klass1().some_class_method(
            "a",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Mocked"
    )
    assert (
        Klass1.some_class_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )


def test_should_work_with_normal_functions(when):
    when(tests.test_test_utils.test_when, "some_normal_function").called_with(
        "a",
        Markers.any,
        kwarg1="b",
        kwarg2=Markers.any,
    ).then_return("Mocked")

    assert (
        tests.test_test_utils.test_when.some_normal_function(
            "a",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Mocked"
    )
    assert (
        tests.test_test_utils.test_when.some_normal_function(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )


def test_should_be_able_to_patch_multiple_calls(when):
    when(Klass1, "some_method").called_with(
        "a",
        Markers.any,
        kwarg1="b",
        kwarg2=Markers.any,
    ).then_return("Mocked first time")

    when(Klass1, "some_method").called_with(
        Markers.any,
        1,
        kwarg1=Markers.any,
        kwarg2="b",
    ).then_return("Mocked second time")

    assert (
        Klass1().some_method(
            "a",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Mocked first time"
    )
    assert (
        Klass1().some_method(
            "any",
            1,
            kwarg1="any too",
            kwarg2="b",
        )
        == "Mocked second time"
    )
    assert (
        Klass1().some_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )


def test_should_be_able_to_patch_multiple_objects(when):
    when(Klass1, "some_method").called_with(
        "a",
        Markers.any,
        kwarg1="b",
        kwarg2=Markers.any,
    ).then_return("Mocked Klass1")

    when(Klass2, "some_method").called_with(
        "b",
        Markers.any,
        kwarg1="c",
        kwarg2=Markers.any,
    ).then_return("Mocked Klass2")

    assert (
        Klass1().some_method(
            "a",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Mocked Klass1"
    )
    assert (
        Klass1().some_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )

    assert (
        Klass2().some_method(
            "b",
            1,
            kwarg1="c",
            kwarg2="any",
        )
        == "Mocked Klass2"
    )
    assert (
        Klass2().some_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )


def test_should_work_with_default_params_in_functions(when):
    patched_klass = (
        when(Klass1, "some_method_with_defaults")
        .called_with(
            Markers.any,
            Markers.any,
            kwarg1=Markers.any,
            kwarg2="some default string",
        )
        .then_return("Mocked")
    )

    assert (
        Klass1().some_method_with_defaults(
            "a",
            1,
            kwarg1="b",
        )
        == "Mocked"
    )
    assert (
        Klass1().some_method(
            "not mocked param",
            1,
            kwarg1="b",
            kwarg2="c",
        )
        == "Not mocked"
    )
    patched_klass.assert_called()
