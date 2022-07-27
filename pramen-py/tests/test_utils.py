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

from functools import partial

import pytest

from pramen_py.test_utils import AsyncMock
from pramen_py.utils import run_and_retry


@pytest.mark.parametrize(
    (
        "side_effect",
        "trials",
        "expected_exc",
        "expected_result",
        "exception",
        "retry_hook_triggered_count",
    ),
    (
        #
        # normal cases for functions (the function fails
        #   on 1, 2nd call, but return 5 on the third. Because number of
        # trials are 3 the function should pass at the end
        (
            (ValueError, ValueError, 5),
            3,
            ValueError,
            5,
            None,
            2,
        ),
        #
        # setting number of trials to 2 so it should fail
        (
            (ValueError, ValueError, 5),
            2,
            ValueError,
            None,
            ValueError,
            2,
        ),
        #
        # Or function always fails
        (
            (ValueError, ValueError, ValueError),
            3,
            ValueError,
            None,
            ValueError,
            3,
        ),
        #
        # If wrong exception provided it should fail with real exception
        (
            (ValueError, ValueError, 5),
            3,
            TypeError,
            None,
            ValueError,
            0,
        ),
    ),
)
@pytest.mark.asyncio
async def test_run_and_retry(
    mocker,
    side_effect,
    trials,
    expected_exc,
    expected_result,
    exception,
    retry_hook_triggered_count,
):
    fn = mocker.AsyncMock(side_effect=side_effect)
    retry_hook = mocker.AsyncMock()

    if exception:
        with pytest.raises(exception):
            await run_and_retry(
                partial(fn, "some arg)"),
                number_of_trials=trials,
                exception=expected_exc,
                retry_hook=retry_hook,
            )
    else:
        res = await run_and_retry(
            partial(fn, "some arg"),
            number_of_trials=trials,
            exception=expected_exc,
            retry_hook=retry_hook,
        )
        assert res == expected_result
    assert retry_hook.mock.call_count == retry_hook_triggered_count


@pytest.mark.asyncio
async def test_run_and_retry_works_with_sync_async_func():
    def some_sync_foo(a, b):
        return a + b

    async def some_async_foo(a, b):
        return a + b

    assert 3 == await run_and_retry(
        partial(some_sync_foo, 1, 2),
        number_of_trials=2,
        exception=ValueError,
        retry_hook=AsyncMock(),
    )
    assert 3 == await run_and_retry(
        partial(some_async_foo, 1, 2),
        number_of_trials=2,
        exception=ValueError,
        retry_hook=AsyncMock(),
    )
