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

from typing import Any, cast
from unittest import mock


class AsyncMock:
    """Backport of AsyncMock to python3.6."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.mock = mock.Mock(*args, **kwargs)

    async def __call__(self, *args: Any, **kwargs: Any) -> mock.Mock:
        return cast(mock.Mock, self.mock(*args, **kwargs))

    def __getattr__(self, item: Any) -> Any:
        return getattr(self.mock, item)


mock.AsyncMock = cast(mock.Mock, AsyncMock)  # type: ignore
