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
