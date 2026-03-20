import logging
from collections.abc import Callable
from typing import TypeVar

from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


def default_bar(disable=False) -> Progress:
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        refresh_per_second=1,
        disable=disable,
    )


def setup_logging(level: int = logging.INFO, main_logger: str = "main"):
    logging.basicConfig(
        level=max(logging.INFO, level), format="%(message)s", handlers=[RichHandler()]
    )
    logging.getLogger(main_logger).setLevel(level)


_T = TypeVar("_T")


def bisect(a: list[_T], test_fn: Callable[[_T], bool]) -> int:
    """Find the first index of `a` that does not satisfy `test_fn`,
    assuming that `test_fn` is monotonous.

    Parameters
    ----------
    a : list[_T]
        List of elements to test
    test_fn : Callable[[_T], bool]
        Test function. Should be monotonous decreasing over `a`,
        i.e. it returns True for some first number of elements, then False.

    Returns
    -------
    int
        The index of the first element of `a` to not satisfy `test_fn`,
        or `len(a)` if all elements satisfy it.

    """
    lo, hi = 0, len(a)
    while hi - lo >= 1:
        mid = (lo + hi) // 2
        if test_fn(a[mid]):
            lo = mid + 1
        else:
            hi = mid
    return lo
