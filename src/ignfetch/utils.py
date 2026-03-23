import logging
from collections.abc import Callable
from typing import TypeVar

from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text


class MofNMaybeBytes(ProgressColumn):
    """Shows M/N count or MB/MB depending on is_byte field.

    If task.fields.get("is_byte") is True, renders as "{completed:.1f}/{total:.1f} MB".
    Otherwise, renders as "{completed}/{total}" with zero-padding like MofNCompleteColumn.

    Parameters
    ----------
    separator : str
        Text to separate completed and total values. Defaults to "/".
    **kwargs
        Additional arguments passed to parent ProgressColumn.
    """

    def __init__(self, separator: str = "/", **kwargs):
        super().__init__(**kwargs)
        self.separator = separator

    def render(self, task: Task) -> Text:
        """Render progress as M/N or MB/MB based on is_byte field.

        Parameters
        ----------
        task : Task
            The task to render progress for.

        Returns
        -------
        Text
            Formatted progress text.
        """
        is_byte = task.fields.get("is_byte", False)

        if is_byte:
            completed = task.completed / 1_000_000
            total = (task.total or 0) / 1_000_000
            return Text(f"{completed:.1f}{self.separator}{total:.1f} MB", style="cyan")
        else:
            completed = int(task.completed)
            total = int(task.total) if task.total is not None else 0

            completed_str = str(completed)
            total_str = str(total)
            completed_str = completed_str.rjust(len(total_str))
            return Text(f"{completed_str}{self.separator}{total_str}", style="cyan")


def default_bar(disable: bool = False) -> Progress:
    """Create a Progress instance with columns supporting both count and byte-based tracking.

    Parameters
    ----------
    disable : bool
        If True, disable the progress bar display. Defaults to False.

    Returns
    -------
    Progress
        A Progress instance with byte-aware columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNMaybeBytes(),
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
