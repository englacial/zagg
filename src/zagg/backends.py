"""Execution backends — *where* work runs, orthogonal to *what* runs.

zagg dispatch has two independent axes:

- **Pipeline type** (*what*): spatial / temporal / event — selected by
  :func:`zagg.config.get_pipeline_type` and realised as a ``PipelineStrategy``
  in :mod:`zagg.runner`.
- **Backend** (*where*): an :class:`Executor` runs the per-unit work. This
  module provides :class:`LocalExecutor` (in-process); AWS Lambda fan-out is
  provided by :func:`zagg.dispatch.dispatch_lambda`. Additional backends
  (vaex, ray, dask, slurm) are planned in issue #20 and slot in behind the
  same :class:`Executor` interface.

A strategy produces work units and a per-unit ``worker`` callable; the executor
only decides how those calls are parallelised, so a new backend is added once
and every pipeline type inherits it.
"""

import logging
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class Executor(Protocol):
    """Runs a ``worker`` over a list of work units and returns the results."""

    def run(self, items: Iterable[Any], worker: Callable[[Any], Any]) -> list:
        ...


class LocalExecutor:
    """Run work units in-process.

    Suitable for IO-bound workers (the common case — most aggregation is IO
    bound) and for offline testing/validation without a cloud backend. With
    ``max_workers == 1`` runs serially (deterministic; handy for debugging).

    Parameters
    ----------
    max_workers : int
        Maximum concurrent worker calls. Clamped to ``>= 1``.
    """

    def __init__(self, max_workers: int = 4):
        self.max_workers = max(1, int(max_workers))

    def run(self, items: Iterable[Any], worker: Callable[[Any], Any]) -> list:
        items = list(items)
        if not items:
            return []
        if self.max_workers == 1:
            return [worker(item) for item in items]
        results = []
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(items))) as ex:
            futures = [ex.submit(worker, item) for item in items]
            for fut in as_completed(futures):
                results.append(fut.result())
        return results
