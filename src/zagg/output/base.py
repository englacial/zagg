"""The ``Writer`` seam and the output-format registry (issue #12, Phase 6).

A :class:`Writer` is the polymorphic output layer issue #12 (challenge #5) calls
for: the spatial path persists a gridded Zarr store, the temporal/event path a
tabular file, and the runner selects one by the config's ``output.format`` rather
than branching inline. The registry mirrors the ``zagg.registry`` pattern (a
name -> factory dict with a ``register``/``get`` pair) but stays local to this
package so the built-in format -> writer mapping is owned here.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from zagg.config import PipelineConfig


@runtime_checkable
class Writer(Protocol):
    """Persist a pipeline run's output.

    Implementations own one output target (a Zarr store, a tabular file, ...).
    ``write`` is the single required method; its ``payload`` shape is per-writer
    (the spatial path hands :class:`ZarrGridWriter` per-shard carriers as it goes;
    the temporal path hands :class:`TabularWriter` the collected result rows once),
    so the protocol stays deliberately loose rather than forcing one schema across
    the two structurally different output models.
    """

    def write(self, payload, **kwargs):  # pragma: no cover - structural protocol
        ...


#: Built-in ``output.format`` -> writer-factory registry. ``"zarr"`` is the
#: spatial default; ``"tabular"``/``"parquet"``/``"csv"``/``"hdf5"`` all resolve
#: to the tabular writer (the concrete serialisation is chosen from the path /
#: an explicit ``format=`` at write time).
_WRITERS: dict[str, type] = {}


def register_writer(name: str, factory: type | None = None, *, replace: bool = False):
    """Register a writer factory under an ``output.format`` name.

    Usable as a call (``register_writer("zarr", ZarrGridWriter)``) or a decorator
    (``@register_writer("zarr")``). Mirrors ``zagg.registry`` semantics: an empty
    name is rejected and a duplicate raises unless ``replace=True``.
    """

    def _register(fac: type) -> type:
        if not name:
            raise ValueError("writer name must be non-empty")
        if name in _WRITERS and not replace:
            raise ValueError(f"writer {name!r} already registered (pass replace=True to override)")
        _WRITERS[name] = fac
        return fac

    if factory is not None:
        return _register(factory)
    return _register


def get_writer(output_format: str) -> Writer:
    """Return a writer instance for an ``output.format`` name.

    Raises
    ------
    ValueError
        If no writer is registered for ``output_format``; the message lists the
        known formats so a typo is easy to spot.
    """
    try:
        factory = _WRITERS[output_format]
    except KeyError:
        known = ", ".join(sorted(_WRITERS)) or "(none registered)"
        raise ValueError(
            f"no writer for output.format={output_format!r}; known formats: {known}"
        ) from None
    return factory()


def output_format(config: PipelineConfig) -> str:
    """Resolve a config's output format, defaulting to ``"zarr"`` (spatial).

    Every existing spatial config omits ``output.format``, so the default keeps
    the gridded Zarr path selected unchanged.
    """
    return config.output.get("format", "zarr")
