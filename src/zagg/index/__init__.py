"""Virtual chunk-index backends for the read path (issue #160).

A *virtual index* answers exactly two questions per (granule, shard, config):
which chunks / element ranges of the configured datasets intersect this shard
(**selection**), and what column arrays result from fetching + decoding them
(**addressing + decode**). It deliberately does **not** own filtering
(data-dependent predicates like ``signal_conf_ph`` stay downstream, unchanged)
or aggregation — the seam is :meth:`VirtualIndex.read_group`, whose shape
matches ``zagg.processing._read_group`` per the issue #160 protocol decision.

Backends resolve by **name** from the ``data_source.index`` config block
(absent → ``hierarchical``, today's path, zero-change upgrade)::

    data_source:
      index:
        backend: inline       # or hierarchical / sidecar / ...
        write_back: false     # backend-specific keys; irrelevant keys are errors

Built-ins (``hierarchical``, ``inline``) live in a static dict; external
packages contribute via the ``zagg.index_backends`` entry-point group — the
same pattern as h5coro registering itself as an xarray engine, so zagg core
never imports an external backend's dependencies (e.g. hidefix)::

    [project.entry-points."zagg.index_backends"]
    sidecar = "h5coro_hidefix.zagg_backend:SidecarIndex"

Each entry point resolves directly to a :class:`VirtualIndex` subclass. A
builtin name cannot be shadowed; a broken entry point is logged and skipped.
The entry-point scan is memoized per interpreter (like ``zagg.registry``'s
discovery); a failed ``entry_points()`` lookup is not cached, so the next
call retries.
"""

from __future__ import annotations

import logging
from importlib import metadata
from typing import Any, ClassVar

from zagg.registry import UnknownCapability

logger = logging.getLogger(__name__)

#: Entry-point group external backend packages declare (issue #160 Q4:
#: ``zagg.index_backends``, mirroring the ``xarray.backends`` precedent).
INDEX_BACKENDS_GROUP = "zagg.index_backends"


class VirtualIndex:
    """Protocol base for chunk-index backends (issue #160).

    Subclasses implement :meth:`read_group` (the per-(granule, group) read —
    same signature as ``zagg.processing._read_group``, per the issue's
    granularity decision) and may override :meth:`finish_granule` (a
    per-granule hook the worker calls after the last group of a granule is
    read — the write-back seam). Class-level ``config_keys`` /
    ``required_config_keys`` declare the backend's ``data_source.index``
    parameters so validation can reject irrelevant keys as errors.
    """

    #: Registry name (matches the ``backend:`` config value).
    name: ClassVar[str] = ""
    #: Config keys accepted under ``data_source.index`` besides ``backend``.
    config_keys: ClassVar[frozenset[str]] = frozenset()
    #: Subset of ``config_keys`` that must be present.
    required_config_keys: ClassVar[frozenset[str]] = frozenset()

    @classmethod
    def validate_index_config(cls, index_cfg: dict, data_source: dict | None = None) -> None:
        """Backend-specific validation hook (values, cross-field constraints).

        Key presence/absence is already enforced by
        :func:`validate_index_config` before this is called. Default: no-op.
        """

    @classmethod
    def from_index_config(cls, index_cfg: dict) -> "VirtualIndex":
        """Construct the backend from a validated ``data_source.index`` block."""
        return cls()

    def read_group(
        self, h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
    ):
        """Read + spatially filter one HDF5 group for one shard.

        Same contract as ``zagg.processing._read_group``: returns a
        ``pandas.DataFrame`` / ``arro3.core.Table`` carrier, or ``None`` when
        the group has no observations in this shard.
        """
        raise NotImplementedError

    def finish_granule(self, h5obj, granule_url: str) -> None:
        """Per-granule hook after all groups of ``granule_url`` are read.

        Called by the worker before the granule's h5coro cache is released;
        backends use it for per-granule side effects (e.g. ``inline``
        write-back). Failures are logged by the caller and never fail the
        read. Default: no-op.
        """


def _builtin_backends() -> dict[str, type[VirtualIndex]]:
    """The in-tree backends. Imported lazily so ``zagg.config`` validation can
    import this module without pulling the processing stack."""
    from zagg.index.hierarchical import HierarchicalIndex
    from zagg.index.inline import InlineIndex

    return {HierarchicalIndex.name: HierarchicalIndex, InlineIndex.name: InlineIndex}


# Memoized entry-point discoveries (``None`` = not yet scanned, or the last
# scan failed and should be retried). Worst case under concurrent first use is
# a duplicate scan — idempotent, so no lock is needed (unlike zagg.registry,
# whose plugins run arbitrary ``register()`` callables).
_EP_BACKENDS: dict[str, type[VirtualIndex]] | None = None


def available_index_backends() -> dict[str, type[VirtualIndex]]:
    """Builtins merged with ``zagg.index_backends`` entry-point discoveries.

    Builtins win a name collision (logged at ERROR, entry point skipped); a
    failing ``ep.load()`` is logged and skipped so one broken plugin cannot
    take down the read path. The scan runs once per interpreter (entry points
    cannot change mid-run); a failed lookup is retried on the next call.
    """
    global _EP_BACKENDS
    backends = _builtin_backends()
    if _EP_BACKENDS is None:
        try:
            eps = metadata.entry_points(group=INDEX_BACKENDS_GROUP)
        except Exception:
            logger.exception(
                "%s entry-point lookup failed; using builtins only", INDEX_BACKENDS_GROUP
            )
            return backends
        discovered: dict[str, type[VirtualIndex]] = {}
        for ep in eps:
            if ep.name in backends:
                logger.error(
                    "%s entry point %r collides with a registered backend; skipped",
                    INDEX_BACKENDS_GROUP,
                    ep.name,
                )
                continue
            try:
                discovered[ep.name] = ep.load()
            except Exception:
                logger.exception("Failed to load %s entry point %r", INDEX_BACKENDS_GROUP, ep.name)
        _EP_BACKENDS = discovered
    backends.update(_EP_BACKENDS)
    return backends


def get_index_backend(name: str) -> type[VirtualIndex]:
    """Resolve a backend class by name (builtin or entry point)."""
    backends = available_index_backends()
    try:
        return backends[name]
    except KeyError:
        raise UnknownCapability(name, "index_backend", sorted(backends)) from None


def validate_index_config(index_cfg: Any, data_source: dict | None = None) -> None:
    """Validate a ``data_source.index`` block (issue #160 config semantics).

    ``store`` / ``on_miss`` / ``write_back`` etc. are backend-specific
    parameters, not global keys: a key the named backend does not accept is a
    config **error**, not ignored. Backend-specific value checks are delegated
    to the backend class.
    """
    if not isinstance(index_cfg, dict):
        raise ValueError("data_source.index must be a mapping (e.g. index: {backend: inline})")
    backend = index_cfg.get("backend")
    if not isinstance(backend, str) or not backend:
        raise ValueError(
            "data_source.index.backend is required and must be a string "
            "(omit the whole index block for the default hierarchical path)"
        )
    try:
        cls = get_index_backend(backend)
    except UnknownCapability as e:
        # Config validation surfaces ValueError everywhere else in
        # zagg.config; keep that contract here (UnknownCapability stays the
        # runtime-resolution error, matching zagg.registry).
        raise ValueError(f"data_source.index: {e}") from e
    extra = set(index_cfg) - {"backend"} - set(cls.config_keys)
    if extra:
        raise ValueError(
            f"data_source.index: keys {sorted(extra)} are not accepted by backend "
            f"{backend!r} (accepted: {sorted(cls.config_keys)}); irrelevant keys "
            f"under a backend are config errors, not ignored"
        )
    missing = set(cls.required_config_keys) - set(index_cfg)
    if missing:
        raise ValueError(f"data_source.index: backend {backend!r} requires keys {sorted(missing)}")
    cls.validate_index_config(index_cfg, data_source)


def index_from_config(config) -> VirtualIndex:
    """Construct the configured backend from a ``PipelineConfig``.

    An absent ``data_source.index`` block is the zero-change upgrade path:
    today's hierarchical read, byte-identical.
    """
    index_cfg = (config.data_source or {}).get("index")
    if index_cfg is None:
        from zagg.index.hierarchical import HierarchicalIndex

        return HierarchicalIndex()
    # Re-validate here (cheap) so dict-built configs that skipped
    # ``validate_config`` (e.g. hand-rolled Lambda payloads) fail loudly at
    # backend resolution rather than deep in a group read.
    validate_index_config(index_cfg, config.data_source)
    cls = get_index_backend(index_cfg["backend"])
    return cls.from_index_config(index_cfg)


__all__ = [
    "INDEX_BACKENDS_GROUP",
    "VirtualIndex",
    "available_index_backends",
    "get_index_backend",
    "index_from_config",
    "validate_index_config",
]
