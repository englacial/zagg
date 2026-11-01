"""Config object, loading funnel, and the strict-JSON dispatch gate.

Phase 0 of issue #677: the ``zagg.config`` package split. This module holds
what every other config module depends on -- the ``PipelineConfig``
dataclass, its typed-dict annotations, the load funnels, and the constants the
validators and accessors share -- so no sibling module imports upward at
import time (the two loaders, ``load_config`` and ``default_config``, import
``validate_config`` from ``zagg.config.validate`` at call time).
"""

import json
from dataclasses import asdict, dataclass, field
from importlib import resources
from typing import NotRequired, TypedDict

import numpy as np
import yaml

import zagg.configs


class LinkDict(TypedDict):
    """Per-level link to the next coarser level (issue #43, Phase B).

    A *link* describes a contiguous-range parent->child tiling: each parent segment
    ``p`` covers base-rate indices ``[index_beg[p] - index_base, ...`` for
    ``count[p]`` children.  ``index_base`` shifts the raw ``index_beg`` values so
    that Python 0-based indexing into the base array is straightforward.

    ``reference_index`` is a reserved slot for a future explicit-index-array variant
    (non-contiguous children per parent); leave it ``None`` for the contiguous case.
    """

    to: str  # key of the coarser level in ``levels``
    index_beg: str  # HDF5 path for the per-parent start index array
    count: str  # HDF5 path for the per-parent child count array
    index_base: NotRequired[int]  # subtracted from index_beg values (default 0)
    reference_index: NotRequired[str | None]  # reserved; must be None


class LevelDict(TypedDict):
    """One hierarchical level in a multi-rate HDF5 source (issue #43, Phase B).

    A source may have several rates (e.g. ATL03 ``photons`` and ``segments``).
    Each level declares its own ``path``, ``coordinates``, and ``variables``,
    plus an optional ``link`` to a coarser parent level.  The flat single-level
    form (no ``levels``/``base_level`` keys in ``data_source``) stays first-class.
    """

    path: str  # HDF5 group path template (may contain ``{group}``)
    coordinates: list[str]  # coordinate dataset names within ``path``
    # ``variables`` has two forms: a documentation-only ``list[str]`` of names, or
    # (non-base levels, issue #30) a ``{name: path-template}`` mapping declaring a
    # *readable* segment-level variable. The mapping form is read at coarse rate and
    # broadcast to the base (photon) rows via ``link`` so e.g. ``dem_h`` (one value
    # per ~100 photons) becomes a per-photon column the aggregation can reduce.
    variables: list[str] | dict[str, str]
    link: NotRequired[LinkDict | None]


class DataSourceDict(TypedDict):
    """Type hints for the ``data_source`` section of a pipeline config."""

    reader: str
    groups: list[str]
    coordinates: dict[str, str]
    variables: dict[str, str]
    quality_filter: NotRequired[dict]
    filters: NotRequired[list[dict]]
    # Hierarchical multi-level form (issue #43, Phase B). When present, the flat
    # ``coordinates``/``variables`` keys are still accepted for the base level but
    # ``levels`` + ``base_level`` take precedence for the read path.
    levels: NotRequired[dict[str, LevelDict]]
    base_level: NotRequired[str]
    # Virtual chunk-index backend block (issue #160). Absent → the default
    # ``hierarchical`` path, byte-identical. ``backend`` names a registered
    # backend (builtin or ``zagg.index_backends`` entry point); the remaining
    # keys are backend-specific and validated against the backend's declared
    # ``config_keys`` — irrelevant keys are config errors, not ignored.
    index: NotRequired[dict]
    # Credential-provider registry name for source-data S3 reads (issue #213
    # Phase 4/6): built-ins ``nsidc``/``gesdisc``; plugins may register others.
    # Absent → the spatial default (NSIDC); temporal events may also carry
    # per-event ``s3_credentials``, which win.
    credentials_provider: NotRequired[str]


# Structured-predicate comparison operators (issue #43). ``in``/``not_in`` take a
# ``values`` list; the rest take a scalar ``value``. These are the only
# pushdown-eligible filter language; an ``expression`` filter is a base-level-only,
# aggregation-time escape hatch that forfeits pushdown.
_SCALAR_OPS = frozenset({"eq", "ne", "ge", "le", "lt", "gt"})
_SET_OPS = frozenset({"in", "not_in"})
FILTER_OPS = _SCALAR_OPS | _SET_OPS


_PIPELINE_TYPES = frozenset({"spatial", "temporal", "event"})

# Memory sizes (MB) of the pre-provisioned Lambda worker-size variants
# (issue #235). Must match template.yaml's WorkerMemorySizes parameter — the
# runner resolves ``worker:`` to a ``<base>-<memory>[-disk]`` function name,
# so an unlisted size would dispatch to a function that does not exist.
WORKER_MEMORIES = frozenset({2048, 4096, 8192})


@dataclass
class PipelineConfig:
    """Full pipeline configuration.

    Parameters
    ----------
    data_source : DataSourceDict
        Reader, groups, coordinates, variables, quality filter.
    aggregation : dict
        Coordinate and variable aggregation definitions.
    output : dict
        Grid spec, store path, and indexing details.
    catalog : str or None
        Optional path to granule catalog JSON.
    bounds : dict or None
        Optional temporal/spatial bounds for filtering.
    pipeline : dict
        Pipeline kind selector (issue #12). ``{"type": "spatial"}`` (default)
        runs the point-cloud->grid aggregation path; ``"temporal"`` /
        ``"event"`` route to the event-streaming engines added in later
        phases. Absent ``pipeline`` key in YAML defaults to ``spatial`` for
        backward compatibility with every existing config.
    worker : dict or None
        Optional Lambda worker-size selector (issue #235):
        ``{"memory": 2048|4096|8192, "extra_disk": bool}``. The runner
        resolves it to a pre-provisioned function-name suffix
        (``-<memory>``, plus ``-disk`` when ``extra_disk`` is true) on the
        lambda backend; an explicit ``function_name`` kwarg wins over it.
        Absent block -> the unsuffixed default function, byte-identical
        prior behavior. Ignored by the local backend.
    """

    data_source: DataSourceDict = field(default_factory=dict)
    aggregation: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    catalog: str | None = None
    bounds: dict | None = None
    pipeline: dict = field(default_factory=lambda: {"type": "spatial"})
    worker: dict | None = None

    @property
    def model(self):
        """The typed view of this config (issue #677): the pydantic grammar tree.

        Validated on every access from the raw blocks (cheap — milliseconds for
        a packaged template); raises ``ValueError`` with path-addressed messages
        on a config the grammar refuses. Additive: the dict attributes stay the
        stored form the accessors and the semantic hash read.
        """
        from zagg.config.models import validate_model

        return validate_model(self)


def get_pipeline_type(config: PipelineConfig) -> str:
    """Return the pipeline kind, defaulting to ``"spatial"``.

    Centralised so dispatch / strategy selection has a single source of truth.
    """
    if not isinstance(config.pipeline, dict):
        raise ValueError("pipeline must be a mapping with a 'type' key")
    t = config.pipeline.get("type", "spatial")
    if t not in _PIPELINE_TYPES:
        raise ValueError(f"pipeline.type must be one of {sorted(_PIPELINE_TYPES)} (got {t!r})")
    return t


def load_config(path: str) -> PipelineConfig:
    """Load a YAML config file and return a validated PipelineConfig.

    Parameters
    ----------
    path : str
        Path to YAML file.

    Returns
    -------
    PipelineConfig
    """
    from zagg.config.validate import validate_config

    with open(path) as f:
        d = yaml.safe_load(f)
    cfg = load_config_from_dict(d)
    validate_config(cfg)
    return cfg


def _is_float_nan(value) -> bool:
    """Whether ``value`` is a float NaN (YAML ``.nan`` / ``float("nan")``).

    ``np.floating`` is checked explicitly (fold review): ``np.float64`` is a
    ``float`` subclass and would pass either way, but ``np.float32`` is not, so
    a Python-built config carrying ``np.float32("nan")`` slipped through.
    """
    return isinstance(value, (float, np.floating)) and bool(np.isnan(value))


def _has_nan_fill(node) -> bool:
    """Whether any ``fill_value`` anywhere under ``node`` is a float NaN."""
    if isinstance(node, dict):
        return any(
            _is_float_nan(v) if k == "fill_value" else _has_nan_fill(v) for k, v in node.items()
        )
    # Tuples traverse too (fold review): unreachable from YAML/JSON, but a
    # Python-built config may hold ``({"fill_value": nan},)``.
    if isinstance(node, (list, tuple)):
        return any(_has_nan_fill(v) for v in node)
    return False


def _normalize_nan_fills(node):
    """Rewrite float-NaN ``fill_value`` declarations to the string ``"NaN"``.

    YAML's ``.nan`` parses to a float NaN, which ``json.dumps`` emits as the
    non-standard token ``NaN`` -- Lambda's strict JSON parser rejects the whole
    dispatch payload (``InvalidRequestContentException``, issue #448). The
    string is the grammar's native form everywhere else: it is the healpix /
    rect template default (``meta.get("fill_value", "NaN")``), what
    :func:`_is_nan_fill` and the aggregate/spill/streaming fill checks compare
    against, and zarr v3's JSON fill encoding. Canonicalizing at load keeps
    ``.nan`` legal in authored YAML while guaranteeing one wire form -- and one
    form for the semantic hash.

    Returns the node unchanged (same object) when there is nothing to rewrite,
    so the common case allocates nothing and callers keep their dict identity.
    """
    if not _has_nan_fill(node):
        return node
    if isinstance(node, dict):
        return {
            k: "NaN" if (k == "fill_value" and _is_float_nan(v)) else _normalize_nan_fills(v)
            for k, v in node.items()
        }
    if isinstance(node, tuple):
        return tuple(_normalize_nan_fills(v) for v in node)
    return [_normalize_nan_fills(v) for v in node]


def load_config_from_dict(d: dict) -> PipelineConfig:
    """Build a PipelineConfig from a plain dict (e.g. Lambda JSON payload).

    Float-NaN **fill_value** declarations are canonicalized to the string
    ``"NaN"`` here (issue #448) -- this is the single funnel every config
    passes through (``load_config``, ``default_config``, the client, and the
    Lambda worker's ``event["config"]``). The scope is deliberate: rewriting
    every float NaN in the tree would silently mangle authored values. A NaN
    somewhere else (a filter ``value``, a variable ``attrs`` entry) is a config
    ERROR, not something to canonicalize, and :func:`validate_config` refuses
    it -- see :func:`_validate_json_floats`.

    Parameters
    ----------
    d : dict
        Dictionary with keys ``data_source``, ``aggregation``, ``output``.

    Returns
    -------
    PipelineConfig
    """
    d = _normalize_nan_fills(d)
    return PipelineConfig(
        data_source=d.get("data_source", {}),
        aggregation=d.get("aggregation", {}),
        output=d.get("output", {}),
        catalog=d.get("catalog"),
        bounds=d.get("bounds"),
        pipeline=d.get("pipeline", {"type": "spatial"}),
        worker=d.get("worker"),
    )


def default_config(name: str = "atl06", *, validate: bool = True) -> PipelineConfig:
    """Load a built-in YAML config shipped with the package.

    Parameters
    ----------
    name : str
        Config name (without ``.yaml`` extension). Default ``"atl06"``.
    validate : bool
        Run ``validate_config`` (default). Internal callers loading a packaged
        config as a structural fallback — e.g. ``HealpixGrid``'s implicit
        ``config`` default, where the aggregation block is irrelevant — skip
        it so template-authoring warnings don't fire on configs the user
        never wrote (packaged configs are validated by the test suite).

    Returns
    -------
    PipelineConfig

    Raises
    ------
    FileNotFoundError
        If the named config does not exist.
    """
    from zagg.config.validate import validate_config

    ref = resources.files(zagg.configs).joinpath(f"{name}.yaml")
    if not ref.is_file():
        raise FileNotFoundError(f"No built-in config named '{name}'")
    text = ref.read_text(encoding="utf-8")
    d = yaml.safe_load(text)
    cfg = load_config_from_dict(d)
    if validate:
        validate_config(cfg)
    return cfg


def _nonfinite_path(node, path: str = "") -> str | None:
    """Dotted path to the first non-finite float under ``node`` (else None).

    Purely a message helper for :func:`_validate_json_floats`: ``json.dumps``
    reports "Out of range float values are not JSON compliant" with no path, so
    the walk locates the offender for an actionable error.
    """
    if isinstance(node, dict):
        for k, v in node.items():
            hit = _nonfinite_path(v, f"{path}.{k}" if path else str(k))
            if hit is not None:
                return hit
        return None
    if isinstance(node, (list, tuple)):
        for i, v in enumerate(node):
            hit = _nonfinite_path(v, f"{path}[{i}]")
            if hit is not None:
                return hit
        return None
    if isinstance(node, (float, np.floating)) and not np.isfinite(node):
        return path or "<config>"
    return None


def _validate_json_floats(config: PipelineConfig) -> None:
    """Refuse a config carrying a float strict JSON cannot encode (issue #448).

    The dispatch event's ``config`` block is ``dataclasses.asdict(config)``
    serialized by ``json.dumps``; Lambda's parser enforces exactly
    ``allow_nan=False``, so a NaN/Infinity anywhere in the tree kills the whole
    invoke with an opaque ``InvalidRequestContentException`` one dispatch
    later. ``fill_value`` NaNs are canonicalized at load
    (:func:`_normalize_nan_fills`) because there the float form is a legal
    *spelling*; everywhere else -- a filter ``value``, a variable ``attrs``
    entry -- it is a config error, and this is the backstop that says so at
    load time, with the offending path named (fold review).

    The walk runs first because it also catches ``np.float32("nan")``, which
    ``json.dumps`` reports as an unserializable *type* rather than an
    out-of-range float. A non-JSON type is a different (and rarer) fault than
    the value question this check owns, so it is left to the serializer.
    """
    data = asdict(config)
    path = _nonfinite_path(data)
    if path is not None:
        raise ValueError(
            f"config value at {path} is a non-finite float (NaN/Infinity), which strict "
            f"JSON refuses -- the Lambda dispatch payload would be rejected "
            f'(InvalidRequestContentException, issue #448). Use the string "NaN" for a '
            f"fill_value declaration; elsewhere, drop the value or spell it as a string."
        )
    try:
        json.dumps(data, allow_nan=False)
    except ValueError as exc:  # pragma: no cover - the walk catches every known case
        raise ValueError(
            f"config cannot be serialized as strict JSON, so it cannot be dispatched "
            f"(issue #448): {exc}"
        ) from exc
    except TypeError:
        # A non-JSON *type* (e.g. a datetime, a set) -- out of scope here; the
        # serializer owns it, and refusing it at validation would newly reject
        # configs that never dispatch.
        pass
