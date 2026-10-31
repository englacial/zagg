"""``data_source`` accessors: filters, levels, bands, collections (issue #677 phase 0)."""

from zagg.config.core import _SET_OPS, PipelineConfig


def get_raster_bands(config: PipelineConfig) -> dict:
    """Normalized ``data_source.bands``: field -> {asset, dtype, fill_value, attrs}.

    ``attrs`` carries CF ``scale_factor``/``add_offset`` when the band
    declares ``scale``/``offset`` — recorded on the array for readers, never
    applied to the stored DNs (issue #218: exact source values).
    """
    out = {}
    for name, meta in (config.data_source.get("bands") or {}).items():
        attrs = {}
        if "scale" in meta:
            attrs["scale_factor"] = meta["scale"]
        if "offset" in meta:
            attrs["add_offset"] = meta["offset"]
        out[name] = {
            "asset": meta["asset"],
            "dtype": meta["dtype"],
            "fill_value": meta.get("fill_value", 0),
            "attrs": attrs,
        }
    return out


def collection_options(config: PipelineConfig) -> dict[str, dict]:
    """Normalize ``data_source.collections`` to ``{name: options}``.

    Both config forms collapse to one shape: the list form (names only) yields
    empty option dicts; the mapping form yields each collection's options
    verbatim (``null`` becomes ``{}``). Consumed by the temporal reader /
    :func:`zagg.temporal.prepare_collection`.
    """
    colls = (config.data_source or {}).get("collections") or []
    if isinstance(colls, dict):
        return {name: dict(opts or {}) for name, opts in colls.items()}
    return {name: {} for name in colls}


def get_filters(config: PipelineConfig) -> list[dict]:
    """Return the ordered list of normalized data-source filters (issue #43).

    Two filter languages coexist:

    - **Structured predicates** ``{level?, dataset, column?, op, value|values,
      keep?}`` are machine-inspectable and are the only kind eligible for read
      pushdown (Phase C). ``op`` is one of :data:`FILTER_OPS`; ``in``/``not_in``
      take ``values`` (a list), the rest take a scalar ``value``. ``column`` is an
      integer selector into an N-D flag array (e.g. ATL03 ``signal_conf_ph``).
      ``keep`` (default ``True``) keeps matching rows; ``keep: false`` drops them.
    - **Expression** filters ``{expression: "<py expr>"}`` are a base-level-only,
      aggregation-time escape hatch that forfeits pushdown (opaque to the planner).

    The flat ``quality_filter: {dataset, value}`` is sugar synthesizing one
    base-level ``op: eq`` structured filter, so the ATL06 path is unchanged. An
    explicit ``filters:`` list, when present, is used as-is (the flat
    ``quality_filter`` is then ignored).

    Each returned filter carries a normalized ``level`` (``None`` means the base
    level) and, for structured predicates, an explicit ``keep`` bool.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[dict]
    """
    return filters_from_data_source(config.data_source)


def filters_from_data_source(data_source: dict) -> list[dict]:
    """Normalize the filter list from a raw ``data_source`` dict.

    Shared by :func:`get_filters` and the read path (which only holds the
    ``data_source`` mapping). See :func:`get_filters` for the schema.
    """
    explicit = data_source.get("filters")
    if explicit is not None:
        return [_normalize_filter(f) for f in explicit]
    qf = data_source.get("quality_filter")
    if qf is not None:
        return [
            {
                "level": None,
                "dataset": qf["dataset"],
                "column": None,
                "op": "eq",
                "value": qf["value"],
                "keep": True,
            }
        ]
    return []


def _normalize_filter(f: dict) -> dict:
    """Normalize one raw filter dict into canonical form (see :func:`get_filters`)."""
    if "expression" in f:
        return {"level": f.get("level"), "expression": f["expression"]}
    op = f["op"]
    out = {
        "level": f.get("level"),
        "asset": f.get("asset"),
        "dataset": f["dataset"],
        "column": f.get("column"),
        "op": op,
        "keep": bool(f.get("keep", True)),
    }
    if op in _SET_OPS:
        out["values"] = list(f["values"])
    else:
        out["value"] = f["value"]
    return out


def _segment_variable_names(data_source: dict) -> set[str]:
    """Names of readable segment-level (non-base) variables (issue #30).

    A non-base level may declare ``variables`` as a ``{name: path-template}``
    mapping; each name becomes a per-photon column once broadcast at read time
    (:func:`zagg.processing._read_segment_broadcasts`). The documentation-only
    ``list[str]`` form contributes nothing. Empty when no level declares the
    mapping form, so plain configs are unaffected.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or base_level is None:
        return set()
    names: set[str] = set()
    for name, lvl in levels.items():
        if name == base_level or not isinstance(lvl, dict):
            continue
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict):
            names |= set(lvl_vars)
    return names


def get_levels(config: "PipelineConfig") -> dict | None:
    """Return the ``levels`` mapping from the data source, or ``None`` if flat.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict or None
    """
    return config.data_source.get("levels")


def get_base_level(config: "PipelineConfig") -> str | None:
    """Return the ``base_level`` key from the data source, or ``None`` if flat.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.data_source.get("base_level")
