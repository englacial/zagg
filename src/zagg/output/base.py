"""Output-format resolution for zagg's pipelines (issue #12, Phase 6).

The two pipeline cores produce structurally different output (challenge #5 in
issue #12): the spatial path writes a gridded, morton-chunked **Zarr** store; the
temporal/event path produces **tabular** output (one row per event x scalar
columns). :func:`output_format` resolves which one a config selects; the
temporal write itself goes through :func:`zagg.output.write_tabular`, and the
spatial path calls the ``zagg.processing`` Zarr writers directly.
"""

from __future__ import annotations

from zagg.config import PipelineConfig


def output_format(config: PipelineConfig) -> str:
    """Resolve a config's output format, defaulting to ``"zarr"`` (spatial).

    Every existing spatial config omits ``output.format``, so the default keeps
    the gridded Zarr path selected unchanged.
    """
    return config.output.get("format", "zarr")
