"""Statistical algorithms for zagg aggregation (issue #48)."""

from .tdigest import build_tdigest, merge_tdigests

__all__ = ["build_tdigest", "merge_tdigests"]
