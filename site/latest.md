### Latest Lambda benchmark — `97af009`

_2026-07-15T00:45:12Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 141.7 | 148.2 | 0.0 | $0.00756 | $0.00466 | 20% | 715 | 17% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 116.4 | 125.1 | 0.0 | $0.00621 | $0.00383 | 16% | 706 | 17% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 122.7 | 127.6 | 0.0 | $0.00654 | $0.00404 | 17% | 636 | 16% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 115.9 | 120.9 | 0.0 | $0.00618 | $0.00381 | 16% | 629 | 15% |

Machine-readable companion: `metrics.json` (same directory).