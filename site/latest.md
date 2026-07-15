### Latest Lambda benchmark — `0b2a18e`

_2026-07-15T20:39:24Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 127.2 | 132.2 | 0.0 | $0.00678 | $0.00418 | 18% | 695 | 17% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 131.0 | 139.9 | 0.0 | $0.00699 | $0.00431 | 18% | 715 | 17% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 130.5 | 136.1 | 0.0 | $0.00696 | $0.00429 | 18% | 633 | 15% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 115.0 | 123.4 | 0.0 | $0.00614 | $0.00378 | 16% | 630 | 15% |

Machine-readable companion: `metrics.json` (same directory).