### Latest Lambda benchmark — `53c16d6`

_2026-07-14T21:54:24Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 108.2 | 116.3 | 0.0 | $0.00577 | $0.00356 | 12% | 549 | 13% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 97.0 | 101.5 | 0.0 | $0.00517 | $0.00319 | 11% | 562 | 14% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 118.7 | 126.7 | 0.0 | $0.00633 | $0.00390 | 13% | 451 | 11% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 94.7 | 101.4 | 0.0 | $0.00505 | $0.00311 | 11% | 419 | 10% |

Machine-readable companion: `metrics.json` (same directory).