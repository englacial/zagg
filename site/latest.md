### Latest Lambda benchmark — `3cf81e6`

_2026-07-15T01:53:58Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 142.3 | 148.6 | 0.0 | $0.00759 | $0.00468 | 20% | 671 | 16% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 128.0 | 136.0 | 0.0 | $0.00682 | $0.00421 | 18% | 681 | 17% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 115.7 | 122.7 | 0.0 | $0.00617 | $0.00380 | 16% | 636 | 16% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 103.0 | 106.9 | 0.0 | $0.00549 | $0.00339 | 14% | 637 | 16% |

Machine-readable companion: `metrics.json` (same directory).