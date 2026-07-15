### Latest Lambda benchmark — `10d14f1`

_2026-07-15T00:01:17Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 129.3 | 133.4 | 0.0 | $0.00690 | $0.00425 | 18% | 655 | 16% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 120.2 | 124.0 | 0.0 | $0.00641 | $0.00396 | 17% | 672 | 16% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 143.0 | 148.5 | 0.0 | $0.00763 | $0.00470 | 20% | 625 | 15% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 123.9 | 132.9 | 0.0 | $0.00661 | $0.00408 | 17% | 629 | 15% |

Machine-readable companion: `metrics.json` (same directory).