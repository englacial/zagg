### Latest Lambda benchmark — `cd71a6c`

_2026-07-15T02:05:02Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 152.8 | 158.4 | 0.0 | $0.00815 | $0.00503 | 21% | 675 | 16% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 114.8 | 121.3 | 0.0 | $0.00612 | $0.00378 | 16% | 718 | 18% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 130.9 | 140.0 | 0.0 | $0.00698 | $0.00431 | 18% | 622 | 15% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 109.4 | 114.5 | 0.0 | $0.00584 | $0.00360 | 15% | 611 | 15% |

Machine-readable companion: `metrics.json` (same directory).