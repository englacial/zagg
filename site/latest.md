### Latest Lambda benchmark — `174c228`

_2026-07-10T19:14:56Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 75.1 | 77.9 | 0.0 | $0.00401 | $0.00988 | 10% | 656 | 16% |
| tdigest_healpix_o10_sidecar | 939,217 | 56.5 | 61.3 | 0.0 | $0.00301 | $0.00744 | 8% | 467 | 11% |
| tdigest_healpix_o9_inline | 3,112,138 | 109.0 | 120.6 | 0.0 | $0.00581 | $0.00359 | 15% | 691 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 79.2 | 87.3 | 0.0 | $0.00422 | $0.00261 | 11% | 645 | 16% |

Machine-readable companion: `metrics.json` (same directory).