### Latest Lambda benchmark — `305d03b`

_2026-07-08T19:49:36Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 71.2 | 78.6 | 0.0 | $0.00380 | $0.00936 | 10% | 652 | 16% |
| tdigest_healpix_o10_sidecar | 939,217 | 53.9 | 61.8 | 0.0 | $0.00288 | $0.00710 | 7% | 448 | 11% |
| tdigest_healpix_o9_inline | 3,112,138 | 106.1 | 117.3 | 0.0 | $0.00566 | $0.00349 | 15% | 680 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 82.7 | 95.0 | 0.0 | $0.00441 | $0.00272 | 11% | 660 | 16% |

Machine-readable companion: `metrics.json` (same directory).