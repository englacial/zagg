### Latest Lambda benchmark — `2883c82`

_2026-07-14T21:00:27Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 79.4 | 84.7 | 0.0 | $0.00424 | $0.01045 | 11% | 674 | 16% |
| tdigest_healpix_o10_sidecar | 939,217 | 70.1 | 76.4 | 0.0 | $0.00374 | $0.00922 | 10% | 425 | 10% |
| tdigest_healpix_o9_inline | 3,112,138 | 108.0 | 114.1 | 0.0 | $0.00576 | $0.00355 | 15% | 708 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 96.2 | 102.7 | 0.0 | $0.00513 | $0.00316 | 13% | 610 | 15% |

Machine-readable companion: `metrics.json` (same directory).