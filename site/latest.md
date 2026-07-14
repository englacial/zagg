### Latest Lambda benchmark — `15d170f`

_2026-07-14T07:00:07Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 71.4 | 73.3 | 0.0 | $0.00381 | $0.00939 | 10% | 655 | 16% |
| tdigest_healpix_o10_sidecar | 939,217 | 66.1 | 73.6 | 0.0 | $0.00352 | $0.00869 | 9% | 425 | 10% |
| tdigest_healpix_o9_inline | 3,112,138 | 116.1 | 121.9 | 0.0 | $0.00619 | $0.00382 | 16% | 687 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 94.6 | 101.4 | 0.0 | $0.00505 | $0.00311 | 13% | 610 | 15% |

Machine-readable companion: `metrics.json` (same directory).