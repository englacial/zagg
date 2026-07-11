### Latest Lambda benchmark — `b07414f`

_2026-07-11T00:06:05Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 79.4 | 83.0 | 0.0 | $0.00424 | $0.01045 | 11% | 677 | 17% |
| tdigest_healpix_o10_sidecar | 939,217 | 75.8 | 79.7 | 0.0 | $0.00404 | $0.00997 | 11% | 433 | 11% |
| tdigest_healpix_o9_inline | 3,112,138 | 123.1 | 136.8 | 0.0 | $0.00657 | $0.00405 | 17% | 713 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 102.8 | 114.8 | 0.0 | $0.00548 | $0.00338 | 14% | 633 | 15% |

Machine-readable companion: `metrics.json` (same directory).