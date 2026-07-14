### Latest Lambda benchmark — `9789c86`

_2026-07-14T04:17:57Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 70.8 | 74.6 | 0.0 | $0.00378 | $0.00931 | 10% | 694 | 17% |
| tdigest_healpix_o10_sidecar | 939,217 | 68.1 | 73.1 | 0.0 | $0.00363 | $0.00896 | 9% | 447 | 11% |
| tdigest_healpix_o9_inline | 3,112,138 | 104.8 | 111.2 | 0.0 | $0.00559 | $0.00345 | 15% | 717 | 17% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 108.7 | 117.0 | 0.0 | $0.00580 | $0.00357 | 15% | 619 | 15% |

Machine-readable companion: `metrics.json` (same directory).