### Latest Lambda benchmark — `4936d23`

_2026-07-07T01:53:24Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 211.1 | $0.00563 | $0.01389 | 29% | 874 | 43% |
| tdigest_healpix_o10_inner | 939,217 | 259.5 | $0.00692 | $0.01707 | 36% | 901 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 186.8 | $0.00498 | $0.01229 | 26% | 898 | 44% |
| tdigest_healpix_o11_cached | 200,676 | 154.9 | $0.00413 | $0.04077 | 22% | 821 | 40% |
| tdigest_healpix_o11_inner | 200,676 | 154.2 | $0.00411 | $0.04057 | 21% | 805 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 150.7 | $0.00402 | $0.03966 | 21% | 789 | 39% |
| tdigest_healpix_o9_cached | 3,112,138 | 394.6 | $0.01052 | $0.00649 | 55% | 1136 | 55% |
| tdigest_healpix_o9_inner | 3,112,138 | 440.8 | $0.01176 | $0.00725 | 61% | 1129 | 55% |
| tdigest_healpix_o9_sharded | 3,112,138 | 324.9 | $0.00867 | $0.00534 | 45% | 1224 | 60% |

Machine-readable companion: `metrics.json` (same directory).