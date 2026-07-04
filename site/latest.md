### Latest Lambda benchmark — `7cfe71e`

_2026-07-04T09:57:10Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 260.8 | $0.00696 | $0.01716 | 36% | 895 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 226.5 | $0.00604 | $0.01490 | 31% | 898 | 44% |
| tdigest_healpix_o11_inner | 200,676 | 149.6 | $0.00399 | $0.03937 | 21% | 801 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 141.7 | $0.00378 | $0.03729 | 20% | 811 | 40% |
| tdigest_healpix_o9_inner | 3,112,138 | 433.8 | $0.01157 | $0.00713 | 60% | 1161 | 57% |
| tdigest_healpix_o9_sharded | 3,112,138 | 282.8 | $0.00754 | $0.00465 | 39% | 1233 | 60% |

Machine-readable companion: `metrics.json` (same directory).