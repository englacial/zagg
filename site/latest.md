### Latest Lambda benchmark — `3a87186`

_2026-07-04T07:39:30Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 255.9 | $0.00682 | $0.01683 | 36% | 903 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 192.3 | $0.00513 | $0.01265 | 27% | 912 | 45% |
| tdigest_healpix_o11_inner | 200,676 | 156.0 | $0.00416 | $0.04104 | 22% | 801 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 146.6 | $0.00391 | $0.03857 | 20% | 791 | 39% |
| tdigest_healpix_o9_inner | 3,112,138 | 433.3 | $0.01155 | $0.00713 | 60% | 1167 | 57% |
| tdigest_healpix_o9_sharded | 3,112,138 | 282.7 | $0.00754 | $0.00465 | 39% | 1209 | 59% |

Machine-readable companion: `metrics.json` (same directory).