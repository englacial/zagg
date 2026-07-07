### Latest Lambda benchmark — `33c9ed8`

_2026-07-07T04:17:19Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 236.0 | $0.00629 | $0.01552 | 33% | 911 | 44% |
| tdigest_healpix_o10_inner | 939,217 | 234.8 | $0.00626 | $0.01545 | 33% | 892 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 195.9 | $0.00522 | $0.01289 | 27% | 899 | 44% |
| tdigest_healpix_o11_cached | 200,676 | 175.9 | $0.00469 | $0.04629 | 24% | 810 | 40% |
| tdigest_healpix_o11_inner | 200,676 | 179.3 | $0.00478 | $0.04718 | 25% | 794 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 166.8 | $0.00445 | $0.04389 | 23% | 816 | 40% |
| tdigest_healpix_o9_cached | 3,112,138 | 413.4 | $0.01102 | $0.00680 | 57% | 1129 | 55% |
| tdigest_healpix_o9_inner | 3,112,138 | 452.2 | $0.01206 | $0.00744 | 63% | 1144 | 56% |
| tdigest_healpix_o9_sharded | 3,112,138 | 317.1 | $0.00846 | $0.00522 | 44% | 1208 | 59% |

Machine-readable companion: `metrics.json` (same directory).