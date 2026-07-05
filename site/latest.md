### Latest Lambda benchmark — `904704c`

_2026-07-05T21:32:29Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 230.4 | $0.00614 | $0.01516 | 32% | 902 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 193.4 | $0.00516 | $0.01273 | 27% | 901 | 44% |
| tdigest_healpix_o11_inner | 200,676 | 184.3 | $0.00491 | $0.04849 | 26% | 792 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 175.3 | $0.00467 | $0.04613 | 24% | 798 | 39% |
| tdigest_healpix_o9_inner | 3,112,138 | 438.5 | $0.01169 | $0.00721 | 61% | 1167 | 57% |
| tdigest_healpix_o9_sharded | 3,112,138 | 295.0 | $0.00787 | $0.00485 | 41% | 1209 | 59% |

Machine-readable companion: `metrics.json` (same directory).