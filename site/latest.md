### Latest Lambda benchmark — `5f29a04`

_2026-07-06T22:45:12Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 217.5 | $0.00580 | $0.01431 | 30% | 851 | 42% |
| tdigest_healpix_o10_inner | 939,217 | 221.8 | $0.00592 | $0.01459 | 31% | 903 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 183.0 | $0.00488 | $0.01204 | 25% | 904 | 44% |
| tdigest_healpix_o11_cached | 200,676 | 179.8 | $0.00479 | $0.04731 | 25% | 780 | 38% |
| tdigest_healpix_o11_inner | 200,676 | 177.7 | $0.00474 | $0.04675 | 25% | 795 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 170.9 | $0.00456 | $0.04496 | 24% | 789 | 39% |
| tdigest_healpix_o9_cached | 3,112,138 | 397.0 | $0.01059 | $0.00653 | 55% | 1099 | 54% |
| tdigest_healpix_o9_inner | 3,112,138 | 473.7 | $0.01263 | $0.00779 | 66% | 1154 | 56% |
| tdigest_healpix_o9_sharded | 3,112,138 | 340.8 | $0.00909 | $0.00560 | 47% | 1218 | 59% |

Machine-readable companion: `metrics.json` (same directory).