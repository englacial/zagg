### Latest Lambda benchmark — `4d4da8a`

_2026-07-08T01:38:47Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 89.4 | 94.4 | 0.0 | $0.00238 | $0.00588 | 12% | 494 | 24% |
| tdigest_healpix_o10_inner | 939,217 | 108.7 | 110.2 | 0.0 | $0.00290 | $0.00715 | 15% | 824 | 40% |
| tdigest_healpix_o10_sharded | 939,217 | 70.6 | 75.2 | 0.0 | $0.00188 | $0.00464 | 10% | 784 | 38% |
| tdigest_healpix_o11_cached | 200,676 | 41.1 | 44.1 | 0.0 | $0.00110 | $0.01082 | 6% | 436 | 21% |
| tdigest_healpix_o11_inner | 200,676 | 62.4 | 62.9 | 0.0 | $0.00166 | $0.01642 | 9% | 719 | 35% |
| tdigest_healpix_o11_sharded | 200,676 | 51.9 | 56.5 | 0.0 | $0.00138 | $0.01365 | 7% | 737 | 36% |
| tdigest_healpix_o9_cached | 3,112,138 | 231.6 | 232.7 | 0.0 | $0.00618 | $0.00381 | 32% | 629 | 31% |
| tdigest_healpix_o9_inner | 3,112,138 | 283.5 | 289.5 | 0.0 | $0.00756 | $0.00466 | 39% | 949 | 46% |
| tdigest_healpix_o9_sharded | 3,112,138 | 105.6 | 123.2 | 0.0 | $0.00282 | $0.00174 | 15% | 944 | 46% |

Machine-readable companion: `metrics.json` (same directory).