### Latest Lambda benchmark — `dc26484`

_2026-07-05T20:31:28Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 264.2 | $0.00705 | $0.01738 | 37% | 892 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 231.0 | $0.00616 | $0.01519 | 32% | 908 | 44% |
| tdigest_healpix_o11_inner | 200,676 | 167.7 | $0.00447 | $0.04414 | 23% | 807 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 158.2 | $0.00422 | $0.04164 | 22% | 795 | 39% |
| tdigest_healpix_o9_inner | 3,112,138 | 442.1 | $0.01179 | $0.00727 | 61% | 1115 | 54% |
| tdigest_healpix_o9_sharded | 3,112,138 | 278.9 | $0.00744 | $0.00459 | 39% | 1173 | 57% |

Machine-readable companion: `metrics.json` (same directory).