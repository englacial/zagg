### Latest Lambda benchmark — `3ea4132`

_2026-07-04T09:25:02Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 228.6 | $0.00610 | $0.01504 | 32% | 910 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 190.2 | $0.00507 | $0.01251 | 26% | 906 | 44% |
| tdigest_healpix_o11_inner | 200,676 | 158.5 | $0.00423 | $0.04170 | 22% | 801 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 150.8 | $0.00402 | $0.03969 | 21% | 793 | 39% |
| tdigest_healpix_o9_inner | 3,112,138 | 449.3 | $0.01198 | $0.00739 | 62% | 1139 | 56% |
| tdigest_healpix_o9_sharded | 3,112,138 | 304.6 | $0.00812 | $0.00501 | 42% | 1160 | 57% |

Machine-readable companion: `metrics.json` (same directory).