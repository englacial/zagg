### Latest Lambda benchmark — `9107b06`

_2026-07-08T00:43:51Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 85.8 | 87.1 | 0.0 | $0.00229 | $0.00565 | 12% | 482 | 24% |
| tdigest_healpix_o10_inner | 939,217 | 107.4 | 109.7 | 0.0 | $0.00286 | $0.00707 | 15% | 761 | 37% |
| tdigest_healpix_o10_sharded | 939,217 | 78.2 | 85.7 | 0.0 | $0.00209 | $0.00515 | 11% | 788 | 38% |
| tdigest_healpix_o11_cached | 200,676 | 45.6 | 50.6 | 0.0 | $0.00122 | $0.01200 | 6% | 436 | 21% |
| tdigest_healpix_o11_inner | 200,676 | 68.4 | 69.1 | 0.0 | $0.00182 | $0.01799 | 9% | 736 | 36% |
| tdigest_healpix_o11_sharded | 200,676 | 57.9 | 63.4 | 0.0 | $0.00154 | $0.01524 | 8% | 727 | 36% |
| tdigest_healpix_o9_cached | 3,112,138 | 236.2 | 236.9 | 0.0 | $0.00630 | $0.00388 | 33% | 618 | 30% |
| tdigest_healpix_o9_inner | 3,112,138 | 271.3 | 275.4 | 0.0 | $0.00723 | $0.00446 | 38% | 941 | 46% |
| tdigest_healpix_o9_sharded | 3,112,138 | 123.8 | 144.5 | 0.0 | $0.00330 | $0.00204 | 17% | 953 | 47% |

Machine-readable companion: `metrics.json` (same directory).