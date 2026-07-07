### Latest Lambda benchmark — `8183577`

_2026-07-07T06:44:39Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 168.3 | $0.00449 | $0.01107 | 23% | 333 | 16% |
| tdigest_healpix_o10_inner | 939,217 | 142.8 | $0.00381 | $0.00940 | 20% | 477 | 23% |
| tdigest_healpix_o10_sharded | 939,217 | 105.2 | $0.00281 | $0.00692 | 15% | 469 | 23% |
| tdigest_healpix_o11_cached | 200,676 | 96.8 | $0.00258 | $0.02548 | 13% | 277 | 14% |
| tdigest_healpix_o11_inner | 200,676 | 107.0 | $0.00285 | $0.02817 | 15% | 441 | 22% |
| tdigest_healpix_o11_sharded | 200,676 | 87.5 | $0.00233 | $0.02301 | 12% | 429 | 21% |
| tdigest_healpix_o9_cached | 3,112,138 | 322.3 | $0.00859 | $0.00530 | 45% | 499 | 24% |
| tdigest_healpix_o9_inner | 3,112,138 | 309.6 | $0.00826 | $0.00509 | 43% | 526 | 26% |
| tdigest_healpix_o9_sharded | 3,112,138 | 160.6 | $0.00428 | $0.00264 | 22% | 542 | 26% |

Machine-readable companion: `metrics.json` (same directory).