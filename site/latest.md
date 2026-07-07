### Latest Lambda benchmark — `ae943b7`

_2026-07-07T04:35:20Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 140.5 | $0.00375 | $0.00924 | 20% | 390 | 19% |
| tdigest_healpix_o10_inner | 939,217 | 140.3 | $0.00374 | $0.00923 | 19% | 461 | 23% |
| tdigest_healpix_o10_sharded | 939,217 | 101.9 | $0.00272 | $0.00670 | 14% | 463 | 23% |
| tdigest_healpix_o11_cached | 200,676 | 100.8 | $0.00269 | $0.02651 | 14% | 396 | 19% |
| tdigest_healpix_o11_inner | 200,676 | 100.1 | $0.00267 | $0.02633 | 14% | 425 | 21% |
| tdigest_healpix_o11_sharded | 200,676 | 90.9 | $0.00243 | $0.02393 | 13% | 423 | 21% |
| tdigest_healpix_o9_cached | 3,112,138 | 316.2 | $0.00843 | $0.00520 | 44% | 511 | 25% |
| tdigest_healpix_o9_inner | 3,112,138 | 328.2 | $0.00875 | $0.00540 | 46% | 546 | 27% |
| tdigest_healpix_o9_sharded | 3,112,138 | 165.6 | $0.00442 | $0.00272 | 23% | 556 | 27% |

Machine-readable companion: `metrics.json` (same directory).