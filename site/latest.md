### Latest Lambda benchmark — `183c762`

_2026-07-07T06:51:31Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 152.1 | $0.00405 | $0.01000 | 21% | 421 | 21% |
| tdigest_healpix_o10_inner | 939,217 | 135.3 | $0.00361 | $0.00890 | 19% | 486 | 24% |
| tdigest_healpix_o10_sharded | 939,217 | 99.4 | $0.00265 | $0.00654 | 14% | 513 | 25% |
| tdigest_healpix_o11_cached | 200,676 | 94.0 | $0.00251 | $0.02473 | 13% | 367 | 18% |
| tdigest_healpix_o11_inner | 200,676 | 90.2 | $0.00241 | $0.02374 | 13% | 564 | 28% |
| tdigest_healpix_o11_sharded | 200,676 | 78.7 | $0.00210 | $0.02070 | 11% | 637 | 31% |
| tdigest_healpix_o9_cached | 3,112,138 | 318.1 | $0.00848 | $0.00523 | 44% | 566 | 28% |
| tdigest_healpix_o9_inner | 3,112,138 | 305.3 | $0.00814 | $0.00502 | 42% | 556 | 27% |
| tdigest_healpix_o9_sharded | 3,112,138 | 154.2 | $0.00411 | $0.00254 | 21% | 878 | 43% |

Machine-readable companion: `metrics.json` (same directory).