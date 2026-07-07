### Latest Lambda benchmark — `a168911`

_2026-07-07T16:00:13Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 97.4 | $0.00260 | $0.00641 | 14% | 465 | 23% |
| tdigest_healpix_o10_inner | 939,217 | 120.0 | $0.00320 | $0.00790 | 17% | 752 | 37% |
| tdigest_healpix_o10_sharded | 939,217 | 78.1 | $0.00208 | $0.00514 | 11% | 779 | 38% |
| tdigest_healpix_o11_cached | 200,676 | 43.5 | $0.00116 | $0.01144 | 6% | 431 | 21% |
| tdigest_healpix_o11_inner | 200,676 | 64.1 | $0.00171 | $0.01686 | 9% | 750 | 37% |
| tdigest_healpix_o11_sharded | 200,676 | 59.5 | $0.00159 | $0.01566 | 8% | 710 | 35% |
| tdigest_healpix_o9_cached | 3,112,138 | 241.3 | $0.00643 | $0.00397 | 34% | 630 | 31% |
| tdigest_healpix_o9_inner | 3,112,138 | 293.1 | $0.00782 | $0.00482 | 41% | 958 | 47% |
| tdigest_healpix_o9_sharded | 3,112,138 | 120.2 | $0.00321 | $0.00198 | 17% | 941 | 46% |

Machine-readable companion: `metrics.json` (same directory).