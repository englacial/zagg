### Latest Lambda benchmark — `dca9a91`

_2026-07-05T20:44:15Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 239.5 | $0.00639 | $0.01575 | 33% | 908 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 215.1 | $0.00574 | $0.01415 | 30% | 903 | 44% |
| tdigest_healpix_o11_inner | 200,676 | 161.1 | $0.00430 | $0.04240 | 22% | 796 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 152.1 | $0.00406 | $0.04003 | 21% | 796 | 39% |
| tdigest_healpix_o9_inner | 3,112,138 | 439.2 | $0.01171 | $0.00722 | 61% | 1144 | 56% |
| tdigest_healpix_o9_sharded | 3,112,138 | 297.2 | $0.00793 | $0.00489 | 41% | 1140 | 56% |

Machine-readable companion: `metrics.json` (same directory).