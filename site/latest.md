### Latest Lambda benchmark — `260221b`

_2026-07-06T23:13:25Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 214.3 | $0.00571 | $0.01410 | 30% | 871 | 43% |
| tdigest_healpix_o10_inner | 939,217 | 222.2 | $0.00593 | $0.01462 | 31% | 898 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 183.8 | $0.00490 | $0.01209 | 26% | 901 | 44% |
| tdigest_healpix_o11_cached | 200,676 | 167.3 | $0.00446 | $0.04403 | 23% | 787 | 38% |
| tdigest_healpix_o11_inner | 200,676 | 167.0 | $0.00445 | $0.04396 | 23% | 806 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 157.2 | $0.00419 | $0.04137 | 22% | 806 | 39% |
| tdigest_healpix_o9_cached | 3,112,138 | 417.5 | $0.01113 | $0.00687 | 58% | 1135 | 55% |
| tdigest_healpix_o9_inner | 3,112,138 | 447.6 | $0.01194 | $0.00736 | 62% | 1122 | 55% |
| tdigest_healpix_o9_sharded | 3,112,138 | 290.1 | $0.00774 | $0.00477 | 40% | 1155 | 56% |

Machine-readable companion: `metrics.json` (same directory).