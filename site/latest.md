### Latest Lambda benchmark — `5004df4`

_2026-07-07T21:29:10Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 86.1 | $0.00230 | $0.00566 | 12% | 471 | 23% |
| tdigest_healpix_o10_inner | 939,217 | 111.6 | $0.00298 | $0.00734 | 15% | 781 | 38% |
| tdigest_healpix_o10_sharded | 939,217 | 68.8 | $0.00184 | $0.00453 | 10% | 796 | 39% |
| tdigest_healpix_o11_cached | 200,676 | 48.3 | $0.00129 | $0.01270 | 7% | 419 | 20% |
| tdigest_healpix_o11_inner | 200,676 | 62.9 | $0.00168 | $0.01656 | 9% | 711 | 35% |
| tdigest_healpix_o11_sharded | 200,676 | 53.6 | $0.00143 | $0.01409 | 7% | 702 | 34% |
| tdigest_healpix_o9_cached | 3,112,138 | 240.7 | $0.00642 | $0.00396 | 33% | 650 | 32% |
| tdigest_healpix_o9_inner | 3,112,138 | 286.2 | $0.00763 | $0.00471 | 40% | 942 | 46% |
| tdigest_healpix_o9_sharded | 3,112,138 | 118.2 | $0.00315 | $0.00194 | 16% | 933 | 46% |

Machine-readable companion: `metrics.json` (same directory).