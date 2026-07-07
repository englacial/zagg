### Latest Lambda benchmark — `d4985eb`

_2026-07-07T00:01:19Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 217.5 | $0.00580 | $0.01431 | 30% | 867 | 42% |
| tdigest_healpix_o10_inner | 939,217 | 228.6 | $0.00610 | $0.01504 | 32% | 893 | 44% |
| tdigest_healpix_o10_sharded | 939,217 | 227.4 | $0.00606 | $0.01496 | 32% | 898 | 44% |
| tdigest_healpix_o11_cached | 200,676 | 177.4 | $0.00473 | $0.04667 | 25% | 803 | 39% |
| tdigest_healpix_o11_inner | 200,676 | 179.3 | $0.00478 | $0.04719 | 25% | 796 | 39% |
| tdigest_healpix_o11_sharded | 200,676 | 171.3 | $0.00457 | $0.04508 | 24% | 811 | 40% |
| tdigest_healpix_o9_cached | 3,112,138 | 460.6 | $0.01228 | $0.00757 | 64% | 1151 | 56% |
| tdigest_healpix_o9_inner | 3,112,138 | 453.7 | $0.01210 | $0.00746 | 63% | 1164 | 57% |
| tdigest_healpix_o9_sharded | 3,112,138 | 341.3 | $0.00910 | $0.00561 | 47% | 1237 | 60% |

Machine-readable companion: `metrics.json` (same directory).