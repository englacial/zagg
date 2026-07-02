### Latest Lambda benchmark — `8bece8b`

_2026-07-02T01:03:59Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inner | 939,217 | 257.1 | $0.00686 | $0.01691 | 36% | 1903 | 93% |
| tdigest_healpix_o10_sharded | 939,217 | 211.3 | $0.00564 | $0.01390 | 29% | 882 | 43% |
| tdigest_healpix_o11_inner | 200,676 | 183.3 | $0.00489 | $0.04823 | 25% | 1724 | 84% |
| tdigest_healpix_o11_sharded | 200,676 | 174.5 | $0.00465 | $0.04593 | 24% | 1508 | 74% |
| tdigest_healpix_o9_inner | 3,112,138 | 499.4 | $0.01332 | $0.00821 | 69% | 1111 | 54% |
| tdigest_healpix_o9_sharded | 3,112,138 | 326.2 | $0.00870 | $0.00536 | 45% | 1206 | 59% |

Machine-readable companion: `metrics.json` (same directory).