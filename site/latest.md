### Latest Lambda benchmark — `83003cb`

_2026-07-07T06:04:39Z · arm64 · 2.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_cached | 939,217 | 150.3 | $0.00401 | $0.00989 | 21% | 406 | 20% |
| tdigest_healpix_o10_inner | 939,217 | 145.8 | $0.00389 | $0.00959 | 20% | 469 | 23% |
| tdigest_healpix_o10_sharded | 939,217 | 110.0 | $0.00293 | $0.00724 | 15% | 473 | 23% |
| tdigest_healpix_o11_cached | 200,676 | 105.4 | $0.00281 | $0.02774 | 15% | 398 | 19% |
| tdigest_healpix_o11_inner | 200,676 | 106.7 | $0.00284 | $0.02807 | 15% | 439 | 21% |
| tdigest_healpix_o11_sharded | 200,676 | 95.7 | $0.00255 | $0.02517 | 13% | 423 | 21% |
| tdigest_healpix_o9_cached | 3,112,138 | 327.1 | $0.00872 | $0.00538 | 45% | 503 | 25% |
| tdigest_healpix_o9_inner | 3,112,138 | 332.8 | $0.00888 | $0.00547 | 46% | 529 | 26% |
| tdigest_healpix_o9_sharded | 3,112,138 | 169.5 | $0.00452 | $0.00279 | 24% | 538 | 26% |

Machine-readable companion: `metrics.json` (same directory).