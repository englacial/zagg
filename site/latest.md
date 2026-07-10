### Latest Lambda benchmark — `9dfa06c`

_2026-07-10T19:23:13Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o10_inline | 939,217 | 73.9 | 81.9 | 0.0 | $0.00394 | $0.00972 | 10% | 676 | 17% |
| tdigest_healpix_o10_sidecar | 939,217 | 53.8 | 61.7 | 0.0 | $0.00287 | $0.00708 | 7% | 484 | 12% |
| tdigest_healpix_o9_inline | 3,112,138 | 104.0 | 113.4 | 0.0 | $0.00555 | $0.00342 | 14% | 1098 | 27% |
| tdigest_healpix_o9_sidecar | 3,112,138 | 76.1 | 87.0 | 0.0 | $0.00406 | $0.00250 | 11% | 681 | 17% |

Machine-readable companion: `metrics.json` (same directory).