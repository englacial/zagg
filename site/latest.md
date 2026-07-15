### Latest Lambda benchmark — `bbedc81`

_2026-07-15T00:52:38Z · arm64 · 4.0 GB · $1.33334e-05/GB-s · one densest shard/target · retained merge point._

| target | obs | runtime (s) | wall (s) | finalize (s) | cost/shard | cost/100 km² | % timeout | mem (MB) | % cap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tdigest_healpix_o9_inline_mask | 3,329,701 | 126.9 | 132.2 | 0.0 | $0.00677 | $0.00417 | 18% | 703 | 17% |
| tdigest_healpix_o9_inline_nomask | 3,329,701 | 114.6 | 122.2 | 0.0 | $0.00611 | $0.00377 | 16% | 718 | 18% |
| tdigest_healpix_o9_sidecar_mask | 3,329,701 | 115.7 | 119.7 | 0.0 | $0.00617 | $0.00380 | 16% | 647 | 16% |
| tdigest_healpix_o9_sidecar_nomask | 3,329,701 | 103.2 | 109.8 | 0.0 | $0.00550 | $0.00339 | 14% | 626 | 15% |

Machine-readable companion: `metrics.json` (same directory).