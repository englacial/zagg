# Catalog drift — pre-break baseline (Phase 0, #24)

Captured **before** the catalog-API refactor / hard break, so post-refactor
output (`new`) can be compared against `recent` on an identical granule set.

Regenerate with `bench/drift_catalog.py` (build + compare subcommands).

## Inputs

- **Granule set**: cycle-22 ATL06, Antarctic-bbox CMR pull, **4,103 granules**
  (cached at `/tmp/cmr_cycle22_atl06.pkl`). `recent_*` and future `new` builds
  reuse this exact set; `old` used a slightly different ~4,153-granule pull.
- **Grid**: `HealpixGrid(parent_order=6, child_order=6, layout="fullsphere")`.

| role | file | backend | shards | pairs |
|---|---|---|---|---|
| old | `old_cycle22_atl06_order6.json` | buggy mortie + EPSG:3031 (Dec 2025) | 1742 | 60,786 |
| recent | `recent_cycle22_atl06_order6_mortie.json` | mortie 0.7.2 MOC order 8 | 1330 | 76,577 |
| recent (oracle) | `recent_cycle22_atl06_order6_spherely.json` | spherely 0.1.1 brute (exact S2) | 1330 | 76,575 |

## Drift (pairs = `(shard_key, granule_basename)`)

**mortie vs spherely-oracle** — the current default is effectively exact:
- overlap **99.9974%**; mortie commission **2**, omission **0**.
- Confirms mortie-MOC@8 is a safe default (tiny commission, the harmless
  direction; extra files get read then filtered downstream).

**old vs spherely-oracle** — quantifies the historical bug:
- overlap **69.5%**; old commission **7,565**, **omission 23,354**.
- 412 spurious extra shards (EPSG:3031 distortion). The old catalog *missed
  ~30% of true overlaps* — this is the "stale mortie" regression from #23,
  now measured end-to-end.

## Expectation for `new` (post-refactor)

The refactor keeps the mortie-MOC math and swaps spherely brute → exact
`SpatialIndex` (same S2 predicate). So `new` should match `recent` to
**~100%** (0 omission, ≤ a couple commission). Any larger drift is a
regression to investigate, **not** expected improvement — the improvement
already happened (old → recent). This baseline is the reference, not an oracle.
