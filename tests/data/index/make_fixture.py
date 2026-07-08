"""Generate ``atl03_mini.h5``, the committed inline-index test fixture (issue #160).

The inline backend walks real v1 chunk B-trees, which no stub can serve, so
the tests read this tiny (~50 KB) synthetic ATL03-shaped granule through
h5coro's ``FileDriver``. Regenerating requires **h5py, which is deliberately
NOT a zagg dependency at any tier** — run this offline in any scratch env::

    python -m venv /tmp/fx && /tmp/fx/bin/pip install h5py numpy
    /tmp/fx/bin/python tests/data/index/make_fixture.py

Layout (two beams, ``gt1l``/``gt2l``, mirroring the ATL03 hierarchy the
shipped ``atl03.yaml`` config reads):

- ``/{beam}/heights/{lat_ph,lon_ph,h_ph}`` — chunked ``(CHUNK,)``,
  gzip+shuffle; ``signal_conf_ph`` — 2-D ``(N, 5)`` chunked ``(CHUNK, 5)``
  (chunk grid 1-wide in the trailing dim, like the real product).
- ``/{beam}/geolocation/{reference_photon_lat,reference_photon_lon,
  ph_index_beg,segment_ph_cnt}`` — small, **contiguous** (exercises the
  pseudo-chunk path of ``build_chunk_map``).

Datasets the shipped config never reads, present so the write-back
full-coverage walk (issue #190) has something beyond the read set to cover:

- ``/{beam}/heights/delta_time`` — chunked (a non-read chunked dataset),
  ``/{beam}/geolocation/segment_id`` — contiguous (a non-read pseudo-chunk).
- ``/ancillary_data/atlas_sdp_gps_epoch`` — a top-level (non-beam) group the
  read never descends into.
- ``/ancillary_data/data_start_utc`` — a fixed-length **string** dataset: an
  undecodable dtype the full walk records (``dtype == ""``) and the sidecar
  consumer skips at read (include-and-skip, issue #190).
- ``/ancillary_data/control`` — a **compact-layout** dataset (data lives in
  the object header, no file offset): the walk skips it at write.

Geometry is deterministic: 20 segments of 128 photons each, except segment 8
which is EMPTY (``cnt == 0`` and ``ph_index_beg == 0``, the real ATL03
empty-segment marker — issue #116), for 2432 photons total → 10 chunks of
256 (last partial). Every photon's lat equals its segment index (gt2l offset
by +100), so a test grid can select exact segment sets; 128-photon segments
put plan starts on ``k*256`` chunk boundaries for pinned segment choices
(the h5coro start-edge off-by-one regression case). ``signal_conf_ph``
column 0 flags every 7th photon ``-2`` (the TEP drop the shipped filter
uses); other columns are constant 9.
"""

import numpy as np

N_SEG = 20
SEG_LEN = 128
EMPTY_SEG = 8  # this segment has no photons (issue #116 marker)
CHUNK = 256
BEAMS = {"gt1l": 0, "gt2l": 100}  # beam -> segment-lat offset


def beam_arrays(lat_offset: int):
    counts = np.full(N_SEG, SEG_LEN, dtype=np.int32)
    counts[EMPTY_SEG] = 0
    n_ph = int(counts.sum())  # 2432
    ibeg = np.zeros(N_SEG, dtype=np.int64)  # 1-based; 0 marks the empty segment
    seg_lat = np.arange(N_SEG, dtype=np.float64) + lat_offset
    pos = 1
    lat_ph = np.empty(n_ph, dtype=np.float64)
    for s in range(N_SEG):
        if counts[s] == 0:
            continue
        ibeg[s] = pos
        lat_ph[pos - 1 : pos - 1 + counts[s]] = seg_lat[s]
        pos += counts[s]
    i = np.arange(n_ph)
    return {
        "heights/lat_ph": lat_ph,
        "heights/lon_ph": 0.001 * i + lat_offset,
        "heights/h_ph": (i % 50).astype(np.float32) + 0.25,
        "heights/signal_conf_ph": np.where(
            (i % 7 == 0)[:, None] & (np.arange(5) == 0)[None, :], -2, 9
        ).astype(np.int8),
        "geolocation/reference_photon_lat": seg_lat,
        "geolocation/reference_photon_lon": 0.001 * ibeg + lat_offset,
        "geolocation/ph_index_beg": ibeg,
        "geolocation/segment_ph_cnt": counts,
        # Not in the shipped read set (issue #190 full-coverage walk fodder):
        # a chunked dataset and a contiguous one the config never touches.
        "heights/delta_time": (0.0001 * i).astype(np.float64),
        "geolocation/segment_id": np.arange(N_SEG, dtype=np.int32) + 1,
    }


def _add_compact_dataset(f, name: str, arr: np.ndarray) -> None:
    """Create a COMPACT-layout dataset (data in the object header, no file
    offset) via h5py's low-level API — the write-back walk skips these."""
    import h5py

    space = h5py.h5s.create_simple(arr.shape)
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    dcpl.set_layout(h5py.h5d.COMPACT)
    tid = h5py.h5t.py_create(arr.dtype)
    dsid = h5py.h5d.create(f.id, name.encode(), tid, space, dcpl)
    dsid.write(h5py.h5s.ALL, h5py.h5s.ALL, np.ascontiguousarray(arr))


def main(out_path: str = "tests/data/index/atl03_mini.h5") -> None:
    import h5py

    with h5py.File(out_path, "w", libver="earliest") as f:
        for beam, lat_offset in BEAMS.items():
            for name, arr in beam_arrays(lat_offset).items():
                chunked = name.startswith("heights/")
                f.create_dataset(
                    f"{beam}/{name}",
                    data=arr,
                    chunks=((CHUNK,) + arr.shape[1:] if chunked else None),
                    compression=("gzip" if chunked else None),
                    compression_opts=(4 if chunked else None),
                    shuffle=chunked,
                )
        # A top-level (non-beam) group the read never descends into, holding
        # the edge dtypes/layouts the full-coverage walk must handle (#190):
        # a plain contiguous array, an undecodable fixed-length string, and a
        # compact-layout dataset.
        f.create_dataset(
            "ancillary_data/atlas_sdp_gps_epoch", data=np.array([1.198800018e9], dtype=np.float64)
        )
        f.create_dataset(
            "ancillary_data/data_start_utc", data=np.array(["2018-12-25T02:42:52Z"], dtype="S20")
        )
        _add_compact_dataset(f, "ancillary_data/control", np.arange(4, dtype=np.int32))


if __name__ == "__main__":
    main()
