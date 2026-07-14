"""Tests for the pull-NN raster path (issue #218).

``grid.sample()`` (HEALPix + rectilinear) and ``processing.raster`` are
exercised end-to-end against synthetic tiled GeoTIFFs written by a minimal
in-test writer (classic little-endian TIFF, uncompressed 32x32 uint16 tiles,
pixel-scale + tiepoint georeferencing) — no GDAL, no network: async-tiff
reads them through a LocalStore.
"""

import asyncio
import struct

import numpy as np
import pytest
from pyproj import CRS, Transformer

from zagg.config import default_config
from zagg.grids import HealpixGrid, RectilinearGrid
from zagg.grids.base import sample_nearest
from zagg.processing.raster import _geo_from_ifd, _store_and_path, sample_asset

UTM18 = "EPSG:32618"
# 96x96 @ 10 m raster anchored at an 18N origin (mid-latitude, ~39.7N).
ORIGIN = (300000.0, 4400040.0)
RES = 10.0
TRANSFORM = (RES, 0.0, ORIGIN[0], 0.0, -RES, ORIGIN[1])


def _write_tiff(path, data, *, tile=32, epsg=32618, origin=ORIGIN, res=RES, geo=True, bands=1):
    """Minimal tiled uncompressed little-endian GeoTIFF (uint16).

    ``bands > 1`` writes a pixel-interleaved multi-band raster (each band a copy
    of ``data``) so the single-band guard in the reader can be exercised; the
    ``bands == 1`` layout is byte-identical to before.
    """
    data = np.ascontiguousarray(data, dtype=np.uint16)
    h, w = data.shape
    th = tw = tile
    ntr, ntc = -(-h // th), -(-w // tw)
    blocks = []
    for r in range(ntr):
        for c in range(ntc):
            blk = np.zeros((th, tw), dtype=np.uint16)
            sub = data[r * th : (r + 1) * th, c * tw : (c + 1) * tw]
            blk[: sub.shape[0], : sub.shape[1]] = sub
            if bands > 1:
                blk = np.repeat(blk[:, :, None], bands, axis=2)
            blocks.append(np.ascontiguousarray(blk).tobytes())
    n = len(blocks)
    pos = 8
    offsets = []
    for b in blocks:
        offsets.append(pos)
        pos += len(b)
    bytecounts = [len(b) for b in blocks]

    ext = []

    def extern(fmt, vals):
        nonlocal pos
        raw = struct.pack("<" + fmt * len(vals), *vals)
        off = pos
        ext.append(raw)
        pos += len(raw)
        return off

    off_to = extern("L", offsets) if n > 1 else offsets[0]
    off_bc = extern("L", bytecounts) if n > 1 else bytecounts[0]
    bps = extern("H", [16] * bands) if bands > 1 else 16
    sf = extern("H", [1] * bands) if bands > 1 else 1
    entries = [
        (256, "L", 1, w),
        (257, "L", 1, h),
        (258, "H", bands, bps),
        (259, "H", 1, 1),
        (262, "H", 1, 1),
        (277, "H", 1, bands),
        (322, "H", 1, tw),
        (323, "H", 1, th),
        (324, "L", n, off_to),
        (325, "L", n, off_bc),
        (339, "H", bands, sf),
    ]
    if geo:
        # GTModelType=1 (projected), GTRasterType=1 (area), ProjectedCSType=epsg
        geokeys = [1, 1, 0, 3, 1024, 0, 1, 1, 1025, 0, 1, 1, 3072, 0, 1, epsg]
        entries += [
            (33550, "d", 3, extern("d", [res, res, 0.0])),
            (33922, "d", 6, extern("d", [0.0, 0.0, 0.0, origin[0], origin[1], 0.0])),
            (34735, "H", 16, extern("H", geokeys)),
        ]
    types = {"H": 3, "L": 4, "d": 12}
    ifd = struct.pack("<H", len(entries))
    for tag, fmt, count, val in entries:
        vfield = (
            struct.pack("<HH", val, 0) if (fmt == "H" and count == 1) else struct.pack("<L", val)
        )
        ifd += struct.pack("<HHL", tag, types[fmt], count) + vfield
    ifd += struct.pack("<L", 0)

    with open(path, "wb") as f:
        f.write(struct.pack("<2sHL", b"II", 42, pos))
        for b in blocks:
            f.write(b)
        for raw in ext:
            f.write(raw)
        f.write(ifd)


def _index_raster(h=96, w=96):
    """Values encode pixel identity: data[r, c] = r * w + c (fits uint16)."""
    return (np.arange(h * w, dtype=np.uint16)).reshape(h, w)


def _rect_grid(res=RES, size=96):
    bounds = [ORIGIN[0], ORIGIN[1] - size * RES, ORIGIN[0] + size * RES, ORIGIN[1]]
    n = int(size * RES / res)
    return RectilinearGrid(
        UTM18, res, bounds, [n // 2, n // 2], config=default_config("atl06_polar")
    )


class TestSampleNearest:
    def test_north_up_pixel_centers(self):
        xs = ORIGIN[0] + np.array([5.0, 15.0, 955.0])
        ys = ORIGIN[1] - np.array([5.0, 25.0, 955.0])
        rows, cols, valid = sample_nearest(xs, ys, UTM18, UTM18, TRANSFORM, (96, 96))
        assert valid.all()
        assert cols.tolist() == [0, 1, 95]
        assert rows.tolist() == [0, 2, 95]

    def test_corner_point_floors_into_pixel(self):
        rows, cols, valid = sample_nearest(
            [ORIGIN[0] + 10.0], [ORIGIN[1] - 10.0], UTM18, UTM18, TRANSFORM, (96, 96)
        )
        assert valid.all() and rows[0] == 1 and cols[0] == 1

    def test_out_of_bounds_masked(self):
        xs = [ORIGIN[0] - 5.0, ORIGIN[0] + 5.0, ORIGIN[0] + 965.0]
        ys = [ORIGIN[1] - 5.0] * 3
        _, _, valid = sample_nearest(xs, ys, UTM18, UTM18, TRANSFORM, (96, 96))
        assert valid.tolist() == [False, True, False]

    def test_rotated_affine(self):
        # x = 10*row + x0 ; y = -10*col + y0 (a 90-degree-rotated raster)
        t = (0.0, 10.0, ORIGIN[0], -10.0, 0.0, ORIGIN[1])
        x = 10.0 * 3.5 + ORIGIN[0]
        y = -10.0 * 7.5 + ORIGIN[1]
        rows, cols, valid = sample_nearest([x], [y], UTM18, UTM18, t, (96, 96))
        assert valid.all() and rows[0] == 3 and cols[0] == 7

    def test_nine_element_transform(self):
        t = list(TRANSFORM) + [0.0, 0.0, 1.0]
        rows, cols, valid = sample_nearest(
            [ORIGIN[0] + 5.0], [ORIGIN[1] - 5.0], UTM18, UTM18, t, (96, 96)
        )
        assert valid.all() and rows[0] == 0 and cols[0] == 0

    def test_degenerate_transform_raises(self):
        with pytest.raises(ValueError, match="degenerate"):
            sample_nearest([0.0], [0.0], UTM18, UTM18, (0, 0, 0, 0, 0, 0), (1, 1))

    def test_reprojection_between_crs(self):
        # A WGS84 point equal to a known UTM pixel center round-trips.
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 45.0, ORIGIN[1] - 25.0)
        rows, cols, valid = sample_nearest([lon], [lat], "EPSG:4326", UTM18, TRANSFORM, (96, 96))
        assert valid.all() and rows[0] == 2 and cols[0] == 4


class TestRectilinearSample:
    def test_identity_resolution(self):
        grid = _rect_grid()
        cells = np.arange(96 * 96)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        assert valid.all()
        np.testing.assert_array_equal(rows, cells // 96)
        np.testing.assert_array_equal(cols, cells % 96)

    def test_coarser_grid_hits_interior_pixels(self):
        grid = _rect_grid(res=20.0)  # 48x48 cells over the same footprint
        rows, cols, valid = grid.sample(np.array([0]), UTM18, TRANSFORM, (96, 96))
        assert valid.all() and rows[0] == 1 and cols[0] == 1


class TestHealpixSample:
    def _truth_cells(self, lat0, lon0, aeqd, half=250.0):
        """Order-19 cells overlapping the patch interior (2 m ground truth)."""
        from mortie import clip2order, geo2mort

        to_geo = Transformer.from_crs(aeqd, CRS("EPSG:4326"), always_xy=True)
        ax = np.arange(-half, half, 2.0) + 1.0
        xx, yy = np.meshgrid(ax, ax)
        lons, lats = to_geo.transform(xx.ravel(), yy.ravel())
        leaf = geo2mort(np.asarray(lats), np.asarray(lons), order=29, points=True)
        return np.unique(clip2order(19, leaf))

    @pytest.mark.parametrize("site", [(0.0, 10.0), (41.81, 45.0)])
    def test_dense_by_construction_at_hostile_sites(self, site):
        lat0, lon0 = site
        aeqd = CRS.from_proj4(f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} +datum=WGS84 +units=m")
        cells = self._truth_cells(lat0, lon0, aeqd)
        # 100x100 @ 10 m raster centered on the patch (bounds well beyond it).
        t = (10.0, 0.0, -500.0, 0.0, -10.0, 500.0)
        grid = HealpixGrid(11, 19)
        rows, cols, valid = grid.sample(cells, aeqd.to_wkt(), t, (100, 100))
        # Pull-NN density guarantee: every covered cell gets a source pixel —
        # the push-hash holes measured on #218 (up to ~4%) cannot occur.
        assert valid.all()
        assert cells.size > 400

    def test_matches_manual_projection(self):
        from mortie import clip2order, geo2mort

        grid = HealpixGrid(11, 19)
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
        cells = np.unique(
            clip2order(19, geo2mort(np.array([lat]), np.array([lon]), order=29, points=True))
        )
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        # Independent computation from the cell center coordinates.
        clats, clons = grid.cell_centers(cells)
        to_utm = Transformer.from_crs(CRS("EPSG:4326"), CRS(UTM18), always_xy=True)
        x, y = to_utm.transform(clons, clats)
        assert valid.all()
        np.testing.assert_array_equal(cols, np.floor((x - ORIGIN[0]) / RES).astype(np.int64))
        np.testing.assert_array_equal(rows, np.floor((ORIGIN[1] - y) / RES).astype(np.int64))


class TestGeoFromIFD:
    def _open_ifd(self, path):
        from async_tiff import TIFF

        async def go():
            store, name = _store_and_path(str(path))
            return (await TIFF.open(name, store=store)).ifds[0]

        return asyncio.run(go())

    def test_reads_epsg_and_affine(self, tmp_path):
        p = tmp_path / "t.tif"
        _write_tiff(p, _index_raster())
        epsg, transform = _geo_from_ifd(self._open_ifd(p))
        assert epsg == 32618
        assert transform == TRANSFORM

    def test_missing_geo_raises(self, tmp_path):
        p = tmp_path / "bare.tif"
        _write_tiff(p, _index_raster(), geo=False)
        with pytest.raises(ValueError, match="GeoKeyDirectory|GeoTIFF"):
            _geo_from_ifd(self._open_ifd(p))


class TestSampleAsset:
    def test_identity_gather_multi_tile(self, tmp_path):
        """1:1 grid over a 3x3-tiled index raster: exact value round-trip."""
        p = tmp_path / "t.tif"
        data = _index_raster()
        _write_tiff(p, data)
        grid = _rect_grid()
        cells = np.arange(96 * 96)
        values, valid = sample_asset(grid, cells, str(p))
        assert valid.all()
        assert values.dtype == np.uint16
        np.testing.assert_array_equal(values, data.ravel())

    def test_out_of_bounds_fill(self, tmp_path):
        p = tmp_path / "t.tif"
        _write_tiff(p, _index_raster())
        # Grid footprint twice the raster's: outer cells fall off the raster.
        bounds = [ORIGIN[0] - 960.0, ORIGIN[1] - 1920.0, ORIGIN[0] + 960.0, ORIGIN[1]]
        grid = RectilinearGrid(UTM18, RES, bounds, [96, 96], config=default_config("atl06_polar"))
        cells = np.arange(192 * 192)
        values, valid = sample_asset(grid, cells, str(p), fill=7)
        assert not valid.all() and valid.any()
        assert (values[~valid] == 7).all()
        # In-bounds cells land on the raster's right half: spot-check one.
        inside = np.flatnonzero(valid)[0]
        r, c = inside // 192, inside % 192
        assert values[inside] == _index_raster()[r, c - 96]

    def test_healpix_end_to_end(self, tmp_path):
        from mortie import clip2order, geo2mort

        p = tmp_path / "t.tif"
        data = _index_raster()
        _write_tiff(p, data)
        grid = HealpixGrid(11, 19)
        # Cells from points strictly inside the raster footprint.
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        gx, gy = np.meshgrid(
            ORIGIN[0] + np.arange(100.0, 900.0, 40.0), ORIGIN[1] - np.arange(100.0, 900.0, 40.0)
        )
        lons, lats = to_wgs.transform(gx.ravel(), gy.ravel())
        cells = np.unique(
            clip2order(19, geo2mort(np.asarray(lats), np.asarray(lons), order=29, points=True))
        )
        values, valid = sample_asset(grid, cells, str(p))
        assert valid.all()
        rows, cols, _ = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        np.testing.assert_array_equal(values, data[rows, cols])

    def test_sync_facade_under_running_loop(self, tmp_path):
        """The sync entry works from inside a running loop (Jupyter/Binder)."""
        p = tmp_path / "t.tif"
        data = _index_raster()
        _write_tiff(p, data)
        grid = _rect_grid()
        cells = np.arange(96 * 96)

        async def call_sync():
            return sample_asset(grid, cells, str(p))

        values, valid = asyncio.run(call_sync())
        assert valid.all()
        assert values.dtype == np.uint16
        np.testing.assert_array_equal(values, data.ravel())

    def test_multiband_asset_raises(self, tmp_path):
        """A multi-band COG fails loudly rather than silently reading band 0."""
        p = tmp_path / "rgb.tif"
        _write_tiff(p, _index_raster(), bands=2)
        grid = _rect_grid()
        cells = np.arange(96 * 96)
        with pytest.raises(ValueError, match="single-band"):
            sample_asset(grid, cells, str(p))

    def test_no_valid_cells_returns_fill_in_asset_dtype(self, tmp_path):
        p = tmp_path / "t.tif"
        _write_tiff(p, _index_raster())
        # A grid footprint fully east of the raster: nothing lands on it.
        bounds = [ORIGIN[0] + 5000.0, ORIGIN[1] - 960.0, ORIGIN[0] + 5960.0, ORIGIN[1]]
        grid = RectilinearGrid(UTM18, RES, bounds, [48, 48], config=default_config("atl06_polar"))
        values, valid = sample_asset(grid, np.arange(96 * 96), str(p), fill=3)
        assert not valid.any()
        assert values.dtype == np.uint16 and (values == 3).all()


class TestStoreAndPath:
    def test_s3_scheme(self):
        store, path = _store_and_path("s3://bkt/some/key.tif", region="us-west-2")
        assert type(store).__name__ == "S3Store"
        assert path == "some/key.tif"

    def test_virtual_hosted_https(self):
        href = "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/a/B04.tif"
        store, path = _store_and_path(href)
        assert type(store).__name__ == "S3Store"
        assert path == "a/B04.tif"

    def test_plain_https(self):
        store, path = _store_and_path("https://example.com/dir/f.tif")
        assert type(store).__name__ == "HTTPStore"
        assert path == "dir/f.tif"

    def test_local(self, tmp_path):
        p = tmp_path / "f.tif"
        p.write_bytes(b"x")
        store, path = _store_and_path(str(p))
        assert type(store).__name__ == "LocalStore"
        assert path == "f.tif"
