"""Local processing runner for magg.

Usage:
    python -m magg --config atl06.yaml --catalog catalog.json
    python -m magg --config atl06.yaml --catalog catalog.json --store ./test.zarr
    python -m magg --config atl06.yaml --catalog catalog.json --max-cells 5
    python -m magg --config atl06.yaml --catalog catalog.json --morton-cell -4211322
"""

import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from zarr import consolidate_metadata

from magg.auth import get_nsidc_s3_credentials
from magg.config import get_child_order, get_store_path, load_config
from magg.processing import process_morton_cell, write_dataframe_to_zarr
from magg.schema import xdggs_zarr_template
from magg.store import open_store

logger = logging.getLogger(__name__)


def _process_and_write(cell, chunk_idx, granule_urls, parent_order, child_order,
                       s3_creds, store, config):
    """Process a single cell and write results to store."""
    df_out, metadata = process_morton_cell(
        parent_morton=int(cell),
        parent_order=parent_order,
        child_order=child_order,
        granule_urls=granule_urls,
        s3_credentials=s3_creds,
        config=config,
    )
    if not df_out.empty:
        write_dataframe_to_zarr(
            df_out, store,
            chunk_idx=chunk_idx,
            child_order=child_order,
            parent_order=parent_order,
        )
    return metadata


def main():
    parser = argparse.ArgumentParser(
        description="magg local processing runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m magg --config atl06.yaml --catalog catalog.json
  python -m magg --config atl06.yaml --catalog catalog.json --store ./test.zarr
  python -m magg --config atl06.yaml --catalog catalog.json --max-cells 5
  python -m magg --config atl06.yaml --catalog catalog.json --morton-cell -4211322
""",
    )
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--catalog", default=None, help="Path to granule catalog JSON (overrides config)")
    parser.add_argument("--store", default=None, help="Output store path (overrides config)")
    parser.add_argument("--max-cells", type=int, default=None, help="Limit number of cells (for testing)")
    parser.add_argument("--morton-cell", type=str, default=None, help="Process a specific morton cell")
    parser.add_argument("--max-workers", type=int, default=4, help="Max concurrent workers (default: 4)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Zarr template")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Load config
    config = load_config(args.config)
    child_order = get_child_order(config)

    # Resolve catalog: CLI > config > error
    catalog_path = args.catalog or config.catalog
    if not catalog_path:
        parser.error("No catalog specified (use --catalog or set catalog: in config)")

    # Resolve store: CLI > config > error
    store_path = args.store or get_store_path(config)
    if not store_path:
        parser.error("No store path specified (use --store or set output.store: in config)")

    # Load catalog
    print(f"Loading catalog from {catalog_path}...")
    with open(catalog_path) as f:
        catalog_data = json.load(f)
    metadata = catalog_data["metadata"]
    catalog = catalog_data["catalog"]
    parent_order = metadata["parent_order"]

    print(f"  Product: {metadata.get('short_name', '?')}")
    print(f"  Cells: {metadata['total_cells']}, Granules: {metadata['total_granules']}")
    print(f"  Parent order: {parent_order}, Child order: {child_order}")

    # Select cells
    all_cells = list(catalog.keys())
    if args.morton_cell:
        if args.morton_cell not in catalog:
            parser.error(f"Morton cell '{args.morton_cell}' not in catalog")
        cells = [args.morton_cell]
    elif args.max_cells:
        cells = all_cells[:args.max_cells]
    else:
        cells = all_cells

    print(f"  Processing {len(cells)} of {len(all_cells)} cells")

    if args.dry_run:
        granule_counts = [len(catalog[c]) for c in cells]
        print(f"\n[DRY RUN] Would process {len(cells)} cells")
        print(f"  Granules per cell: min={min(granule_counts)}, "
              f"max={max(granule_counts)}, avg={sum(granule_counts)/len(granule_counts):.1f}")
        print(f"  Output: {store_path}")
        return

    # Authenticate
    print("\nAuthenticating with NASA Earthdata...")
    s3_creds = get_nsidc_s3_credentials()

    # Open store and create template
    print(f"Opening store: {store_path}")
    store = open_store(store_path)
    store = xdggs_zarr_template(
        store, parent_order, child_order,
        overwrite=args.overwrite,
        n_parent_cells=metadata["total_cells"],
        config=config,
    )

    # Build cell-to-index mapping (must use all_cells for correct chunk indices)
    cell_to_idx = {cell: idx for idx, cell in enumerate(all_cells)}

    # Process
    print(f"\nProcessing with {args.max_workers} workers...")
    start_time = time.time()
    total_obs = 0
    cells_with_data = 0
    cells_error = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                _process_and_write,
                cell, cell_to_idx[cell], catalog[cell],
                parent_order, child_order,
                s3_creds, store, config,
            ): cell
            for cell in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            cell = futures[future]
            try:
                meta = future.result()
                if meta.get("error"):
                    print(f"  [{i}/{len(cells)}] {cell}: {meta['error']}")
                else:
                    obs = meta.get("total_obs", 0)
                    total_obs += obs
                    cells_with_data += 1
                    if i % 10 == 0 or len(cells) <= 20:
                        print(f"  [{i}/{len(cells)}] {cell}: {obs:,} obs")
            except Exception as e:
                cells_error += 1
                print(f"  [{i}/{len(cells)}] {cell}: ERROR {e}")

    # Consolidate
    print("\nConsolidating metadata...")
    consolidate_metadata(store, zarr_format=3)

    elapsed = time.time() - start_time
    print(f"\nDone: {cells_with_data} cells with data, {total_obs:,} obs, "
          f"{cells_error} errors, {elapsed:.1f}s")
    print(f"Output: {store_path}")


if __name__ == "__main__":
    main()
