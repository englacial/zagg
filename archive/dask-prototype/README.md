# Archived: Dask-based Prototype

This folder contains the original Dask Gateway-based prototype implementation.

**Status:** Archived - Superseded by AWS Lambda implementation

## Original Features

- Dask Gateway for parallel processing (48 workers)
- Direct S3 access using h5coro
- Morton-based spatial indexing (HEALPix)
- Order 6 parent cells (~100km), Order 12 child statistics (~1.5km)
- Processing of 1,872 Antarctic drainage basin cells

## Why Archived

The production system uses AWS Lambda for cost-effective, serverless processing. The Lambda implementation provides the same functionality with:

- Better scalability (1,700+ concurrent workers)
- Lower operational overhead (no cluster management)
- More cost-effective ($12-15 per full run vs continuous cluster costs)
- Pre-computed granule catalogs (avoiding CMR rate limits)

## Preserved Files

- **demo_s3_xdggs.ipynb** - Complete Dask workflow demonstration showing the original parallel processing approach
- **example_usage.py** - CMR query examples (Note: References missing modules `query_cmr_stac_atl06.py` and `query_cmr_direct_atl06.py` that were never implemented)
- **environment.yml** - Conda environment specification for the Dask-based workflow
- **QUERY.md** - Historical documentation on CMR query approaches

## Production Implementation

For the current production implementation, see:

- **Source code:** `src/magg/` - Cloud-agnostic processing, catalog builder, authentication
- **AWS deployment:** `deployment/aws/` - Lambda handler and orchestrator
- **Documentation:** `docs/LAMBDA.md` - Complete Lambda deployment guide
- **Build:** `deployment/aws/build_arm64_layer.sh` - Lambda layer build for ARM64
