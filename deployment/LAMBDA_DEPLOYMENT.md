# Lambda Deployment Guide

> **Status: maintainer notes — partially out of date (audited against the code
> 2026-06-15, see [#34](https://github.com/englacial/zagg/issues/34)).** The
> canonical, rendered deploy docs live in the docs site:
> [Standing Up the Backend](../docs/deployment/standup.md) (preferred path),
> [AWS Lambda](../docs/deployment/lambda.md), and
> [Execution Role](../docs/deployment/execution-role.md). This file keeps the
> build/layer internals and the size/cost rationale; several figures below
> (layer/function sizes, the role/bucket names, the layer contents) are
> historical and were not all re-measured — trust the scripts
> (`build_layer.sh` / `build_function.sh`) and `template.yaml` over the numbers
> here. Specific corrections are inline below.

## Current State (2026-02-18)

Both architectures now build on **py3.12** (manylinux_2_28). The target /
production architecture is **arm64 / py3.12** (20% cheaper per GB-second);
x86_64 / py3.12 is available for local/testing parity.

### Current Config
- **Runtime**: python3.12
- **Architecture**: arm64 (default; x86_64 also supported)
- **Layer**: `zagg-deps-{arch}` (py3.12, pyproj/odc-geo for rectilinear grids, h5coro==0.0.8)
- **Function code**: `lambda_handler.py` + `zagg/` package + obstore/zarr/pydantic-zarr/pyyaml
- **Role**: created by `template.yaml` (CloudFormation-auto-named; the template
  sets no `RoleName`), scoped least-privilege to the `OutputBucketName` bucket
  you pass to `stand_up.sh` — *not* a fixed `zagg-lambda-execution`/`xagg`. (The
  dependency layer is named `<FunctionName>-deps`, default `process-shard-deps`.
  The legacy `deploy.sh` in-place updater still defaults `ZAGG_S3_BUCKET=xagg`
  for its >50MB staging copies; that is the updater's staging bucket, not the
  output bucket.)

### What's in the layer vs function code

**Layer** (built by `build_layer.sh`):
numpy, pandas, fastparquet, cramjam, shapely, pyproj, odc-geo, affine, cachetools,
h5coro, mortie, and their transitive deps. (Corrected: `earthaccess` is
orchestrator-only and **not** in the layer; `boto3` is provided by the Lambda
runtime; `astropy`/`s3fs`/`pyarrow` are not installed by `build_layer.sh`. The old
`xagg-dependencies:1`/222MB figure is historical.)

**Function code** (built by `build_function.sh`):
`lambda_handler.py`, `zagg/` package, plus `obstore`, `zarr`, `pydantic-zarr`,
`pyyaml` and their transitive deps; packages already in the layer are stripped
back out to avoid duplication.

---

## Standing up the backend (CloudFormation — recommended)

For a reproducible standup in any AWS account, use the committed
`deployment/aws/template.yaml`, which creates the execution role, dependency
layer, and function as a single stack from the pre-built release zips:

```bash
OUTPUT_BUCKET=my-results-bucket bash deployment/aws/stand_up.sh
```

The Lambda code (deps layer + function zips) lives on the public **source.coop
mirror** (`s3://us-west-2.opendata.source.coop/englacial/zagg/lambda/<minor>/`),
keyed by zagg minor version. CloudFormation reads Lambda code from a same-region
bucket, so:

- **us-west-2** — `stand_up.sh` points the stack straight at the mirror; no
  staging bucket of your own is needed.
- **other regions** — pass `STAGING_BUCKET` (a bucket you own in `REGION`); the
  zips are copied into it from the mirror, then the stack reads them there.

It then runs `aws cloudformation deploy`. The minor is read from the repo's
latest git tag (so a clone needs no install), or the installed zagg, unless
`LAMBDA_VERSION` is set. To (re)populate the mirror after a release,
maintainers run `deployment/aws/publish_mirror.sh <minor>`. See
[docs/deployment/lambda.md](../docs/deployment/lambda.md) for the parameter
table and overrides.

`deploy.sh` (below) is the maintainer path for *in-place updates* to an
already-deployed function and does not create the role/function/bucket.

---

## Rebuilding the ARM64 Layer

### Why
ARM64 Lambda is 20% cheaper ($0.0000133334 vs $0.0000166667 per GB-second). At ~90,000
GB-seconds per full run, this saves ~$0.60/run. Over many runs it adds up.

### What needs to happen

1. Build a new layer with all deps compiled for `manylinux2014_aarch64` + `cp312`
2. The layer must include the same packages as `xagg-dependencies:1` but for ARM64/py3.12
3. Deploy the function with architecture `arm64` and runtime `python3.12`

### Option A: Build on Apple Silicon (manual)

On an Apple Silicon Mac:

```bash
# Create build directory
mkdir -p /tmp/layer_build/python

# Install deps targeting Lambda's manylinux environment
pip install \
  --platform manylinux2014_aarch64 \
  --target /tmp/layer_build/python \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  numpy==2.2.6 pandas==2.2.3 h5coro==0.0.8 mortie earthaccess \
  boto3 fastparquet pyarrow shapely pyproj odc-geo cramjam astropy requests

# Trim bloat
find /tmp/layer_build -type d -name '__pycache__' -exec rm -rf {} +
find /tmp/layer_build -type d -name '*.dist-info' -exec rm -rf {} +
find /tmp/layer_build -type d -name 'tests' -exec rm -rf {} +

# Check size (must be <250MB unzipped when combined with function code)
du -sh /tmp/layer_build/

# Zip
cd /tmp/layer_build && zip -qr /tmp/lambda_layer_arm64.zip python/

# Publish
aws lambda publish-layer-version \
  --layer-name zagg-deps-arm64 \
  --compatible-runtimes python3.12 \
  --compatible-architectures arm64 \
  --zip-file fileb:///tmp/lambda_layer_arm64.zip \
  --region us-west-2

# Update function
aws lambda update-function-configuration \
  --function-name process-shard \
  --runtime python3.12 \
  --layers "arn:aws:lambda:us-west-2:429435741471:layer:zagg-deps-arm64:1" \
  --region us-west-2

# Then update code with arm64 arch
aws lambda update-function-code \
  --function-name process-shard \
  --zip-file fileb:///tmp/lambda_function.zip \
  --architectures arm64 \
  --region us-west-2
```

### Option B: CI/CD on GitHub Actions with macOS Apple Silicon (recommended)

GitHub provides free macOS Apple Silicon runners for public repos. This is the best
option because Linux ARM64 runners have had issues building some of our deps.

Key findings:
- `macos-15` runners use M1 Apple Silicon (ARM64), 3 CPUs, 7GB RAM
- Free for public repos (englacial/zagg is public), $0.062/min for private
- Docker is NOT available on macOS ARM64 runners (Apple Virtualization limitation)
- `pip install --platform manylinux2014_aarch64 --only-binary=:all:` works from macOS
  to cross-compile Lambda layers — no Docker needed
- All our deps have pre-built `manylinux2014_aarch64` wheels on PyPI

The layer + function are now built in CI by `.github/workflows/lambda-build.yml` (both arches, with the combined 250MB size gate); the originally-planned `deploy-lambda.yml` was never added. The manual/CI options below are historical design notes.

### Option C: Cross-compile from x86_64 (current approach for testing)

Works for packages with pre-built `manylinux_2_17_x86_64` wheels. Download with:
```bash
pip download --python-version 311 --platform manylinux_2_17_x86_64 \
  --only-binary :all: --no-deps <package> -d /tmp/wheels
```
Then unzip wheels into the build directory. This is what we're doing now for testing.

---

## Deploying Updated Function Code (no layer change)

When only `lambda_handler.py` or `zagg/` package code changes (no new deps):

```bash
# Build zip
rm -rf /tmp/lambda_build && mkdir -p /tmp/lambda_build
cp deployment/aws/lambda_handler.py /tmp/lambda_build/
cp -r src/zagg /tmp/lambda_build/zagg

# Add deps not in layer (skip native ones if already unpacked)
pip install --target /tmp/lambda_build --no-deps \
  zarr pydantic-zarr pyyaml pydantic typeguard typing_inspect annotated-types

# For obstore (native): download correct wheel and unzip
pip download --python-version <VER> --platform <PLAT> --only-binary :all: \
  --no-deps obstore -d /tmp/wheels
unzip -qo /tmp/wheels/obstore-*.whl -d /tmp/lambda_build

# Clean and zip
find /tmp/lambda_build -type d -name '*.dist-info' -exec rm -rf {} +
find /tmp/lambda_build -type d -name '__pycache__' -exec rm -rf {} +
cd /tmp/lambda_build && zip -qr /tmp/lambda_function.zip .

# Deploy
aws lambda update-function-code \
  --function-name process-shard \
  --zip-file fileb:///tmp/lambda_function.zip \
  --region us-west-2
```

---

## CI/CD Workflow Design (historical design note)

> The build half of this is implemented in `.github/workflows/lambda-build.yml`
> (build + size gate + artifact upload). Automated *deploy*-on-push was never
> wired up; the source.coop mirror + `stand_up.sh` is the deploy path instead.
> Kept below as the original design rationale.

A GitHub Actions workflow for automated Lambda deployment should:

1. **Trigger**: on push to `lambda` branch (or manual dispatch)
2. **Runner**: `ubuntu-24.04-arm` for ARM64 builds, `ubuntu-latest` for x86_64
3. **Steps**:
   - Build Lambda layer (if deps changed)
   - Build function code zip
   - Publish layer version (if changed)
   - Deploy function code
   - Run a smoke test (invoke with a test event)
4. **Secrets needed**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (or OIDC)

The layer build should be a separate job that only runs when `pyproject.toml` changes.
Function code deployment should run on every push.

---

## Size Budget

Lambda limit: **250MB unzipped** (layer + function code combined)

| Component | Current Size | Notes |
|-----------|-------------|-------|
| Layer (zagg-deps) | ~125MB | py3.12; pyproj/odc-geo in, earthaccess + redundant zarr/obstore out |
| Function code | ~20MB | obstore/zarr/pydantic-zarr/pyyaml; without numcodecs |
| **Total** | **~145MB** | Comfortably under 250MB limit |

If a future dep pushes the layer larger, we may need to split into two layers or move some
deps from the layer into the function code (or vice versa).

---

## Build Infrastructure

### Scripts
- `deployment/aws/build_layer.sh [x86_64|arm64]` — Lambda layer build (runs in an arch-matched Docker container)
- `deployment/aws/build_function.sh` — function code build (handler + zagg + non-layer deps)

### CI/CD
- `.github/workflows/lambda-build.yml` — builds both layer + function for x86_64 and arm64,
  checks combined sizes against 250MB limit, uploads artifacts

### Tests
- `tests/test_lambda_build.py` — verifies imports, build scripts, size budgets, version consistency
  - Fast tests (`pytest tests/test_lambda_build.py -m "not slow"`): import checks, syntax, consistency
  - Slow tests (`pytest tests/test_lambda_build.py -m slow`): actual build + size verification

### Local Build
```bash
# Build function code (auto-detects arch and Python version)
deployment/aws/build_function.sh

# Build with combined size check (requires layer zip in deployment/layers/)
deployment/aws/build_function.sh --check-size
```
