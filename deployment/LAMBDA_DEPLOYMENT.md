# Lambda Deployment Guide

## Current State (2026-02-18)

The Lambda function `process-morton-cell` is temporarily running on **x86_64 / py3.11**
for testing. The target architecture is **arm64 / py3.12** (20% cheaper per GB-second).

### Current Config
- **Runtime**: python3.11
- **Architecture**: x86_64
- **Layer**: `xagg-dependencies:1` (x86_64, py3.11, h5coro==0.0.8)
- **Function code**: `lambda_handler.py` + `magg/` package + obstore/zarr/pydantic/pyyaml
- **Role**: `magg-lambda-execution` (scoped to `xagg` bucket)

### What's in the layer vs function code

**Layer** (`xagg-dependencies:1`, 222MB unzipped):
numpy, pandas, h5coro, mortie, healpy, earthaccess, boto3, astropy, shapely, cramjam,
fastparquet, requests, s3fs, and transitive deps.

**Function code** (20MB unzipped):
`lambda_handler.py`, `magg/` package, obstore, zarr, pydantic-zarr, pyyaml, pydantic,
pydantic-core, typeguard, typing_inspect, annotated-types.

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
  numpy==2.2.6 pandas==2.2.3 h5coro==0.0.8 mortie healpy earthaccess \
  boto3 fastparquet pyarrow shapely cramjam astropy requests

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
  --layer-name magg-deps-arm64 \
  --compatible-runtimes python3.12 \
  --compatible-architectures arm64 \
  --zip-file fileb:///tmp/lambda_layer_arm64.zip \
  --region us-west-2

# Update function
aws lambda update-function-configuration \
  --function-name process-morton-cell \
  --runtime python3.12 \
  --layers "arn:aws:lambda:us-west-2:429435741471:layer:magg-deps-arm64:1" \
  --region us-west-2

# Then update code with arm64 arch
aws lambda update-function-code \
  --function-name process-morton-cell \
  --zip-file fileb:///tmp/lambda_function.zip \
  --architectures arm64 \
  --region us-west-2
```

### Option B: CI/CD on GitHub Actions with macOS Apple Silicon (recommended)

GitHub provides free macOS Apple Silicon runners for public repos. This is the best
option because Linux ARM64 runners have had issues building some of our deps.

Key findings:
- `macos-15` runners use M1 Apple Silicon (ARM64), 3 CPUs, 7GB RAM
- Free for public repos (englacial/magg is public), $0.062/min for private
- Docker is NOT available on macOS ARM64 runners (Apple Virtualization limitation)
- `pip install --platform manylinux2014_aarch64 --only-binary=:all:` works from macOS
  to cross-compile Lambda layers — no Docker needed
- All our deps have pre-built `manylinux2014_aarch64` wheels on PyPI

See `.github/workflows/deploy-lambda.yml` for the workflow (to be created).

### Option C: Cross-compile from x86_64 (current approach for testing)

Works for packages with pre-built `manylinux_2_17_x86_64` wheels. Download with:
```bash
pip download --python-version 311 --platform manylinux_2_17_x86_64 \
  --only-binary :all: --no-deps <package> -d /tmp/wheels
```
Then unzip wheels into the build directory. This is what we're doing now for testing.

---

## Deploying Updated Function Code (no layer change)

When only `lambda_handler.py` or `magg/` package code changes (no new deps):

```bash
# Build zip
rm -rf /tmp/lambda_build && mkdir -p /tmp/lambda_build
cp deployment/aws/lambda_handler.py /tmp/lambda_build/
cp -r src/magg /tmp/lambda_build/magg

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
  --function-name process-morton-cell \
  --zip-file fileb:///tmp/lambda_function.zip \
  --region us-west-2
```

---

## CI/CD Workflow Design

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
| Layer (xagg-dependencies:1) | 222MB | x86_64/py3.11 |
| Function code | 20MB | Without numcodecs |
| **Total** | **242MB** | Under 250MB limit |

If the ARM64/py3.12 layer is larger, we may need to split into two layers or move some
deps from the layer into the function code (or vice versa).

---

## Build Infrastructure

### Scripts
- `deployment/aws/build_layer_v14.sh` — x86_64 layer build (runs in AL2023 Docker container)
- `deployment/aws/build_arm64_layer.sh` — arm64 layer build (runs in manylinux Docker container)
- `deployment/aws/build_function.sh` — function code build (handler + magg + non-layer deps)

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
