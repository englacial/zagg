# ARM64 Lambda Layer

Building an ARM64 (Graviton2) Lambda layer on Apple Silicon Mac.

!!! note "The build command is normative in `deployment/LAMBDA_DEPLOYMENT.md`"
    The containerized layer build — the exact podman/docker invocation, the
    SELinux caveat, and what consumes the resulting zip — lives in
    [Rebuilding the Layer](https://github.com/englacial/zagg/blob/main/deployment/LAMBDA_DEPLOYMENT.md#rebuilding-the-layer).
    This page covers the Apple-Silicon-specific *why* (image choice, page
    alignment, glibc) and troubleshooting. If the two disagree, that section
    wins.

## Overview

M1/M2/M3 Macs run `linux/arm64` containers natively without emulation, making ARM builds fast and reliable.

## Requirements

### Lambda Runtime Constraints

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.12 | Must match Lambda runtime exactly |
| glibc | ≤2.34 | Amazon Linux 2023 uses glibc 2.34 |
| Architecture | aarch64 | ARM64/Graviton2 |

### Build Environment

| Component | Required |
|-----------|----------|
| macOS | Apple Silicon (M1/M2/M3) |
| Container runtime | podman (what we use; Docker Desktop / OrbStack / Colima also work) |
| Disk space | ~5 GB free |

### Container Image

Use `quay.io/pypa/manylinux_2_28_aarch64`:

- glibc 2.28 (compatible with Lambda's 2.34)
- Modern GCC toolchain (≥9.3, needed for numpy)
- Pre-configured for building Python wheels

## Build Steps

1. **Install podman** (if not already installed), and start its VM:

    ```bash
    brew install podman
    podman machine init && podman machine start
    ```

2. **Run the build inside the container** — `build_layer.sh` is not a bare-host
   command; it needs the manylinux cp312 toolchain and refuses to run on a
   non-`aarch64` machine. Its pins come from the `lambda` extra and, for
   everything else, from `deployment/aws/layer-requirements.txt`, which is
   generated from `uv.lock` (see [Dependency pins](lambda.md#zip-pins)). From
   the repo root:

    ```bash
    podman run --rm \
      -v "$(pwd)":/workspace \
      -w /workspace/deployment/aws \
      quay.io/pypa/manylinux_2_28_aarch64 \
      bash -c "yum install -y zip && chmod +x build_layer.sh && ./build_layer.sh arm64"
    ```

3. **Transfer the zip** (if building on a different machine):

    ```bash
    scp deployment/layers/lambda_layer_arm64.zip user@remote:/path/
    ```

## Deploying the Layer

Normally you don't publish a locally built zip: `deployment/aws/deploy.sh`
publishes from the CI artifact and updates the function config, and
`stand_up.sh` deploys release zips for a full standup. The manual equivalent,
for a one-off layer of your own:

```bash
# Upload to S3 (if >50MB)
aws s3 cp deployment/layers/lambda_layer_arm64.zip s3://your-bucket/layers/

# Create/update layer
aws lambda publish-layer-version \
    --layer-name zagg-layer-arm64 \
    --description "zagg dependencies for ARM64/Graviton2" \
    --content S3Bucket=your-bucket,S3Key=layers/lambda_layer_arm64.zip \
    --compatible-runtimes python3.12 \
    --compatible-architectures arm64
```

## Verifying the Build

Import only what the *layer* actually contains. `zarr`, `obstore` and
`pydantic-zarr` are function-code deps (`build_function.sh`) and are stripped
out of the layer; `earthaccess` is orchestrator-only and never installed into
it. Expecting any of those here will fail a good layer.

```bash
podman run --rm --platform linux/arm64 \
    -v ./deployment/layers/lambda_layer_arm64.zip:/layer.zip \
    public.ecr.aws/lambda/python:3.12 \
    bash -c '
        unzip -q /layer.zip -d /opt
        python3.12 -c "
import sys
sys.path.insert(0, \"/opt/python\")
import numpy; print(f\"numpy {numpy.__version__}\")
import pandas; print(f\"pandas {pandas.__version__}\")
import pyproj; print(f\"pyproj {pyproj.__version__}\")
import shapely; print(f\"shapely {shapely.__version__}\")
import xarray; print(f\"xarray {xarray.__version__}\")
import mortie; print(\"mortie OK\")
import h5coro; print(\"h5coro OK\")
print(\"All imports successful!\")
"
    '
```

## Troubleshooting

!!! danger "ELF load command address/offset not properly aligned"
    NumPy wasn't built with 64KB page alignment. Lambda ARM64 requires page alignment of 64KB (0x10000), but pre-built wheels use 4KB. The build script handles this with `LDFLAGS="-Wl,-z,max-page-size=0x10000"` and `--no-binary numpy`.

!!! warning "GLIBC_2.XX not found"
    Your build container has a newer glibc than Lambda. Use `manylinux_2_28` (glibc 2.28) which is compatible with Lambda's glibc 2.34.

!!! info "Slow build"
    Make sure the runtime is executing the image natively rather than emulating it — an `aarch64` image on Apple Silicon needs no emulation. `podman machine inspect` should report an `aarch64` VM; on Docker Desktop, Settings → General → "Use Virtualization framework".

## Why This Works

| Problem on CI/GitHub Actions | Solution on Mac |
|------------------------------|-----------------|
| Lambda container has GCC 7.3 | manylinux_2_28 has GCC ≥9.3 |
| Ubuntu has glibc 2.39 | manylinux_2_28 has glibc 2.28 |
| x86 runners would need QEMU for ARM (CI avoids this with native `ubuntu-24.04-arm` runners) | Mac runs ARM natively |
| NumPy wheels have 4KB page alignment | Build from source with 64KB alignment |
