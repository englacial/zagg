# ARM64 Lambda Layer

Building an ARM64 (Graviton2) Lambda layer on Apple Silicon Mac.

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
| Docker Desktop | Latest (or OrbStack, Colima) |
| Disk space | ~5 GB free |

### Container Image

Use `quay.io/pypa/manylinux_2_28_aarch64`:

- glibc 2.28 (compatible with Lambda's 2.34)
- Modern GCC toolchain (≥9.3, needed for numpy)
- Pre-configured for building Python wheels

## Build Steps

1. **Install Docker Desktop** (if not already installed):

    ```bash
    brew install --cask docker
    ```

2. **Run the build**:

    ```bash
    bash deployment/aws/build_arm64_layer.sh
    ```

3. **Transfer the zip** (if building on a different machine):

    ```bash
    scp deployment/layers/lambda_layer_arm64.zip user@remote:/path/
    ```

## Deploying the Layer

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

```bash
docker run --rm --platform linux/arm64 \
    -v ./deployment/layers/lambda_layer_arm64.zip:/layer.zip \
    public.ecr.aws/lambda/python:3.12 \
    bash -c '
        unzip -q /layer.zip -d /opt
        python3.12 -c "
import sys
sys.path.insert(0, \"/opt/python\")
import numpy; print(f\"numpy {numpy.__version__}\")
import pandas; print(f\"pandas {pandas.__version__}\")
import healpy; print(f\"healpy {healpy.__version__}\")
import zarr; print(f\"zarr {zarr.__version__}\")
import pydantic_zarr; print(\"pydantic_zarr OK\")
import obstore; print(\"obstore OK\")
import h5coro; print(\"h5coro OK\")
import earthaccess; print(\"earthaccess OK\")
print(\"All imports successful!\")
"
    '
```

## Troubleshooting

!!! danger "ELF load command address/offset not properly aligned"
    NumPy wasn't built with 64KB page alignment. Lambda ARM64 requires page alignment of 64KB (0x10000), but pre-built wheels use 4KB. The build script handles this with `LDFLAGS="-Wl,-z,max-page-size=0x10000"` and `--no-binary numpy`.

!!! warning "healpy build fails"
    Ensure you're using `manylinux_2_28_aarch64` (has GCC ≥9.3). The Lambda container's GCC is too old.

!!! warning "GLIBC_2.XX not found"
    Your build container has a newer glibc than Lambda. Use `manylinux_2_28` (glibc 2.28) which is compatible with Lambda's glibc 2.34.

!!! info "Slow build"
    Ensure Docker Desktop is configured for native ARM execution (not Rosetta emulation). Check: Docker Desktop → Settings → General → "Use Virtualization framework".

## Why This Works

| Problem on CI/GitHub Actions | Solution on Mac |
|------------------------------|-----------------|
| Lambda container has GCC 7.3 | manylinux_2_28 has GCC ≥9.3 |
| Ubuntu has glibc 2.39 | manylinux_2_28 has glibc 2.28 |
| x86 runners need QEMU for ARM | Mac runs ARM natively |
| healpy has no ARM wheels | Build from source with proper toolchain |
| NumPy wheels have 4KB page alignment | Build from source with 64KB alignment |
