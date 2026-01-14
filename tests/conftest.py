import json
import time
import uuid

import numpy as np
import pandas as pd
import pytest

from magg.schema import DATA_VARS


@pytest.fixture(scope="session")
def minio_container():
    """Start MinIO container for S3 mocking."""
    import docker

    client = docker.from_env()
    port = 9000
    container = client.containers.run(
        "quay.io/minio/minio",
        "server /data",
        detach=True,
        ports={f"{port}/tcp": port},
        environment={
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
        },
    )
    time.sleep(3)  # give it time to boot
    yield {
        "port": port,
        "endpoint": f"http://localhost:{port}",
        "username": "minioadmin",
        "password": "minioadmin",
    }
    container.stop()
    container.remove()


@pytest.fixture(scope="function")
def minio_bucket(minio_container):
    """Create a fresh bucket for each test."""
    from minio import Minio

    bucket = f"test-{uuid.uuid4().hex[:8]}"

    client = Minio(
        f"localhost:{minio_container['port']}",
        access_key=minio_container["username"],
        secret_key=minio_container["password"],
        secure=False,
    )
    client.make_bucket(bucket)

    # Set public read policy for testing
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": f"arn:aws:s3:::{bucket}/*",
            },
        ],
    }
    client.set_bucket_policy(bucket, json.dumps(policy))

    yield {
        "port": minio_container["port"],
        "endpoint": minio_container["endpoint"],
        "username": minio_container["username"],
        "password": minio_container["password"],
        "bucket": bucket,
        "client": client,
    }

    # Cleanup: remove all objects and bucket
    for obj in client.list_objects(bucket, recursive=True):
        client.remove_object(bucket, obj.object_name)
    client.remove_bucket(bucket)


@pytest.fixture
def s3_store_factory(minio_bucket):
    """Factory to create S3Store instances pointing to MinIO."""
    from obstore.store import S3Store
    from zarr.storage import ObjectStore

    def _create(prefix: str = "test.zarr"):
        s3_store = S3Store(
            minio_bucket["bucket"],
            prefix=prefix,
            aws_endpoint=minio_bucket["endpoint"],
            access_key_id=minio_bucket["username"],
            secret_access_key=minio_bucket["password"],
            virtual_hosted_style_request=False,
            client_options={"allow_http": True},
        )
        return ObjectStore(store=s3_store, read_only=False)

    return _create


@pytest.fixture
def mock_dataframe_factory():
    """Factory to create mock DataFrames matching process_morton_cell output."""
    from mortie import generate_morton_children, geo2mort, mort2healpix

    def _create(lat: float, lon: float, parent_order: int, child_order: int) -> pd.DataFrame:
        parent_morton = geo2mort(lat, lon, order=parent_order)

        children = generate_morton_children(parent_morton[0], child_order)
        cell_ids, _ = mort2healpix(children)
        n = len(children)

        df = pd.DataFrame({"morton": children, "cell_ids": cell_ids}).assign(
            **{var: np.random.randn(n).astype(np.float32) for var in DATA_VARS if var != "count"}
        )
        df = df.assign(count=np.random.randn(n).astype(np.int32))
        return df

    return _create
