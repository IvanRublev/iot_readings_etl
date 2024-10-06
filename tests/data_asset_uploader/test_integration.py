import boto3
import datetime
import os
import pytest
import pandas as pd
import tempfile
import time
import uuid

from data_asset_uploader.raw_data_files_S3_uploader import upload_raw_data
from tests.factories import build_data_asset, dump_raw_data_file


S3_FILE_CREATION_WAIT_MILLISECONDS = 500
AWS_ACCESS_KEY_ID = os.environ["DOWNLOADER_AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["DOWNLOADER_AWS_SECRET_ACCESS_KEY"]
PARQUET_BUCKET = os.environ["DOWNLOADER_DAILY_PARQUET_BUCKET_NAME"]
RAW_DATA_BUCKET_NAME = os.environ["UPLOADER_RAW_DATA_BUCKET_NAME"]


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.mark.integration
# This test can be terminated by a pytest-timeout, see timeout setting in pyproject.toml
def test_pass_upload_raw_data_given_one_data_asset_then_persisted_in_daily_parquet_file(temp_dir):
    product = "mars"
    date = datetime.date(2024, 9, 30)
    data_asset = build_data_asset(dataAsset=product, timestamp=date.isoformat(), iotreadings={"value1": 752})
    dump_raw_data_file([data_asset], os.path.join(temp_dir, "raw_data.json"))

    job_uuid = uuid.uuid4()
    upload_raw_data(temp_dir, job_uuid)

    print("Upload complete.")
    parquet_path = wait_download_parquet_file(job_uuid, product, date, temp_dir)
    df = pd.read_parquet(parquet_path)
    print("\nDaily parquet file content:")
    print(df)
    assert df.iloc[0]["iotreadings_value1"] == 752


## Helpers


def wait_download_parquet_file(job_uuid, product, date, temp_dir):
    s3_path = f"job_{job_uuid}/{RAW_DATA_BUCKET_NAME}/{product}/{date.strftime('%Y/%m/%d')}/{date.strftime('%Y-%m-%d')}.{job_uuid}.snappy.parquet/part-0.parquet"
    print(f"Waiting for s3://{PARQUET_BUCKET}/{s3_path} to be created")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    while True:
        try:
            s3_client.head_object(Bucket=PARQUET_BUCKET, Key=s3_path)
            # File exists, download it
            local_path = os.path.join(temp_dir, f"{date.strftime('%Y-%m-%d')}.{job_uuid}.snappy.parquet/part-0.parquet")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3_client.download_file(PARQUET_BUCKET, s3_path, local_path)
            return local_path
        except s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise e
            # File not found, wait for it to be created
        time.sleep(S3_FILE_CREATION_WAIT_MILLISECONDS / 1000)
        print(".", end="", flush=True)
