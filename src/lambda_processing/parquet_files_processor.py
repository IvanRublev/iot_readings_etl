import boto3
import os
import shutil
import tempfile
from typing import Tuple

from datetime import datetime
from pyarrow import dataset as ds


MAX_ROWS_PER_GROUP = 10000  # Dataset writer will batch incoming data and only write the row groups to the disk when sufficient rows have accumulated.


def lambda_handler(chunked_parquet_files, context, s3_client=None, temp_dir=None, cleanup_on_finish=True):
    print(f"Processing chunked parquet files: {chunked_parquet_files}")

    if s3_client is None:
        s3_client = boto3.client("s3")

    if temp_dir is None:
        temp_dir = tempfile.gettempdir()

    bucket_name = os.environ["PARQUET_FILES_BUCKET_NAME"]

    source_keys_list = sum(chunked_parquet_files, [])
    # jbpd -> job_id, bucket, product, day
    source_key_by_jbpd = {}
    for source_key in source_keys_list:
        jbpd_parts = chunked_parquet_key_parts(source_key)
        if jbpd_parts in source_key_by_jbpd:
            source_key_by_jbpd[jbpd_parts].append(source_key)
        else:
            source_key_by_jbpd[jbpd_parts] = [source_key]

    # Let's download and assemble daily Parquet files day by day to reduce a spike load on S3
    source_files_path = os.path.join(temp_dir, "source_files")
    daily_path = os.path.join(temp_dir, "daily_files")
    uploaded_file_keys = []

    for jbpd_parts, source_keys in source_key_by_jbpd.items():
        print(f"Downloading {len(source_keys)} Parquet files from s3://{bucket_name}")
        downloaded_files = []
        for key in source_keys:
            target_file_path = os.path.join(source_files_path, key)
            os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
            s3_client.download_file(bucket_name, key, target_file_path)
            downloaded_files.append(target_file_path)

        joined_dataset = ds.dataset(downloaded_files, format="parquet")

        job_id, bucket, product, day = jbpd_parts
        datetime_obj = datetime.strptime(day, "%Y-%m-%d")
        target_key = f"job_{job_id}/{bucket}/{product}/{datetime.strftime(datetime_obj, '%Y/%m/%d')}/{datetime.strftime(datetime_obj, '%Y-%m-%d')}.{job_id}.snappy.parquet"
        daily_parquet_path = os.path.join(daily_path, target_key)
        os.makedirs(os.path.dirname(daily_parquet_path), exist_ok=True)

        print(f"Writing {daily_parquet_path}")
        write_options = ds.ParquetFileFormat().make_write_options(compression="snappy")
        ds.write_dataset(
            joined_dataset,
            daily_parquet_path,
            format="parquet",
            basename_template="part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
            file_options=write_options,
            max_rows_per_group=MAX_ROWS_PER_GROUP,
        )

        print(f"Uploading {os.path.basename(daily_parquet_path)} to s3://{bucket_name}")
        keys = upload_directory_to_s3(s3_client, daily_parquet_path, bucket_name, target_key)
        uploaded_file_keys.extend(keys)

    # Remove source and generated daily files
    if cleanup_on_finish:
        if os.path.exists(source_files_path):
            shutil.rmtree(source_files_path)
        if os.path.exists(daily_path):
            shutil.rmtree(daily_path)

    return uploaded_file_keys


def chunked_parquet_key_parts(key: str) -> Tuple[str, str, str, str]:
    # "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    job, bucket, product, file_name = key.split("/")[1:5]
    job_id = job.split("_")[1]
    day = file_name.split("T")[0]
    return job_id, bucket, product, day


def upload_directory_to_s3(s3_client, local_directory, bucket, file_key_prefix):
    uploaded_file_keys = []
    for root, _dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(file_key_prefix, relative_path)
            s3_client.upload_file(local_path, bucket, s3_path)
            uploaded_file_keys.append(s3_path)

    return uploaded_file_keys
