import boto3
import json
import os
import pyarrow as pa
import shutil
import tempfile
import uuid

from pyarrow import dataset as ds
from datetime import datetime


MAX_ROWS_PER_FILE = 100000
MAX_ROWS_PER_GROUP = 10000  # Dataset writer will batch incoming data and only write the row groups to the disk when sufficient rows have accumulated.


def lambda_handler(files_list, context, s3_client=None, temp_dir=None, invocation_id=None):
    print(f"Processing files: {files_list}")

    if s3_client is None:
        s3_client = boto3.client("s3")

    if temp_dir is None:
        temp_dir = tempfile.gettempdir()

    if invocation_id is None:
        invocation_id = uuid.uuid4().hex[:8]

    source_bucket = os.environ["RAW_DATA_FILES_BUCKET_NAME"]
    source_files_directory = os.path.join(temp_dir, "source_files")
    generated_files_directory = os.path.join(temp_dir, "generated_files")
    directory_paths_to_upload = []

    # Download and process files one by one to avoid spike load on S3
    print(f"Downloading and processing {len(files_list)} Raw data files from s3://{source_bucket}")
    for file_key in files_list:
        job_subdirectory = os.path.basename(os.path.dirname(file_key))
        file_name = os.path.basename(file_key)
        file_path = os.path.join(source_files_directory, source_bucket, job_subdirectory, file_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        s3_client.download_file(source_bucket, file_key, file_path)

        if os.path.exists(file_path):
            with open(file_path, "r") as raw_data_file:
                data_assets = json.load(raw_data_file)
                output_directory_path = os.path.join(generated_files_directory, job_subdirectory, source_bucket)
                generated_parquet_paths = dump_to_parquet(data_assets, output_directory_path, invocation_id)
                directory_paths_to_upload.extend(generated_parquet_paths)

    # Upload parquet files when all of them are ready, to avoid partial uploads
    destination_bucket = os.environ["PARQUET_FILES_BUCKET_NAME"]
    uploaded_file_keys = []
    print(f"Uploading {len(directory_paths_to_upload)} items of 15min Parquet files to s3://{destination_bucket}")
    for directory_path in directory_paths_to_upload:
        file_key_prefix = os.path.relpath(directory_path, generated_files_directory)
        file_key_prefix = os.path.join("15min_chunks", file_key_prefix)
        s3_file_keys = upload_directory_to_s3(s3_client, directory_path, destination_bucket, file_key_prefix)
        uploaded_file_keys.extend(s3_file_keys)

    # Remove downloaded and generated files
    if os.path.exists(source_files_directory):
        shutil.rmtree(source_files_directory)
    if os.path.exists(generated_files_directory):
        shutil.rmtree(generated_files_directory)

    return uploaded_file_keys


# This function can consume 2x memory size of data_assets
def dump_to_parquet(data_assets, output_directory_path, invocation_id):
    asset_per_file_path = {}

    for data_asset in data_assets:
        # Clean data
        data_asset["dataAsset"] = data_asset["dataAsset"].strip()
        product = data_asset["dataAsset"]

        timestamp = datetime.fromisoformat(data_asset["timestamp"].replace("Z", "+00:00"))
        hour_quarter_min = (timestamp.minute // 15 + 1) * 15
        file_name = f"{timestamp.strftime('%Y-%m-%dT%H')}_{hour_quarter_min}m-{invocation_id}.parquet"

        file_path = os.path.join(output_directory_path, product, file_name)

        normalize_inplace(data_asset)

        if file_path in asset_per_file_path:
            asset_per_file_path[file_path].append(data_asset)
        else:
            asset_per_file_path[file_path] = [data_asset]

    for file_path, assets in asset_per_file_path.items():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        table = pa.Table.from_pylist(assets)
        append_dataset = ds.dataset(table)

        original_file_path = None
        if os.path.exists(file_path):
            original_file_path = file_path + ".orig"
            os.rename(file_path, original_file_path)
            original_dataset = ds.dataset(original_file_path, format="parquet")
            joined_dataset = ds.dataset([original_dataset, append_dataset])
        else:
            joined_dataset = append_dataset

        write_options = ds.ParquetFileFormat().make_write_options(compression="snappy")
        ds.write_dataset(
            joined_dataset,
            file_path,
            format="parquet",
            basename_template="part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
            file_options=write_options,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_rows_per_group=MAX_ROWS_PER_GROUP,
        )
        if original_file_path:
            shutil.rmtree(original_file_path)

    return list(asset_per_file_path.keys())


def normalize_inplace(data_asset):
    # Pull iotreadings one level up
    iotreadings = data_asset.pop("iotreadings", {})
    for key, value in iotreadings.items():
        data_asset[f"iotreadings_{key}"] = value


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
