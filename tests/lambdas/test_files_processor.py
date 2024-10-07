import os
import pandas as pd
import pytest
import tempfile
import uuid

from tests.factories import build_data_asset, dump_raw_data_file
from unittest.mock import MagicMock, patch

from lambda_processing.files_processor import lambda_handler, dump_to_parquet, normalize_inplace

RAW_DATA_FILES_BUCKET_NAME = "s3bronze-bucket"
PARQUET_FILES_BUCKET_NAME = "s3silver-bucket"


@pytest.fixture(autouse=True)
def mock_env_variables():
    variables = {
        "RAW_DATA_FILES_BUCKET_NAME": RAW_DATA_FILES_BUCKET_NAME,
        "PARQUET_FILES_BUCKET_NAME": PARQUET_FILES_BUCKET_NAME,
    }
    with patch.dict("os.environ", variables):
        yield


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


# Lambda handler tests


def test_pass_lambda_handler_given_no_files_returns_empty_list():
    assert lambda_handler([], {}) == []


def test_pass_lambda_handler_given_file_list_downloads_them_from_s3(temp_dir):
    job_subdirectory = "job_842d6e1c-0630-4af8-a3e1-8d18a24ce805"
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file_list = [
        f"2024/10/03/{job_subdirectory}/raw-2.json",
        f"2024/10/03/{job_subdirectory}/raw-1.json",
    ]

    lambda_handler(file_list, {}, mock_s3_client, temp_dir)

    assert mock_s3_client.download_file.call_count == 2
    for file_key in file_list:
        file_name = os.path.basename(file_key)
        target_file_path = os.path.join(
            temp_dir, "source_files", RAW_DATA_FILES_BUCKET_NAME, job_subdirectory, file_name
        )
        mock_s3_client.download_file.assert_any_call(RAW_DATA_FILES_BUCKET_NAME, file_key, target_file_path)


def test_pass_lambda_handler_given_file_list_uploads_appropriate_parquet_directories_to_s3(temp_dir):
    # Create a Raw data file that the lambda handler would have downloaded
    data_asset = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z")
    job_subdirectory = "job_842d6e1c-0630-4af8-a3e1-8d18a24ce805"
    file_path = os.path.join(temp_dir, "source_files", RAW_DATA_FILES_BUCKET_NAME, job_subdirectory, "raw-1.json")
    dump_raw_data_file([data_asset], file_path)

    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    invocation_id = "3FDE7B3B"

    uploaded_file_keys = lambda_handler(
        [f"2024/10/03/{job_subdirectory}/raw-1.json"],
        {},
        mock_s3_client,
        temp_dir,
        invocation_id,
    )

    assert mock_s3_client.upload_file.call_count == 1
    file_path = os.path.join(
        temp_dir,
        "generated_files",
        job_subdirectory,
        RAW_DATA_FILES_BUCKET_NAME,
        "mars",
        f"2024-09-30T13_45m-{invocation_id}.parquet",
        "part-0.parquet",
    )
    file_key = os.path.join(
        "15min_chunks",
        job_subdirectory,
        RAW_DATA_FILES_BUCKET_NAME,
        "mars",
        f"2024-09-30T13_45m-{invocation_id}.parquet",
        "part-0.parquet",
    )
    mock_s3_client.upload_file.assert_any_call(file_path, PARQUET_FILES_BUCKET_NAME, file_key)
    assert uploaded_file_keys == [file_key]


def test_pass_lambda_handler_given_duplicate_files_aggregates_them_into_single_15min_parquet(temp_dir):
    # Create a Raw data file that the lambda handler would have downloaded
    data_asset = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z")
    job_subdirectory = "job_842d6e1c-0630-4af8-a3e1-8d18a24ce805"
    file_path = os.path.join(temp_dir, "source_files", RAW_DATA_FILES_BUCKET_NAME, job_subdirectory, "raw-1.json")
    dump_raw_data_file([data_asset], file_path)

    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    invocation_id = "3FDE7B3B"

    uploaded_file_keys = lambda_handler(
        [f"2024/10/03/{job_subdirectory}/raw-1.json"] * 3,
        {},
        mock_s3_client,
        temp_dir,
        invocation_id,
    )

    assert mock_s3_client.upload_file.call_count == 1
    file_path = os.path.join(
        temp_dir,
        "generated_files",
        job_subdirectory,
        RAW_DATA_FILES_BUCKET_NAME,
        "mars",
        f"2024-09-30T13_45m-{invocation_id}.parquet",
        "part-0.parquet",
    )
    file_key = os.path.join(
        "15min_chunks",
        job_subdirectory,
        RAW_DATA_FILES_BUCKET_NAME,
        "mars",
        f"2024-09-30T13_45m-{invocation_id}.parquet",
        "part-0.parquet",
    )
    mock_s3_client.upload_file.assert_any_call(file_path, PARQUET_FILES_BUCKET_NAME, file_key)
    assert uploaded_file_keys == [file_key]


def test_pass_lambda_handler_given_file_list_removes_downloaded_and_generated_files_finally(temp_dir):
    # Create a Raw data file that the lambda handler would have downloaded
    data_asset = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z")
    job_subdirectory = "job_842d6e1c-0630-4af8-a3e1-8d18a24ce805"
    file_path = os.path.join(temp_dir, "source_files", RAW_DATA_FILES_BUCKET_NAME, job_subdirectory, "raw-1.json")
    dump_raw_data_file([data_asset], file_path)

    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    invocation_id = "3FDE7B3B"

    lambda_handler([f"2024/10/03/{job_subdirectory}/raw-1.json"], {}, mock_s3_client, temp_dir, invocation_id)

    assert not os.path.exists(os.path.join(temp_dir, "source_files"))
    assert not os.path.exists(os.path.join(temp_dir, "generated_files"))


# Dump to parquet tests


def test_pass_dump_to_parquet_given_altertnative_timestamps_writes_events_to_separate_15min_parquet_files(temp_dir):
    output_path = os.path.join(temp_dir, str(uuid.uuid4()))
    data_asset_timestamp_1 = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z")
    data_asset_timestamp_2 = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:45:01.000Z")

    dump_to_parquet([data_asset_timestamp_1, data_asset_timestamp_2], output_path, "3FDE7B3B")

    for file_name in ["2024-09-30T13_45m-3FDE7B3B.parquet", "2024-09-30T13_60m-3FDE7B3B.parquet"]:
        expected_file_path = os.path.join(output_path, f"mars/{file_name}")
        assert os.path.exists(expected_file_path), f"File does not exist: {expected_file_path}"


def test_pass_dump_to_parquet_given_two_products_data_writes_events_to_separate_directories(temp_dir):
    output_path = os.path.join(temp_dir, str(uuid.uuid4()))

    data_asset_1 = build_data_asset(dataAsset="mars")
    data_asset_2 = build_data_asset(dataAsset="pluto")

    dump_to_parquet([data_asset_1, data_asset_2], output_path, "5F5E7A8B")

    for product in ["mars", "pluto"]:
        expected_dir_path = os.path.join(output_path, product)
        assert os.path.exists(expected_dir_path), f"Directory does not exist: {expected_dir_path}"


def test_pass_dump_to_parquet_given_product_name_with_space_padding_trims_spaces(temp_dir):
    output_path = os.path.join(temp_dir, str(uuid.uuid4()))
    data_asset_1 = build_data_asset(dataAsset="   mars ", timestamp="2024-09-30T13:44:01.000Z")

    dump_to_parquet([data_asset_1], output_path, "5F5E7A8B")

    file_path = os.path.join(output_path, "mars/2024-09-30T13_45m-5F5E7A8B.parquet")
    read_df = pd.read_parquet(file_path)
    assert read_df.iloc[0]["dataAsset"] == "mars"


def test_pass_dump_to_parqeut_given_existing_parquet_and_new_data_asset_keys_extends_parquet_file(temp_dir):
    output_path = os.path.join(temp_dir, str(uuid.uuid4()))
    data_asset_1 = build_data_asset(
        dataAsset="mars", timestamp="2024-09-30T13:40:01.000Z", iotreadings={"value1": 1, "value2": 4}
    )
    data_asset_2 = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:43:00.000Z", iotreadings={"value9": 7})
    data_asset_3 = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:00.000Z", iotreadings={"value1": 2})

    dump_to_parquet([data_asset_1], output_path, "5F5E7A8B")
    dump_to_parquet([data_asset_2], output_path, "5F5E7A8B")
    dump_to_parquet([data_asset_3], output_path, "5F5E7A8B")

    file_path = os.path.join(output_path, "mars/2024-09-30T13_45m-5F5E7A8B.parquet")
    read_df = pd.read_parquet(file_path)
    # order of rows is not guaranteed
    row1 = read_df[read_df["timestamp"] == "2024-09-30T13:40:01.000Z"].iloc[0]
    assert row1["iotreadings_value1"] == 1
    assert row1["iotreadings_value2"] == 4
    assert pd.isna(row1["iotreadings_value9"])

    row2 = read_df[read_df["timestamp"] == "2024-09-30T13:43:00.000Z"].iloc[0]
    assert pd.isna(row2["iotreadings_value1"])
    assert pd.isna(row2["iotreadings_value2"])
    assert row2["iotreadings_value9"] == 7

    row3 = read_df[read_df["timestamp"] == "2024-09-30T13:44:00.000Z"].iloc[0]
    assert row3["iotreadings_value1"] == 2
    assert pd.isna(row3["iotreadings_value2"])
    assert pd.isna(row3["iotreadings_value9"])


# Normalize data asset tests


def test_pass_normalize_data_asset_given_data_asset_without_iotreadings_returns_same_dict():
    data_asset = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z")
    data_asset.pop("iotreadings", None)
    data_asset_copy = data_asset.copy()

    normalize_inplace(data_asset)

    assert data_asset == data_asset_copy


def test_pass_normalize_data_asset_given_data_asset_with_iotreadings_returns_flattened_dict():
    data_asset = build_data_asset(dataAsset="mars", timestamp="2024-09-30T13:44:01.000Z", iotreadings={"value1": 1})

    normalize_inplace(data_asset)

    assert data_asset["iotreadings_value1"] == 1
