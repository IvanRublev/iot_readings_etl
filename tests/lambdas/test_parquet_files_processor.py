import pytest
import tempfile
import os
import pandas as pd

from tests.factories import build_parquet_dataframe, dump_parquet_file
from unittest.mock import MagicMock, patch

from lambda_processing.parquet_files_processor import lambda_handler, chunked_parquet_key_parts

PARQUET_FILES_BUCKET_NAME = "s3silver-bucket"


@pytest.fixture(autouse=True)
def mock_env_variables():
    variables = {"PARQUET_FILES_BUCKET_NAME": PARQUET_FILES_BUCKET_NAME}
    with patch.dict("os.environ", variables):
        yield


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


# Lambda handler tests


def test_pass_lambda_handler_given_no_files_returns_empty_list():
    assert lambda_handler([], {}) == []


def test_pass_lambda_handler_given_cunked_parquet_files_downloads_them_from_s3(temp_dir):
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file1 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/jupiter/2024-01-01T13_30m-90147479.parquet/part-0.parquet"
    file2 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    file3 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_45m-90147479.parquet/part-0.parquet"
    # Put files in place as they would be downloaded
    dump_source_files(
        temp_dir,
        [(file1, build_parquet_dataframe()), (file2, build_parquet_dataframe()), (file3, build_parquet_dataframe())],
    )

    file_list = [
        [file1, file2],
        [file3],
    ]

    lambda_handler(file_list, {}, mock_s3_client, temp_dir)

    assert mock_s3_client.download_file.call_count == 3
    for file_key in [file1, file2, file3]:
        target_file_path = os.path.join(temp_dir, "source_files", file_key)
        mock_s3_client.download_file.assert_any_call(PARQUET_FILES_BUCKET_NAME, file_key, target_file_path)


def test_pass_lambda_handler_given_chunked_parquet_files_assembles_them_into_daily_parquet(temp_dir):
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file1 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    file2 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_45m-90147479.parquet/part-0.parquet"
    file_list = [[file1], [file2]]
    # Put files in place as they would be downloaded
    dump_source_files(
        temp_dir,
        [
            (file1, build_parquet_dataframe(iotreadings_count=1)),
            (file2, build_parquet_dataframe(iotreadings_count=2)),
        ],
    )

    lambda_handler(file_list, {}, mock_s3_client, temp_dir, cleanup_on_finish=False)

    daily_file_path = os.path.join(
        temp_dir,
        "daily_files/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023/04/01/2023-04-01.41780824-ac46-4b25-9547-a53607b4f37a.snappy.parquet",
    )
    assert os.path.exists(daily_file_path)
    assert os.path.isdir(daily_file_path)

    df = pd.read_parquet(daily_file_path)
    assert df.shape[0] == 2
    assert all(col in df.columns for col in ["timestamp", "dataAsset", "iotreadings_value1", "iotreadings_value2"])


def test_pass_lambda_handler_given_chunked_parquet_files_assembles_them_by_job_product_day(temp_dir):
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file1 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    file2 = "15min_chunks/job_d263ab3a-d452-4c10-9c80-80d554307d9f/medallion-lakehouse-s3bronze/jupiter/2024-09-30T12_15m-90147479.parquet/part-0.parquet"
    file_list = [[file1], [file2]]
    # Put files in place as they would be downloaded
    dump_source_files(
        temp_dir,
        [
            (file1, build_parquet_dataframe()),
            (file2, build_parquet_dataframe()),
        ],
    )

    lambda_handler(file_list, {}, mock_s3_client, temp_dir, cleanup_on_finish=False)

    file_key1 = "daily_files/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023/04/01/2023-04-01.41780824-ac46-4b25-9547-a53607b4f37a.snappy.parquet"
    expected_daily_file1 = os.path.join(temp_dir, file_key1)
    file_key2 = "daily_files/job_d263ab3a-d452-4c10-9c80-80d554307d9f/medallion-lakehouse-s3bronze/jupiter/2024/09/30/2024-09-30.d263ab3a-d452-4c10-9c80-80d554307d9f.snappy.parquet"
    expected_daily_file2 = os.path.join(temp_dir, file_key2)
    assert os.path.exists(expected_daily_file1)
    assert os.path.exists(expected_daily_file2)


def test_pass_lambda_handler_given_chunked_parquet_files_uploads_assembled_daily_parquet_to_s3(temp_dir):
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file1 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    file2 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_45m-90147479.parquet/part-0.parquet"
    file_list = [[file1], [file2]]
    # Put files in place as they would be downloaded
    dump_source_files(
        temp_dir,
        [
            (file1, build_parquet_dataframe(iotreadings_value1=100)),
            (file2, build_parquet_dataframe(iotreadings_value2=47)),
        ],
    )

    uploaded_files = lambda_handler(file_list, {}, mock_s3_client, temp_dir)

    daily_file_path = os.path.join(
        temp_dir,
        "daily_files/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023/04/01/2023-04-01.41780824-ac46-4b25-9547-a53607b4f37a.snappy.parquet/part-0.parquet",
    )
    file_key = "job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023/04/01/2023-04-01.41780824-ac46-4b25-9547-a53607b4f37a.snappy.parquet/part-0.parquet"
    mock_s3_client.upload_file.assert_called_once_with(daily_file_path, PARQUET_FILES_BUCKET_NAME, file_key)
    assert uploaded_files == [file_key]


def test_pass_lambda_handler_given_chunked_parquet_files_removes_source_and_daily_files(temp_dir):
    mock_s3_client = MagicMock()
    mock_s3_client.download_file.return_value = None
    file1 = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    file_list = [[file1]]
    # Put files in place as they would be downloaded
    dump_source_files(temp_dir, [(file1, build_parquet_dataframe())])

    lambda_handler(file_list, {}, mock_s3_client, temp_dir)

    assert not os.path.exists(os.path.join(temp_dir, "source_files"))
    assert not os.path.exists(os.path.join(temp_dir, "daily_files"))


# Chunked parquet key parts tests


def test_pass_chunked_parquet_key_parts_given_parquet_file_key_returns_job_bucket_product_day():
    key = "15min_chunks/job_41780824-ac46-4b25-9547-a53607b4f37a/medallion-lakehouse-s3bronze/mars/2023-04-01T13_30m-90147479.parquet/part-0.parquet"
    assert chunked_parquet_key_parts(key) == (
        "41780824-ac46-4b25-9547-a53607b4f37a",
        "medallion-lakehouse-s3bronze",
        "mars",
        "2023-04-01",
    )


# Helper


def dump_source_files(temp_dir, file_dataframe_pairs):
    source_files_path = os.path.join(temp_dir, "source_files")
    for file_path, df in file_dataframe_pairs:
        parquet_path = os.path.join(source_files_path, file_path)
        dump_parquet_file(df, parquet_path)
