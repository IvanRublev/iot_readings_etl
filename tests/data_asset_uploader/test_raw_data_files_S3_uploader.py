import os
import pytest
import tempfile
import uuid

from unittest.mock import MagicMock

from data_asset_uploader.raw_data_files_S3_uploader import upload_raw_data

BUCKET_NAME = os.environ["UPLOADER_RAW_DATA_BUCKET_NAME"]


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


def test_pass_upload_raw_data_given_raw_data_path_and_job_uuid_uploads_to_s3(freezer, temp_dir):
    mock_s3_client = MagicMock()
    job_uuid = uuid.uuid4()
    # Move datetime.now() to a specific date to ensure the s3 key is deterministic
    freezer.move_to("2023-04-15")
    s3_key_prefix = f"2023/04/15/job_{job_uuid}/"
    # Create 3 files in the temp directory
    for file_name in ["raw-1.json", "raw-2.json", "raw-3.json"]:
        with open(os.path.join(temp_dir, file_name), "w") as f:
            f.write(f"Content for {file_name}")

    upload_raw_data(temp_dir, job_uuid, mock_s3_client)

    assert mock_s3_client.upload_file.call_count == 3
    for file_name in ["raw-1.json", "raw-2.json", "raw-3.json"]:
        mock_s3_client.upload_file.assert_any_call(
            os.path.join(temp_dir, file_name), BUCKET_NAME, s3_key_prefix + file_name
        )


def test_pass_upload_raw_data_prints_job_uuid(capfd, temp_dir):
    mock_s3_client = MagicMock()
    job_uuid = str(uuid.uuid4())

    upload_raw_data(temp_dir, job_uuid, mock_s3_client)

    (stdout, _) = capfd.readouterr()
    assert job_uuid in stdout
