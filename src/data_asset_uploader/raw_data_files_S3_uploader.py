import argparse
import boto3
import uuid
import os

from datetime import datetime

RAW_DATA_DIRECTORY_PATH = "data/"


def aws_config():
    try:
        return {
            "aws_access_key_id": os.environ["UPLOADER_AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["UPLOADER_AWS_SECRET_ACCESS_KEY"],
            "bucket_name": os.environ["UPLOADER_RAW_DATA_BUCKET_NAME"],
        }
    except KeyError as e:
        raise KeyError(f"Missing environment variable: {e}")


def upload_raw_data(raw_data_directory_path, job_uuid, s3_client=None):
    print("Uploading raw data files.")
    print(f"job:        {job_uuid}")
    print(f"directory:  {raw_data_directory_path}\n")

    config = aws_config()
    if s3_client is None:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
        )

    bucket = config["bucket_name"]
    for file_name in os.listdir(raw_data_directory_path):
        file_path = os.path.join(raw_data_directory_path, file_name)
        s3_key = f"{datetime.now().strftime('%Y/%m/%d')}/job_{job_uuid}/{file_name}"
        print(f"Uploading {file_name} to s3://{bucket}/{s3_key}.")
        s3_client.upload_file(file_path, bucket, s3_key)


def main():
    parser = argparse.ArgumentParser(
        description="Upload raw data files to be processed in Medallion Lakehouse ETL pipeline."
    )
    parser.add_argument(
        "raw_data_dir",
        type=str,
        nargs="?",
        default=RAW_DATA_DIRECTORY_PATH,
        help="Directory containing raw data files in JSON format (default: /data)",
    )
    parser.add_argument("--job_uuid", type=str, default=None, help="Job UUID (default: randomly generated)")

    args = parser.parse_args()

    raw_data_dir = args.raw_data_dir
    job_uuid = args.job_uuid or str(uuid.uuid4())

    # Ensure the raw_data_dir exists
    if not os.path.isdir(raw_data_dir):
        print(f"Error: The specified raw data files directory '{raw_data_dir}' does not exist.")
        return

    upload_raw_data(raw_data_dir, job_uuid)


if __name__ == "__main__":
    main()
