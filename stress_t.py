import argparse
import boto3
from datetime import datetime
from faker import Faker
import os
import random
import shutil
import tempfile
import time
from tqdm import tqdm
import uuid

from tests.factories import build_data_asset, dump_raw_data_file

UPLOADER_SQS_QUEUE_URL = (
    "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/medallion-lakehouse-raw-sqs-queue"
)
UPLOADER_RAW_DATA_BUCKET_NAME = "medallion-lakehouse-s3bronze"


def generate_stress_test_data(raw_data_directory, files_count, data_assets_count, days_count, products_count):
    # Remove all files in raw_data_directory if there are any
    for filename in os.listdir(raw_data_directory):
        file_path = os.path.join(raw_data_directory, filename)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    # generate files
    print("Generating files", end="", flush=True)
    data_assets = []
    faker = Faker()
    products = [faker.word() for _ in range(products_count)]
    for _ in range(data_assets_count):
        timestamp = faker.date_time_between(start_date=f"-{days_count}d", end_date="-1d").isoformat()
        dataAsset = random.choice(products)
        data_asset = build_data_asset(timestamp=timestamp, dataAsset=dataAsset)
        data_assets.append(data_asset)
        print(".", end="", flush=True)

    file_path = os.path.join(raw_data_directory, "raw_data_1.json")
    dump_raw_data_file(data_assets, file_path)
    file_size = os.path.getsize(file_path)
    # copy file to get requested count
    for file_num in range(2, files_count + 1):
        shutil.copy(file_path, os.path.join(raw_data_directory, f"raw_data_{file_num}.json"))

    print("")
    print(f"Generated {files_count} files with {data_assets_count} records each.")
    print(f"Raw data files are stored in '{raw_data_directory}' directory.")
    file_size_kb = round(file_size / 1024)
    print(f"File size: {file_size_kb}KB")

    # Process files in etl pipeline
    print("Uploading files to ETL pipeline")
    start_time = time.time()
    upload_raw_data(raw_data_directory)
    print(f"Time taken to upload {files_count} files: {time.time() - start_time:.2f} seconds")

    print("Wait for all raw data files to be processed in SQS queue:")
    while True:
        time.sleep(5)
        msg_count = total_number_of_messages_in_sqs_queue()
        if msg_count == 0:
            break
        print(f" {msg_count}", end="", flush=True)
    print("")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds")


def upload_raw_data(raw_data_directory_path):
    job_uuid = str(uuid.uuid4())
    print(f"\nJob UUID: {job_uuid}\n")

    s3_client = boto3.client("s3")

    bucket = UPLOADER_RAW_DATA_BUCKET_NAME

    file_list = os.listdir(raw_data_directory_path)
    for file_name in tqdm(file_list, desc="Uploading files", unit="file"):
        file_path = os.path.join(raw_data_directory_path, file_name)
        s3_key = f"{datetime.now().strftime('%Y/%m/%d')}/job_{job_uuid}/{file_name}"
        s3_client.upload_file(file_path, bucket, s3_key)


def total_number_of_messages_in_sqs_queue():
    sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_attributes(
        QueueUrl=UPLOADER_SQS_QUEUE_URL,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )
    attrs = response["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]) + int(attrs["ApproximateNumberOfMessagesNotVisible"])


def main():
    parser = argparse.ArgumentParser(description="Generate stress test data and upload them to ETL pipeline.")
    parser.add_argument(
        "--raw-data-directory",
        type=str,
        help="Directory to store raw data files (optional), if not specified, a temporary directory will be used",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--files", type=int, help="Number of files to process in total")
    group.add_argument(
        "--warmup",
        type=int,
        nargs=2,
        metavar=("STEP", "MAX"),
        help="Process files with a given step until the total amount is reached (e.g. process in steps of 5 up to 20 files, --warmup 5 20)",
    )

    parser.add_argument("--assets-per-file", type=int, required=True, help="Number of data assets per file")
    parser.add_argument("--days-count", type=int, required=True, help="Number of days to generate data for")
    parser.add_argument("--products-count", type=int, required=True, help="Number of products to generate data for")

    args = parser.parse_args()

    if args.warmup:
        if args.warmup[1] % args.warmup[0] != 0:
            raise ValueError("MAX must be divisible by STEP")

        for i in range(args.warmup[0], args.warmup[1] + 1, args.warmup[0]):
            files_count = i
            print(
                f"\n{i // args.warmup[0]}/{args.warmup[1] // args.warmup[0]} Processing {files_count} files for warmup..."
            )
            if args.raw_data_directory:
                generate_stress_test_data(
                    args.raw_data_directory, files_count, args.assets_per_file, args.days_count, args.products_count
                )
            else:
                with tempfile.TemporaryDirectory() as tmpdirname:
                    generate_stress_test_data(tmpdirname, args.files, args.assets_per_file, args.days_count)
            # wait to be sure that containers are ready for the next batch
            time.sleep(5)
    else:
        # one pass
        if args.raw_data_directory:
            generate_stress_test_data(
                args.raw_data_directory, args.files, args.assets_per_file, args.days_count, args.products_count
            )
        else:
            with tempfile.TemporaryDirectory() as tmpdirname:
                generate_stress_test_data(tmpdirname, args.files, args.assets_per_file, args.days_count)


if __name__ == "__main__":
    main()
