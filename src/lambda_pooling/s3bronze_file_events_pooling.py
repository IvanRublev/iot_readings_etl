import boto3
import json
import os
import time


def lambda_handler(trigger_event, context, sqs=None, stepfunctions=None):
    file_keys_list = files_from_trigger_event(trigger_event)
    if file_keys_list == []:
        return None

    print(f"Trigger event has following file keys: {file_keys_list}")

    files_per_processor = int(os.environ["RAW_DATA_FILES_PER_PROCESSOR"])
    file_processors_count = int(os.environ["FILE_PROCESSORS_COUNT"])
    total_files_count = file_processors_count * files_per_processor
    max_batching_window_in_seconds = float(os.environ["MAXIMUM_BATCHING_WINDOW_IN_SECONDS"])

    if sqs is None:
        sqs = boto3.client("sqs")

    from_sqs_count = total_files_count - len(file_keys_list)
    print(
        f"Starting to pool {from_sqs_count} file keys from SQS, {len(file_keys_list)} file keys came from trigger event."
    )
    sqs_file_keys, sqs_messages_ids_receipts = pool_file_keys(from_sqs_count, sqs, max_batching_window_in_seconds)
    print(f"Pooled {len(sqs_file_keys)} file keys from SQS.")
    file_keys_list.extend(sqs_file_keys)
    file_keys_list = list(dict.fromkeys(file_keys_list))

    files_list_chunks = [
        file_keys_list[i : i + files_per_processor] for i in range(0, len(file_keys_list), files_per_processor)
    ]
    files_list_json = json.dumps(files_list_chunks)

    if stepfunctions is None:
        stepfunctions = boto3.client("stepfunctions")

    state_machine_arn = os.environ["DATA_PROCESSING_STATE_MACHINE_ARN"]
    print(f"Starting step function execution: {state_machine_arn} with {len(file_keys_list)} unique Raw data files.")
    response = stepfunctions.start_sync_execution(stateMachineArn=state_machine_arn, input=files_list_json)
    print(f"Step function execution completed with status: {response['status']}")

    if response["status"] == "SUCCEEDED":
        sqs_messages_count = len(sqs_messages_ids_receipts)
        if sqs_messages_count > 0:
            print(f"Deleteing {len(sqs_messages_ids_receipts)} SQS messages.")
            # Delete SQS messages in batches of 10
            for i in range(0, sqs_messages_count, 10):
                batch = sqs_messages_ids_receipts[i : i + 10]
                sqs.delete_message_batch(
                    QueueUrl=os.environ["RAW_DATA_FILES_SQS_QUEUE_URL"],
                    Entries=[
                        {"Id": message_id, "ReceiptHandle": receipt_handle} for message_id, receipt_handle in batch
                    ],
                )
            print(f"Deleted {sqs_messages_count} SQS messages in batches.")
    else:
        error = {"status": response["status"], "error": response["error"], "cause": response["cause"]}
        raise RuntimeError(
            f"Step Function: {state_machine_arn} execution: {response['executionArn']} failed with error: {error}"
        )


def files_from_trigger_event(event):
    files_keys = []
    for message in event.get("Records", []):
        try:
            record_body = json.loads(message["body"])
            s3_record = record_body["Records"][0]
            s3_key = s3_record["s3"]["object"]["key"]
            files_keys.append(s3_key)
        except Exception:
            # Ignore records that are not a valid S3 event
            pass
    return files_keys


def pool_file_keys(keys_count, sqs, timeout):
    queue_url = os.environ["RAW_DATA_FILES_SQS_QUEUE_URL"]

    file_keys = []
    message_ids_receipts = []
    start = time.time()
    while len(file_keys) < keys_count:
        if time.time() - start > timeout:
            print(
                f"Batching window timeout reached while pooling file keys from SQS, {keys_count - len(file_keys)} keys left to pool."
            )
            break

        fetch_count = min(keys_count - len(file_keys), 10)
        response = sqs.receive_message(
            QueueUrl=queue_url, MessageSystemAttributeNames=[], MaxNumberOfMessages=fetch_count
        )
        messages = response.get("Messages", [])[:fetch_count]
        keys_batch, message_ids_receipts_batch = parse_sqs_messages(messages)
        file_keys.extend(keys_batch)
        message_ids_receipts.extend(message_ids_receipts_batch)
        # Sleep for 3ms between requests
        time.sleep(0.003)

    return file_keys, message_ids_receipts


def parse_sqs_messages(messages):
    message_ids = []
    files_keys = []
    for message in messages:
        try:
            message_id = message["MessageId"]
            receipt_handle = message["ReceiptHandle"]
            message_ids.append((message_id, receipt_handle))
            record_body = json.loads(message["Body"])
            s3_record = record_body["Records"][0]
            s3_key = s3_record["s3"]["object"]["key"]
            files_keys.append(s3_key)
        except Exception:
            # Ignore records that are not a valid S3 event
            pass
    return files_keys, message_ids
