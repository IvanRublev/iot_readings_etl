import boto3
import json
import os


def lambda_handler(event, context, stepfunctions=None):
    files_list = files_from_sqs(event)
    if files_list == []:
        return None

    files_per_processor = int(os.environ["RAW_DATA_FILES_PER_FILES_PROCESSOR"])
    files_list_chunks = [
        files_list[i : i + files_per_processor] for i in range(0, len(files_list), files_per_processor)
    ]
    files_list_json = json.dumps(files_list_chunks)

    if stepfunctions is None:
        stepfunctions = boto3.client("stepfunctions")

    state_machine_arn = os.environ["DATA_PROCESSING_STATE_MACHINE_ARN"]
    print(f"Starting step function execution: {state_machine_arn} with {len(files_list)} raw datafiles.")

    response = stepfunctions.start_sync_execution(stateMachineArn=state_machine_arn, input=files_list_json)
    if response["status"] == "SUCCEEDED":
        return None
    else:
        error = {"status": response["status"], "error": response["error"], "cause": response["cause"]}
        raise RuntimeError(
            f"Step Function: {state_machine_arn} execution: {response['executionArn']} failed with error: {error}"
        )


def files_from_sqs(event):
    files_list = []
    for record in event.get("Records", []):
        try:
            record_body = json.loads(record["body"])
            s3_record = record_body["Records"][0]
            s3_key = s3_record["s3"]["object"]["key"]
            files_list.append(s3_key)
        except Exception:
            # Ignore records that are not a valid S3 event
            pass
    return files_list
