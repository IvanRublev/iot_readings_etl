import json
import pytest
import time

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from lambda_pooling.s3bronze_file_events_pooling import lambda_handler, pool_file_keys


STEP_FUNCTION_ARN = "step-function-name-arn"
RAW_DATA_FILES_SQS_QUEUE_URL = "raw-sqs-queue-url"
MAXIMUM_BATCHING_WINDOW_IN_SECONDS = 0.1


@contextmanager
def mock_env(processors_count=2, files_per_processor=5):
    variables = {
        "FILE_PROCESSORS_COUNT": str(processors_count),
        "RAW_DATA_FILES_PER_PROCESSOR": str(files_per_processor),
        "DATA_PROCESSING_STATE_MACHINE_ARN": STEP_FUNCTION_ARN,
        "RAW_DATA_FILES_SQS_QUEUE_URL": RAW_DATA_FILES_SQS_QUEUE_URL,
        "MAXIMUM_BATCHING_WINDOW_IN_SECONDS": str(MAXIMUM_BATCHING_WINDOW_IN_SECONDS),
    }
    with patch.dict("os.environ", variables):
        yield


# Lambda handler tests


def test_pass_lambda_handler_given_empty_event_returns_none():
    with mock_env():
        assert lambda_handler({}, {}) is None


def test_pass_lambda_handler_given_malformed_file_event_ignores_it():
    event_fixture = {"Records": [{"key": "value"}]}
    mock_stepfunctions = MagicMock()
    mock_sqs = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env():
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    assert mock_stepfunctions.start_sync_execution.call_count == 0


def test_pass_lambda_handler_given_trigger_event_pulls_file_keys_from_sqs_to_load_all_processors():
    trigger_msg_count = 2
    event_fixture = build_trigger_event_fixture(trigger_msg_count)
    mock_sqs = MagicMock()
    sqs_msg_count = 2
    mock_sqs.receive_message.return_value = build_sqs_messages_fixture(sqs_msg_count)
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env(processors_count=3, files_per_processor=6):
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    assert mock_sqs.receive_message.call_count == (18 - trigger_msg_count) / sqs_msg_count


def test_pass_lambda_handler_given_trigger_event_launches_step_function_with_joined_list_of_trigger_event_and_sqs_file_keys():
    event_fixture = build_trigger_event_fixture(2)
    mock_sqs = MagicMock()
    mock_sqs.receive_message.return_value = build_sqs_messages_fixture(2)
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env(processors_count=1, files_per_processor=4):
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    files_list = json.dumps(
        [
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-3.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-4.json",
            ]
        ]
    )
    mock_stepfunctions.start_sync_execution.assert_called_once_with(stateMachineArn=STEP_FUNCTION_ARN, input=files_list)


def test_pass_lambda_handler_given_trigger_event_chunks_file_list_by_raw_data_files_per_files_processor_value():
    event_fixture = build_trigger_event_fixture(2)
    mock_sqs = MagicMock()
    # Simulate only 2 messages in SQS queue
    mock_sqs.receive_message.side_effect = [build_sqs_messages_fixture(2)] + [{} for _ in range(100)]
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env(processors_count=2, files_per_processor=3):
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    files_list = json.dumps(
        [
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-3.json",
            ],
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-4.json",
            ],
        ]
    )
    mock_stepfunctions.start_sync_execution.assert_called_once_with(stateMachineArn=STEP_FUNCTION_ARN, input=files_list)


def test_fail_lambda_handler_when_step_function_finishes_with_error():
    event_fixture = build_trigger_event_fixture(10)
    mock_sqs = MagicMock()
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {
        "executionArn": "step-function-execution-arn",
        "stateMachineArn": STEP_FUNCTION_ARN,
        "status": "TIMED_OUT",
        "error": "error happend",
        "cause": "underlying cause",
    }

    error = {"status": "TIMED_OUT", "error": "error happend", "cause": "underlying cause"}
    error_string = (
        f"Step Function: {STEP_FUNCTION_ARN} execution: step-function-execution-arn failed with error: {error}"
    )
    with pytest.raises(RuntimeError, match=error_string):
        with mock_env(processors_count=2, files_per_processor=5):
            lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)


def test_pass_lambda_handler_when_finished_processing_deletes_fetched_messages_from_sqs_in_batches_of_10():
    event_fixture = build_trigger_event_fixture(2)
    mock_sqs = MagicMock()
    mock_sqs.receive_message.return_value = build_sqs_messages_fixture(12)
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env(processors_count=2, files_per_processor=7):
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    while mock_stepfunctions.start_sync_execution.call_count == 0:
        time.sleep(0.1)

    assert mock_sqs.delete_message_batch.call_count == 2
    mock_sqs.delete_message_batch.assert_any_call(
        QueueUrl=RAW_DATA_FILES_SQS_QUEUE_URL,
        Entries=[
            {"Id": "57867db4-aba7-4dd5-a680-afd257f15def", "ReceiptHandle": "receipt-handle-1"},
            {"Id": "3dbde258-b05d-484b-a796-700bbd0d6368", "ReceiptHandle": "receipt-handle-2"},
        ]
        * 5,
    )

    mock_sqs.delete_message_batch.assert_any_call(
        QueueUrl=RAW_DATA_FILES_SQS_QUEUE_URL,
        Entries=[
            {"Id": "57867db4-aba7-4dd5-a680-afd257f15def", "ReceiptHandle": "receipt-handle-1"},
            {"Id": "3dbde258-b05d-484b-a796-700bbd0d6368", "ReceiptHandle": "receipt-handle-2"},
        ],
    )


def test_pass_lambda_handler_when_didnt_pull_messages_from_sqs_doest_delete_any():
    event_fixture = build_trigger_event_fixture(2)
    mock_sqs = MagicMock()
    mock_sqs.receive_message.return_value = {}
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    with mock_env(processors_count=2, files_per_processor=4):
        lambda_handler(event_fixture, {}, mock_sqs, mock_stepfunctions)

    assert mock_sqs.delete_message_batch.call_count == 0


# Pooling file keys tests


def test_pass_pool_file_keys_given_keys_count_pulls_that_amount_of_file_keys_from_sqs():
    mock_sqs = MagicMock()
    mock_sqs.receive_message.return_value = build_sqs_messages_fixture(10)

    with mock_env():
        file_keys, message_ids_receipts = pool_file_keys(11, mock_sqs, MAXIMUM_BATCHING_WINDOW_IN_SECONDS)

    assert len(file_keys) == 11
    assert file_keys == [
        "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-3.json",
        "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-4.json",
    ] * 5 + ["2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-3.json"]

    assert len(message_ids_receipts) == 11
    assert message_ids_receipts == [
        ("57867db4-aba7-4dd5-a680-afd257f15def", "receipt-handle-1"),
        ("3dbde258-b05d-484b-a796-700bbd0d6368", "receipt-handle-2"),
    ] * 5 + [("57867db4-aba7-4dd5-a680-afd257f15def", "receipt-handle-1")]

    assert mock_sqs.receive_message.call_count == 2
    mock_sqs.receive_message.assert_any_call(
        QueueUrl=RAW_DATA_FILES_SQS_QUEUE_URL,
        MessageSystemAttributeNames=[],
        MaxNumberOfMessages=10,
    )
    mock_sqs.receive_message.assert_any_call(
        QueueUrl=RAW_DATA_FILES_SQS_QUEUE_URL,
        MessageSystemAttributeNames=[],
        MaxNumberOfMessages=1,
    )


def test_pass_pool_file_keys_given_empty_sqs_returns_empty_lists_after_batching_window_timeout():
    mock_sqs = MagicMock()
    mock_sqs.receive_message.return_value = {}
    timeout = 0.3

    with mock_env():
        file_keys, message_ids_receipts = pool_file_keys(11, mock_sqs, timeout)

    assert file_keys == []
    assert message_ids_receipts == []


# Helper functions


def build_trigger_event_fixture(messages_count=2):
    assert messages_count <= 10, "By AWS limitation messages count in trigger event should be less or equal to 10."
    return build_raw_file_event_fixture(
        messages_count, "tests/lambdas/fixtures/raw_data_file_lambda_trigger_event.json", "Records"
    )


def build_sqs_messages_fixture(messages_count=2):
    return build_raw_file_event_fixture(
        messages_count, "tests/lambdas/fixtures/raw_data_file_sqs_messages.json", "Messages"
    )


def build_raw_file_event_fixture(messages_count, fixture_path, root_key):
    assert messages_count % 2 == 0, "Messages count should be even number"
    pairs_count = messages_count // 2

    text = open(fixture_path, "r").read()
    event_dict = json.loads(text)
    event_dict[root_key] = event_dict[root_key] * pairs_count

    return event_dict
