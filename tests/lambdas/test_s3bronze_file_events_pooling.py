import json
import pytest

from unittest.mock import MagicMock, patch

from lambda_pooling.s3bronze_file_events_pooling import lambda_handler


STEP_FUNCTION_ARN = "step-function-name-arn"
RAW_DATA_FILES_PER_FILES_PROCESSOR = 5


@pytest.fixture(autouse=True)
def mock_env_variables():
    variables = {
        "DATA_PROCESSING_STATE_MACHINE_ARN": STEP_FUNCTION_ARN,
        "RAW_DATA_FILES_PER_FILES_PROCESSOR": str(RAW_DATA_FILES_PER_FILES_PROCESSOR),
    }
    with patch.dict("os.environ", variables):
        yield


def test_pass_lambda_handler_given_empty_event_returns_none():
    assert lambda_handler({}, {}) is None


def test_pass_lambda_handler_given_malformed_file_event_ignores_it():
    event_fixture = {"Records": [{"key": "value"}]}
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    result = lambda_handler(event_fixture, {}, mock_stepfunctions)

    assert result is None
    assert mock_stepfunctions.start_sync_execution.call_count == 0


def test_pass_lambda_handler_given_raw_data_file_event_launches_step_function_with_list_of_files_to_process():
    event_fixture = json.loads(open("tests/lambdas/fixtures/s3bronze_raw_data_file_event_sqs.json", "r").read())
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    result = lambda_handler(event_fixture, {}, mock_stepfunctions)

    assert result is None
    files_list = json.dumps(
        [
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
            ]
        ]
    )
    mock_stepfunctions.start_sync_execution.assert_called_once_with(stateMachineArn=STEP_FUNCTION_ARN, input=files_list)


def test_pass_lambda_handler_given_raw_data_file_event_chunks_file_list_by_raw_data_files_per_files_processor_value():
    event_fixture = build_event_fixture(messages_count=RAW_DATA_FILES_PER_FILES_PROCESSOR + 1)
    mock_stepfunctions = MagicMock()
    mock_stepfunctions.start_sync_execution.return_value = {"status": "SUCCEEDED"}

    lambda_handler(event_fixture, {}, mock_stepfunctions)

    files_list = json.dumps(
        [
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-2.json",
            ],
            [
                "2024/10/02/job_328e430e-2569-46f2-8ca7-2fd8eb7f1549/raw-1.json",
            ],
        ]
    )
    mock_stepfunctions.start_sync_execution.assert_called_once_with(stateMachineArn=STEP_FUNCTION_ARN, input=files_list)


def test_fail_lambda_handler_when_step_function_finishes_with_error():
    event_fixture = build_event_fixture()
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
        lambda_handler(event_fixture, {}, mock_stepfunctions)


# Helper functions


def build_event_fixture(messages_count=2):
    assert messages_count % 2 == 0, "Messages count should be even number"
    pairs_count = messages_count // 2
    event_dict = json.loads(open("tests/lambdas/fixtures/s3bronze_raw_data_file_event_sqs.json", "r").read())
    event_dict["Records"] = event_dict["Records"] * pairs_count
    return event_dict
