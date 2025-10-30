import json
from unittest.mock import Mock, patch
import pytest

from services.credit_consumer import CreditConsumer
from services.schemas.credit_consumer import LoanApplication, LoanType, CreditReport


@pytest.fixture
def mock_producer():
    p = Mock()
    p.produce = Mock()
    p.flush = Mock()
    return p


@pytest.fixture
def mock_consumer():
    c = Mock()
    c.poll = Mock()
    c.subscribe = Mock()
    c.close = Mock()
    return c


@pytest.fixture
def credit_consumer(mock_producer, mock_consumer):
    # Patch the confluent_kafka Producer and Consumer constructors to return our mocks
    with patch("services.credit_consumer.Producer", return_value=mock_producer), \
         patch("services.credit_consumer.Consumer", return_value=mock_consumer):
        consumer = CreditConsumer(bootstrap_servers="localhost:9092", input_topic="test_topic")
        return consumer


def test_process_loan_application_default_logic(credit_consumer):
    loan_app = LoanApplication(
        application_id="app-3",
        pan_number="ZZZZZ9999Z",
        applicant_name="Charlie",
        monthly_income_inr=80000,  # > 75000 so +40
        loan_amount_inr=200000,
        loan_type=LoanType.HOME  # +10
    )

    report = credit_consumer.process_loan_application(loan_app)
    # base 650 +40 +10 +/-5 -> expected between 695 and 705
    assert 695 <= report.cibil_score <= 705
    assert 300 <= report.cibil_score <= 900


def test_publish_credit_report_calls_produce_and_flush(credit_consumer, mock_producer):
    report = CreditReport(application_id="app-4", pan_number="P", cibil_score=750)
    credit_consumer.publish_credit_report(report)

    mock_producer.produce.assert_called_once()
    # inspect call args
    produce_args, produce_kwargs = mock_producer.produce.call_args
    # first positional arg is topic
    assert produce_args[0] == 'credit_reports_generated'
    # value passed as bytes
    assert isinstance(produce_kwargs.get("value", produce_args[1] if len(produce_args) > 1 else None), (bytes, type(None)))
    # decode and validate content
    value_bytes = produce_kwargs.get("value", produce_args[1] if len(produce_args) > 1 else None)
    if value_bytes:
        payload = json.loads(value_bytes.decode("utf-8"))
        assert payload["application_id"] == "app-4"
        assert payload["cibil_score"] == 750
    mock_producer.flush.assert_called_once()


def _make_mock_msg(payload_bytes: bytes, error_obj=None):
    m = Mock()
    if error_obj is None:
        m.error.return_value = None
    else:
        m.error.return_value = error_obj
    m.value.return_value = payload_bytes
    return m


def test_run_processes_one_message_and_exits(credit_consumer, mock_consumer):
    payload = {
        "application_id": "run-1",
        "pan_number": "ABCDE1234F",
        "applicant_name": "Run User",
        "monthly_income_inr": 50000,
        "loan_amount_inr": 200000,
        "loan_type": "PERSONAL"
    }
    msg = _make_mock_msg(json.dumps(payload).encode("utf-8"))
    # consumer.poll will return msg then raise KeyboardInterrupt to exit loop
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]
    # run should process the message and exit on KeyboardInterrupt
    with patch.object(credit_consumer, "publish_credit_report") as mock_publish:
        credit_consumer.run()

    mock_consumer.subscribe.assert_called_once_with(["test_topic"])
    mock_publish.assert_called_once()
    mock_consumer.close.assert_called_once()


def test_run_handles_invalid_json_and_exits(credit_consumer, mock_consumer):
    msg = _make_mock_msg(b'invalid json')
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    # Should not raise; should close consumer
    credit_consumer.run()
    mock_consumer.close.assert_called_once()


def test_run_handles_kafka_error_and_exits(credit_consumer, mock_consumer):
    # create an error object with code() method not equal to partition EOF
    error_obj = Mock()
    error_obj.code.return_value = 999
    msg = _make_mock_msg(b'', error_obj=error_obj)
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    # Should handle the error and exit gracefully
    credit_consumer.run()
    mock_consumer.close.assert_called_once()
