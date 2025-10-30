import json
from types import SimpleNamespace
from unittest.mock import Mock, patch
import pytest
from confluent_kafka import KafkaError

from services.decision_consumer import DecisionConsumer


def _make_msg(value_bytes: bytes, error_obj=None):
    m = Mock()
    if error_obj is None:
        m.error.return_value = None
    else:
        m.error.return_value = error_obj
    m.value.return_value = value_bytes
    return m


@pytest.fixture
def mock_consumer():
    c = Mock()
    c.poll = Mock()
    c.subscribe = Mock()
    c.close = Mock()
    return c


@pytest.fixture
def decision_consumer(mock_consumer):
    """
    Create a DecisionConsumer while fully mocking:
    - Kafka Consumer (so no real broker)
    - SQLAlchemy create_engine, sessionmaker and automap_base to avoid DB access
    """
    with patch("services.decision_consumer.Consumer", return_value=mock_consumer), \
         patch("services.decision_consumer.create_engine", return_value=Mock()) as mock_create_engine, \
         patch("services.decision_consumer.sessionmaker", return_value=lambda *a, **k: (lambda: Mock())) as mock_sessionmaker, \
         patch("services.decision_consumer.automap_base") as mock_automap:
        # automap_base() should return an object with prepare() and classes.<applications>
        base = Mock()
        base.prepare = Mock()
        base.classes = SimpleNamespace(applications=Mock())
        mock_automap.return_value = base

        # instantiate DecisionConsumer (safe because we've mocked DB/consumer constructors)
        dc = DecisionConsumer(
            bootstrap_servers="localhost:9092",
            input_topic="test_topic",
            database_url="postgresql://user:pass@localhost/db"
        )
        return dc


def test_decide_status_rules():
    # instantiate without running __init__
    dc = DecisionConsumer.__new__(DecisionConsumer)
    assert dc._decide_status(649, 10000, 100000) == "REJECTED"
    assert dc._decide_status(650, 5000, 20000) == "PRE_APPROVED"
    assert dc._decide_status(700, 1000, 200000) == "MANUAL_REVIEW"


def test_run_with_full_payload_calls_update(decision_consumer, mock_consumer):
    payload = {
        "application_id": "app-1",
        "cibil_score": 700,
        "monthly_income_inr": 10000,
        "loan_amount_inr": 100000
    }
    msg = _make_msg(json.dumps(payload).encode("utf-8"))
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_consumer.subscribe.assert_called_once_with(["test_topic"])
    mock_update.assert_called_once_with("app-1", "PRE_APPROVED", 700)
    mock_consumer.close.assert_called_once()


def test_run_fetches_financials_when_missing(decision_consumer, mock_consumer):
    payload = {
        "application_id": "app-2",
        "cibil_score": 760
    }
    msg = _make_msg(json.dumps(payload).encode("utf-8"))
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_fetch_app_financials", return_value=(5000.0, 20000.0)) as mock_fetch, \
         patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_fetch.assert_called_once_with("app-2")
    mock_update.assert_called_once_with("app-2", "PRE_APPROVED", 760)
    mock_consumer.close.assert_called_once()


def test_run_skips_when_no_application_id(decision_consumer, mock_consumer):
    payload = {
        "cibil_score": 700,
        "monthly_income_inr": 5000,
        "loan_amount_inr": 20000
    }
    msg = _make_msg(json.dumps(payload).encode("utf-8"))
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_update.assert_not_called()
    mock_consumer.close.assert_called_once()


def test_run_handles_invalid_json_gracefully(decision_consumer, mock_consumer):
    msg = _make_msg(b'invalid json')
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_update.assert_not_called()
    mock_consumer.close.assert_called_once()


def test_run_handles_kafka_error_codes(decision_consumer, mock_consumer):
    err = Mock()
    err.code.return_value = 999
    msg = _make_msg(b'', error_obj=err)
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_update.assert_not_called()
    mock_consumer.close.assert_called_once()


def test_run_ignores_partition_eof(decision_consumer, mock_consumer):
    err = Mock()
    err.code.return_value = KafkaError._PARTITION_EOF
    msg = _make_msg(b'', error_obj=err)
    mock_consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    with patch.object(decision_consumer, "_update_application") as mock_update:
        decision_consumer.run()

    mock_update.assert_not_called()
    mock_consumer.close.assert_called_once()