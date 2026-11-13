import ast
from pathlib import Path

import pytest
from fraudlib.schemas import TransactionEvent
from pydantic import ValidationError

GOOD = {"transaction_id": "T1", "event_time": "2026-01-01T00:00:00+00:00", "account_id": "C1",
        "counterparty_id": "C2", "txn_type": "TRANSFER", "amount": 10.0, "old_balance_orig": 5.0,
        "old_balance_dest": 0.0}


def test_valid_event():
    assert TransactionEvent(**GOOD).amount == 10.0


@pytest.mark.parametrize("patch", [{"amount": -1}, {"amount": 0}, {"txn_type": "REFUND"},
                                   {"event_time": "2026-01-01T00:00:00"}, {"schema_version": 2},
                                   {"unexpected": 1}])
def test_invalid_events_rejected(patch):
    with pytest.raises(ValidationError):
        TransactionEvent(**{**GOOD, **patch})


def test_spark_schema_matches_pydantic_contract():
    """stream_job.py can't import pydantic inside Spark, so check field names statically."""
    src = Path("services/streaming/stream_job.py").read_text()
    tree = ast.parse(src)
    spark_fields = [
        n.args[0].value for n in ast.walk(tree)
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "StructField"
    ]
    assert spark_fields == list(TransactionEvent.model_fields)
