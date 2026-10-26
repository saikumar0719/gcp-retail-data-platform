"""Unit tests for data_quality/checks.py. Run: pytest tests/ -q"""

from data_quality.checks import (
    duplicates_check,
    null_rate_check,
    row_count_check,
)


def test_row_count_matches():
    assert row_count_check(100, 100).passed


def test_row_count_mismatch_fails():
    assert not row_count_check(100, 98).passed


def test_no_duplicates_passes():
    rows = [{"event_id": "a"}, {"event_id": "b"}]
    assert duplicates_check(rows, "event_id").passed


def test_duplicates_fail():
    rows = [{"event_id": "a"}, {"event_id": "a"}]
    assert not duplicates_check(rows, "event_id").passed


def test_null_rate_within_threshold():
    rows = [{"product_id": "SKU-1"}] * 99 + [{"product_id": None}]
    assert null_rate_check(rows, "product_id").passed


def test_null_rate_above_threshold_fails():
    rows = [{"product_id": None}] * 5 + [{"product_id": "SKU-1"}] * 5
    assert not null_rate_check(rows, "product_id").passed
