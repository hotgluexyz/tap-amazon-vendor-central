"""Tests for repeat purchase report stream and week alignment helpers."""

import datetime

from tap_amazon_vendor_central.tap import TapAmazonVendorCentral
from tap_amazon_vendor_central.utils import align_to_week_end, align_to_week_start


SAMPLE_CONFIG = {
    "start_date": "2026-01-01",
    "lwa_client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "refresh_token": "test-refresh-token",
}


def test_align_to_week_start_sunday():
    dt = datetime.datetime(2026, 8, 16)  # Sunday
    assert align_to_week_start(dt).date() == datetime.date(2026, 8, 16)


def test_align_to_week_start_monday():
    dt = datetime.datetime(2026, 8, 17)  # Monday
    assert align_to_week_start(dt).date() == datetime.date(2026, 8, 16)


def test_align_to_week_end_saturday():
    dt = datetime.datetime(2026, 8, 15)  # Saturday
    assert align_to_week_end(dt).date() == datetime.date(2026, 8, 15)


def test_align_to_week_end_monday():
    dt = datetime.datetime(2026, 8, 17)  # Monday
    assert align_to_week_end(dt).date() == datetime.date(2026, 8, 15)


def test_repeat_purchase_stream_in_catalog():
    tap = TapAmazonVendorCentral(config=SAMPLE_CONFIG)
    stream_names = {s.name for s in tap.streams.values()}
    assert "vendor_repeat_purchase_report" in stream_names


def test_repeat_purchase_stream_metadata():
    tap = TapAmazonVendorCentral(config=SAMPLE_CONFIG)
    stream = tap.streams["vendor_repeat_purchase_report"]
    assert stream.report_name == "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT"
    assert stream.report_options == {"reportPeriod": "WEEK"}
    assert stream.replication_key == "report_end_date"


def test_repeat_purchase_post_process_sets_report_end_date():
    tap = TapAmazonVendorCentral(config=SAMPLE_CONFIG)
    stream = tap.streams["vendor_repeat_purchase_report"]
    row = {
        "dataByAsin": [
            {"endDate": "2026-08-09", "asin": "B001"},
            {"endDate": "2026-08-16", "asin": "B002"},
        ]
    }
    result = stream.post_process(row)
    assert result["report_end_date"] == "2026-08-16"
