"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_amazon_vendor_central.tap import TapAmazonVendorCentral

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "lwa_client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "refresh_token": "test-refresh-token",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapAmazonVendorCentral, config=SAMPLE_CONFIG)
    # skip connection/sync tests
    for test in tests[:2]:
        test()


# TODO: Create additional tests as appropriate for your tap.
