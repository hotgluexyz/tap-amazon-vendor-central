"""Amazon-Seller tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_amazon_vendor_central.streams import (
    AmazonSellerStream,
    MarketplacesStream,
    ProductDetails,
    VendorPurchaseOrdersStream,
    VendorFulfilmentPurchaseOrdersStream,
    VendorFulfilmentCustomerInvoicesStream,
    VendorsSalesReportStream,
    VendorsTrafficReportStream,
    VendorsRepeatPurchaseReportStream,
    VendorsInventoryReportStream,
    VendorsForecastingReportStream,
    VendorsSalesRealtimeReportStream,
    VendorsInventoryRealtimeReportStream,
    VendorsTrafficRealtimeReportStream,
    InventoryProductsListStream,
    ProductDetails,
    VendorsSalesSourcingReportStream,
    VendorsInventorySourcingReportStream,
    InventoryProductsSourcingListStream,
)

STREAM_TYPES = [
    MarketplacesStream,
    # ProductDetails,
    VendorPurchaseOrdersStream,
    VendorFulfilmentPurchaseOrdersStream,
    VendorFulfilmentCustomerInvoicesStream,
    VendorsSalesReportStream,
    VendorsTrafficReportStream,
    VendorsRepeatPurchaseReportStream,
    VendorsInventoryReportStream,
    VendorsForecastingReportStream,
    VendorsSalesRealtimeReportStream,
    VendorsInventoryRealtimeReportStream,
    VendorsTrafficRealtimeReportStream,
    InventoryProductsListStream,
    ProductDetails,
    VendorsSalesSourcingReportStream,
    VendorsInventorySourcingReportStream,
    InventoryProductsSourcingListStream,
]


class TapAmazonVendorCentral(Tap):
    """Amazon-Vendor Central tap class."""

    name = "tap-amazon-vendor-central"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("lwa_client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("aws_access_key", th.StringType, required=False),
        th.Property("aws_secret_key", th.StringType, required=False),
        th.Property("role_arn", th.StringType, required=False),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("sandbox", th.BooleanType, default=False),
        th.Property(
            "report_types",
            th.CustomType({"type": ["array", "string"]}),
            default=["GET_LEDGER_DETAIL_VIEW_DATA", "GET_MERCHANT_LISTINGS_ALL_DATA"],
        ),
        th.Property(
            "processing_status",
            th.CustomType({"type": ["array", "string"]}),
            default=["IN_QUEUE", "IN_PROGRESS"],
        ),
        th.Property(
            "marketplaces",
            th.CustomType({"type": ["array", "string"]}),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapAmazonVendorCentral.cli()
