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
    VendorPurchaseOrdersStatusStream,
)
from tap_amazon_vendor_central.custom_period_report_stream import REPORT_CONFIGS, CustomPeriodReportStream, create_report_config

STREAM_TYPES = [
    MarketplacesStream,
    # ProductDetails,
    VendorPurchaseOrdersStream,
    VendorFulfilmentPurchaseOrdersStream,
    VendorFulfilmentCustomerInvoicesStream,
    VendorsSalesReportStream,
    VendorsTrafficReportStream,
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
    VendorPurchaseOrdersStatusStream,
]

default_reports_with_periods = [
    {
        "report": "NetPPM",
        "period": "DAY",
    }
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
        th.Property(
            "custom_reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("report", th.StringType, required=True),
                    th.Property("period", th.StringType, required=True),
                )
            ),
            description="List of custom reports to generate. Example: [{'report': 'NetPPM', 'period': 'WEEK'}]"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]        
        # Add custom report streams if configured
        custom_reports = self.config.get("custom_reports", default_reports_with_periods)
        for custom_report in custom_reports:
            report_type = custom_report.get("report")
            period = custom_report.get("period")
            if report_type not in REPORT_CONFIGS:
                raise ValueError(f"Invalid report type: {report_type}. Must be one of: {', '.join(REPORT_CONFIGS.keys())}")
            
            if report_type and period:
                # Create the report config
                report_config = create_report_config(report_type, period)
                
                # Create the custom stream
                custom_stream = CustomPeriodReportStream(self, report_config)
                custom_stream.replication_key = REPORT_CONFIGS[report_type].get("replication_key", "report_end_date")
                streams.append(custom_stream)
                
                self.logger.info(
                    f"Added custom report stream: {custom_stream.name} "
                    f"(report={report_type}, period={period})"
                )
            else:
                self.logger.warning(
                    f"Invalid custom report config: {custom_report}. "
                    "Must have 'report' and 'period' fields."
                )
        return streams


if __name__ == "__main__":
    TapAmazonVendorCentral.cli()
