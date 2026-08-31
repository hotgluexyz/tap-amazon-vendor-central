"""Subscribe & Save replenishment performance streams.

SP-API Replenishment API (2022-11-07), PERFORMANCE only. Vendors do not support
FORECAST on these operations.

Amazon's getSellingPartnerMetrics returns multiple metric bundles per request.
Each bundle has its own timeInterval and field set. Some are single-day KPIs;
others are rolling windows (LTV, retention, signup conversion) with intervals
chosen by Amazon, not aligned to a simple daily incremental grain.

We split that into two partner-level streams:

- vendor_replenishment_daily_metrics: incremental by day (report_end_date).
  Requests only DAILY_PERFORMANCE_METRICS so rolling bundles are not returned.
  Day windows are chunked up to MAX_DAY_CHUNK per API call.

- vendor_replenishment_rolling_metrics: full sync, no replication state.
  Requests only ROLLING_PERFORMANCE_METRICS. Each row is one rolling bundle;
  interval_start_date and interval_end_date come from the API response.

- vendor_replenishment_offer_metrics: incremental per ASIN per day via
  listOfferMetrics (one calendar day per request, paginated).

API reference:
- https://developer-docs.amazon.com/sp-api/reference/getsellingpartnermetrics
- https://developer-docs.amazon.com/sp-api/reference/listoffermetrics
"""

from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import backoff
from dateutil.parser import parse
from singer_sdk import typing as th
from sp_api.base import Marketplaces

from tap_amazon_vendor_central.client import AmazonSellerStream
from tap_amazon_vendor_central.exceptions import (
    InvalidReportParameter,
    PermissionError,
    report_giveup,
)
from tap_amazon_vendor_central.streams import MarketplacesStream
from tap_amazon_vendor_central.utils import InvalidResponse
from sp_api.base.exceptions import SellingApiBadRequestException, SellingApiForbiddenException

PROGRAM_TYPES = ["SUBSCRIBE_AND_SAVE"]
TIME_PERIOD_TYPE = "PERFORMANCE"
AGGREGATION_DAY = "DAY"
MAX_DAY_CHUNK = 31
OFFER_PAGE_SIZE = 100
MAX_OFFER_OFFSET = 9000

DAILY_PERFORMANCE_METRICS = [
    "SHIPPED_SUBSCRIPTION_UNITS",
    "TOTAL_SUBSCRIPTIONS_REVENUE",
    "ACTIVE_SUBSCRIPTIONS",
    "NOT_DELIVERED_DUE_TO_OOS",
    "LOST_REVENUE_DUE_TO_OOS",
    "REVENUE_PENETRATION",
    "COUPONS_REVENUE_PENETRATION",
    "SHARE_OF_COUPON_SUBSCRIPTIONS",
]

ROLLING_PERFORMANCE_METRICS = [
    "SUBSCRIBER_NON_SUBSCRIBER_AVERAGE_REVENUE",
    "SUBSCRIBER_NON_SUBSCRIBER_AVERAGE_REORDERS",
    "SUBSCRIBER_RETENTION",
    "SUBSCRIBER_LIFETIME_VALUE_BY_CUSTOMER_SEGMENT",
    "SIGNUP_CONVERSION_BY_SELLER_FUNDING",
    "REVENUE_BY_DELIVERIES",
    "REVENUE_PENETRATION_BY_SELLER_FUNDING",
]

_DAILY_METRIC_SCHEMA = [
    th.Property("shippedSubscriptionUnits", th.NumberType),
    th.Property("totalSubscriptionsRevenue", th.NumberType),
    th.Property("activeSubscriptions", th.NumberType),
    th.Property("notDeliveredDueToOOS", th.NumberType),
    th.Property("lostRevenueDueToOOS", th.NumberType),
    th.Property("revenuePenetration", th.NumberType),
    th.Property("couponsRevenuePenetration", th.NumberType),
    th.Property("shareOfCouponSubscriptions", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]

_ROLLING_METRIC_SCHEMA = [
    th.Property("subscriberAverageRevenue", th.NumberType),
    th.Property("nonSubscriberAverageRevenue", th.NumberType),
    th.Property("subscriberAverageReorders", th.NumberType),
    th.Property("nonSubscriberAverageReorders", th.NumberType),
    th.Property("subscriberRetentionFor30Days", th.NumberType),
    th.Property("subscriberRetentionFor90Days", th.NumberType),
    th.Property("growingSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("growingSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("establishedSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("establishedSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("lostSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("lostSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("nonSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("signupConversionFor0PercentSellerFunding", th.NumberType),
    th.Property("signupConversionFor5PlusPercentSellerFunding", th.NumberType),
    th.Property("revenueFromSubscriptionsWithMultipleDeliveries", th.NumberType),
    th.Property("revenueFromActiveSubscriptionsWithSingleDelivery", th.NumberType),
    th.Property("revenueFromCancelledSubscriptionsAfterSingleDelivery", th.NumberType),
    th.Property("revenuePenetrationFor0PercentSellerFunding", th.NumberType),
    th.Property("revenuePenetrationFor5PlusPercentSellerFunding", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]

_OFFER_METRIC_SCHEMA = [
    th.Property("asin", th.StringType),
    th.Property("brandName", th.StringType),
    th.Property("productGroup", th.StringType),
    th.Property("shippedSubscriptionUnits", th.NumberType),
    th.Property("totalSubscriptionsRevenue", th.NumberType),
    th.Property("activeSubscriptions", th.NumberType),
    th.Property("notDeliveredDueToOOS", th.NumberType),
    th.Property("lostRevenueDueToOOS", th.NumberType),
    th.Property("revenuePenetration", th.NumberType),
    th.Property("couponsRevenuePenetration", th.NumberType),
    th.Property("shareOfCouponSubscriptions", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]


class ReplenishmentStreamBase(AmazonSellerStream):
    """Shared helpers for Replenishment API streams."""

    parent_stream_type = MarketplacesStream
    lookback_days = 730
    correct_end_date_minus_days = 2

    def get_marketplace_api_id(self, marketplace_code: str) -> str:
        """Return SP-API marketplace id for a marketplace code (e.g. US)."""
        return Marketplaces[marketplace_code].marketplace_id

    def format_day(self, day: datetime) -> str:
        """Format a datetime as an SP-API day boundary timestamp."""
        return day.strftime("%Y-%m-%dT00:00:00Z")

    def day_interval(self, day: datetime) -> Dict[str, str]:
        """Build a single-day timeInterval for Replenishment API requests."""
        formatted = self.format_day(day)
        return {"startDate": formatted, "endDate": formatted}

    def parse_report_end_date(self, time_interval: Optional[dict]) -> Optional[str]:
        """Parse report_end_date from a Replenishment timeInterval."""
        if not time_interval or not time_interval.get("endDate"):
            return None
        return parse(time_interval["endDate"]).date().isoformat()

    def resolve_sync_start(self, context: Optional[dict]) -> datetime:
        """Resolve the incremental sync start datetime (naive local)."""
        start_date = self.get_starting_timestamp(context)
        if start_date:
            return start_date.replace(tzinfo=None)
        if self.config.get("start_date"):
            return parse(self.config.get("start_date")).replace(tzinfo=None)
        return datetime.now() - timedelta(days=self.lookback_days)

    def resolve_max_sync_end(self) -> datetime:
        """Return the latest day to sync, applying lag and optional config end_date."""
        current_date = datetime.now()
        max_end = current_date - timedelta(days=self.correct_end_date_minus_days)
        if self.config.get("end_date"):
            config_end = parse(self.config.get("end_date")).replace(tzinfo=None)
            if config_end < max_end:
                max_end = config_end
        return max_end.replace(hour=0, minute=0, second=0, microsecond=0)

    def clamp_start_to_lookback(self, start_date: datetime) -> datetime:
        """Clamp start_date to the API trailing window."""
        minimum_start = datetime.now() - timedelta(days=self.lookback_days)
        if start_date < minimum_start:
            return minimum_start
        return start_date

    def translate_replenishment_error(self, exc: Exception) -> None:
        """Map SP-API errors to tap exceptions for giveup and child-stream handling."""
        if isinstance(exc, SellingApiForbiddenException):
            raise PermissionError(exc.error or exc.message or str(exc)) from exc
        if isinstance(exc, SellingApiBadRequestException):
            raise InvalidReportParameter(exc.error or exc.message or str(exc)) from exc
        raise InvalidResponse(exc) from exc

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
        giveup=report_giveup,
    )
    def get_selling_partner_metrics(
        self,
        marketplace_code: str,
        time_interval: Dict[str, str],
        metrics: List[str],
    ) -> dict:
        """Call getSellingPartnerMetrics for a marketplace."""
        client = self.get_sp_replenishment(marketplace_code)
        try:
            response = client.get_selling_partner_metrics(
                marketplaceId=self.get_marketplace_api_id(marketplace_code),
                timePeriodType=TIME_PERIOD_TYPE,
                programTypes=PROGRAM_TYPES,
                timeInterval=time_interval,
                aggregationFrequency=AGGREGATION_DAY,
                metrics=metrics,
            )
        except Exception as exc:
            self.translate_replenishment_error(exc)
        return response.payload


class VendorReplenishmentDailyMetricsStream(ReplenishmentStreamBase):
    """Daily Subscribe & Save performance metrics for the selling partner."""

    name = "vendor_replenishment_daily_metrics"
    primary_keys = ["marketplace_id", "report_end_date"]
    replication_key = "report_end_date"

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("report_end_date", th.DateTimeType),
        *_DAILY_METRIC_SCHEMA,
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        start_date = self.clamp_start_to_lookback(self.resolve_sync_start(context))
        max_end = self.resolve_max_sync_end()
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while start_date <= max_end:
            chunk_end = min(
                start_date + timedelta(days=MAX_DAY_CHUNK - 1),
                max_end,
            )
            time_interval = {
                "startDate": self.format_day(start_date),
                "endDate": self.format_day(chunk_end),
            }
            self.logger.info(
                f"Fetching replenishment daily metrics for {marketplace_code}: "
                f"{time_interval['startDate']} to {time_interval['endDate']}"
            )
            payload = self.get_selling_partner_metrics(
                marketplace_code,
                time_interval,
                DAILY_PERFORMANCE_METRICS,
            )
            for bundle in payload.get("metrics", []):
                record = dict(bundle)
                record["marketplace_id"] = marketplace_code
                report_end_date = self.parse_report_end_date(record.get("timeInterval"))
                if not report_end_date:
                    continue
                record["report_end_date"] = report_end_date
                yield record
            start_date = chunk_end + timedelta(days=1)


class VendorReplenishmentRollingMetricsStream(ReplenishmentStreamBase):
    """Rolling-window Subscribe & Save metrics (full sync, no replication state)."""

    name = "vendor_replenishment_rolling_metrics"
    primary_keys = ["marketplace_id", "interval_start_date", "interval_end_date"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("interval_start_date", th.StringType),
        th.Property("interval_end_date", th.StringType),
        *_ROLLING_METRIC_SCHEMA,
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        anchor_day = self.resolve_max_sync_end()
        time_interval = self.day_interval(anchor_day)
        self.logger.info(
            f"Fetching replenishment rolling metrics for {marketplace_code} "
            f"(anchor {time_interval['startDate']})"
        )
        payload = self.get_selling_partner_metrics(
            marketplace_code,
            time_interval,
            ROLLING_PERFORMANCE_METRICS,
        )
        for bundle in payload.get("metrics", []):
            time_interval_data = bundle.get("timeInterval") or {}
            start = time_interval_data.get("startDate")
            end = time_interval_data.get("endDate")
            if not start or not end or start == end:
                continue
            record = dict(bundle)
            record["marketplace_id"] = marketplace_code
            record["interval_start_date"] = start
            record["interval_end_date"] = end
            yield record


class VendorReplenishmentOfferMetricsStream(ReplenishmentStreamBase):
    """Per-ASIN daily Subscribe & Save offer metrics."""

    name = "vendor_replenishment_offer_metrics"
    primary_keys = ["marketplace_id", "asin", "report_end_date"]
    replication_key = "report_end_date"

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("report_end_date", th.DateTimeType),
        *_OFFER_METRIC_SCHEMA,
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
        giveup=report_giveup,
    )
    def list_offer_metrics_page(
        self,
        marketplace_code: str,
        time_interval: Dict[str, str],
        offset: int,
    ) -> dict:
        """Fetch one page of listOfferMetrics."""
        client = self.get_sp_replenishment(marketplace_code)
        try:
            response = client.list_offer_metrics(
                pagination={"limit": OFFER_PAGE_SIZE, "offset": offset},
                filters={
                    "timePeriodType": TIME_PERIOD_TYPE,
                    "programTypes": PROGRAM_TYPES,
                    "marketplaceId": self.get_marketplace_api_id(marketplace_code),
                    "timeInterval": time_interval,
                    "aggregationFrequency": AGGREGATION_DAY,
                },
            )
        except Exception as exc:
            self.translate_replenishment_error(exc)
        return response.payload

    def iter_offers_for_day(
        self, marketplace_code: str, day: datetime
    ) -> Iterable[dict]:
        """Yield all offer metric records for a single day."""
        time_interval = self.day_interval(day)
        offset = 0
        while True:
            payload = self.list_offer_metrics_page(marketplace_code, time_interval, offset)
            offers = payload.get("offers", [])
            if not offers:
                break
            for offer in offers:
                yield offer
            if len(offers) < OFFER_PAGE_SIZE:
                break
            next_offset = offset + len(offers)
            if next_offset > MAX_OFFER_OFFSET:
                if len(offers) == OFFER_PAGE_SIZE:
                    self.logger.warning(
                        f"listOfferMetrics offset limit ({MAX_OFFER_OFFSET}) reached for "
                        f"{marketplace_code} on {time_interval['startDate']}. "
                        "Remaining offers were not fetched."
                    )
                break
            offset = next_offset

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        start_date = self.clamp_start_to_lookback(self.resolve_sync_start(context))
        max_end = self.resolve_max_sync_end()
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while start_date <= max_end:
            self.logger.info(
                f"Fetching replenishment offer metrics for {marketplace_code}: "
                f"{self.format_day(start_date)}"
            )
            for offer in self.iter_offers_for_day(marketplace_code, start_date):
                record = dict(offer)
                record["marketplace_id"] = marketplace_code
                report_end_date = self.parse_report_end_date(record.get("timeInterval"))
                if not report_end_date:
                    continue
                record["report_end_date"] = report_end_date
                yield record
            start_date += timedelta(days=1)
