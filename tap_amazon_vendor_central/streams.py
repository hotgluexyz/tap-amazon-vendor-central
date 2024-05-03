"""Stream type classes for tap-amazon-seller."""
from datetime import datetime, timedelta

from typing import Iterable, Optional, Dict, Any

import backoff
from singer_sdk import typing as th
from sp_api.util import load_all_pages

from tap_amazon_vendor_central.client import AmazonSellerStream
from tap_amazon_vendor_central.utils import InvalidResponse, timeout
from sp_api.base.exceptions import SellingApiServerException
from dateutil.relativedelta import relativedelta
from sp_api.base import Marketplaces
from abc import abstractproperty
import pytz
import json
import os

# Replication methods
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"

from singer_sdk.helpers._state import (
    increment_state,
)

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class MarketplacesStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_marketplaces"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "marketplace_id": record["id"],
        }

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if self.config.get("marketplaces"):
            marketplaces = self.config.get("marketplaces")
        else:
            marketplaces = [
                "US",
                "CA",
                "MX",
                "BR",
                "ES",
                "GB",
                "FR",
                "NL",
                "DE",
                "IT",
                "SE",
                "PL",
                "EG",
                "TR",
                "SA",
                "AE",
                "IN",
                "SG",
                "AU",
                "JP",
            ]

        for mp in marketplaces:
            yield {"id": mp}


class VendorFulfilmentPurchaseOrdersStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_fulfilment_purchase_orders"
    primary_keys = ["purchaseOrderNumber"]
    # TODO loook for relevant replication key in the live data
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        # Optional, not always populated
        th.Property(
            "orderDetails",
            th.ObjectType(
                th.Property("customerOrderNumber", th.StringType),
                th.Property("orderDate", th.DateTimeType),
                th.Property("orderStatus", th.StringType),
                th.Property(
                    "shipmentDetails", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property("taxTotal", th.CustomType({"type": ["object", "string"]})),
                th.Property(
                    "sellingParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property(
                    "shipToParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property(
                    "billToParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property("items", th.CustomType({"type": ["array", "string"]})),
            ),
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    @load_all_pages()
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_vendor_fulfilment(mp)
            orders_obj = orders.get_orders(**kwargs)
            return orders_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("Orders", []):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = datetime.strptime(
                    self.config.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                return self.load_order_page(
                    mp="ATVPDKIKX0DER", CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    createdBefore=end_date,
                    createdAfter=start_date,
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class VendorFulfilmentCustomerInvoicesStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_fulfilment_customer_invoices"
    primary_keys = ["purchaseOrderNumber"]
    # TODO loook for relevant key in live data
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("content", th.StringType),
        th.Property("sellingParty", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipFromParty", th.CustomType({"type": ["object", "string"]})),
        th.Property("labelFormat", th.CustomType({"type": ["object", "string"]})),
        th.Property("labelData", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    @load_all_pages()
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            vendor_shipping = self.get_sp_vendor_fulfilment_shipping(mp)
            invoices_obj = vendor_shipping.get_customer_invoices(**kwargs)
            return invoices_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("shippingLabels"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = datetime.strptime(
                    self.config.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                return self.load_order_page(
                    mp="ATVPDKIKX0DER", CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    # createdBefore=end_date,
                    # createdAfter=start_date,
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class VendorPurchaseOrdersStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_purchase_orders"
    primary_keys = ["purchaseOrderNumber"]
    replication_key = "purchaseOrderStateChangedDate"
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"
    next_token = None

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("purchaseOrderState", th.StringType),
        # Optional, not always populated
        th.Property("orderDetails", th.CustomType({"type": ["object", "string"]})),
        th.Property("deliveryWindow", th.StringType),
        th.Property("items", th.CustomType({"type": ["array", "string"]})),
        th.Property("purchaseOrderStateChangedDate", th.DateTimeType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    @load_all_pages()
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_vendor(mp)
            if self.next_token is not None:
                kwargs.update({"nextToken": self.next_token})
            orders_obj = orders.get_purchase_orders(**kwargs)
            return orders_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            self.next_token = page.next_token
            for order in page.payload.get("orders", []):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = datetime.strptime(
                    self.config.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                return self.load_order_page(
                    mp="ATVPDKIKX0DER", CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    createdAfter=start_date,
                    limit=100,
                    SortOrder="DESC",
                )
            for row in rows:
                for item in row:
                    order_details = item.get("orderDetails", {})
                    if order_details.get("purchaseOrderStateChangedDate"):
                        item.update(
                            {
                                "purchaseOrderStateChangedDate": order_details.get(
                                    "purchaseOrderStateChangedDate"
                                )
                            }
                        )
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class VendorsReportStream(AmazonSellerStream):
    """Define custom stream."""

    lookback_days = 1460
    correct_end_date_minus_days = 2
    products_context = []
    selling_programs = []
    current_selling_program = None

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def primary_keys(self):
        pass

    @abstractproperty
    def primary_keys(self):
        pass

    @abstractproperty
    def schema(self):
        pass

    @abstractproperty
    def report_name(self):
        pass

    @abstractproperty
    def report_options(self):
        pass

    def correct_end_date(self, end_date, start_date, current_date):
        if end_date > current_date:
            # If end_date is greater than today then fetch report for yesterday.
            end_date = current_date - timedelta(days=self.correct_end_date_minus_days)

        if end_date <= start_date:
            end_date = start_date
        return end_date

    def format_end_date(self, end_date):
        return end_date.strftime("%Y-%m-%dT23:59:59")

    def get_current_datetime(self):
        return datetime.now()

    def get_start_date_formatted(self, start_date):
        return start_date.strftime("%Y-%m-%dT00:00:00")

    @property
    def distributor_report_options(self) -> dict:
        report_options = {
            "reportPeriod": "DAY",
            "sellingProgram": "RETAIL",
            "distributorView": "MANUFACTURING"
        }
        return report_options
    
    @property
    def distributor_report_options_sourcing(self) -> dict:
        report_options = {
            "reportPeriod": "DAY",
            "sellingProgram": "RETAIL",
            "distributorView": "SOURCING"
        }
        return report_options

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:

            start_date = self.get_starting_timestamp(context)
            if start_date:
                # Remove timezone info from replication date so we can compare it with other dates.
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = datetime.strptime(
                    self.config.get("start_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            current_date = self.get_current_datetime()
            global_end_date = current_date
            if self.config.get("end_date"):
                global_end_date = datetime.strptime(
                    self.config.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )

            minimum_start_date = current_date - timedelta(days=self.lookback_days)
            if start_date < minimum_start_date:
                # Reset start date to days limit if it is greater than 1460 days
                start_date = current_date - timedelta(days=self.lookback_days)

            end_date = start_date + timedelta(days=14)
            end_date = self.correct_end_date(end_date, start_date, current_date)

            report_types = [self.report_name]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces

            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            while start_date <= current_date and start_date <= global_end_date:
                start_date_f = self.get_start_date_formatted(start_date)
                end_date_f = self.format_end_date(end_date)
                report_options = self.report_options
                if self.current_selling_program:
                    report_options.update({"sellingProgram": self.current_selling_program})
                #Process only reports created by the tap
                self.logger.info(f"Creating new report. StartDate:{start_date_f}, EndDate: {end_date_f}, ReportName:{self.report_name}, ReportOptions: {report_options}")
                reports = self.create_report(
                    report,
                    start_date_f,
                    end_date_f,
                    self.report_name,
                    reportOptions=report_options,
                    report_type="json",
                )
                for row in reports:
                    row.update({"report_end_date": end_date.isoformat()})
                    row = self.post_process(row,context)
                    yield row
                
                # Move to the next time period
                start_date = end_date + timedelta(days=1)
                end_date += timedelta(days=14)
                end_date = self.correct_end_date(end_date, start_date, current_date)

        except Exception as e:
            raise InvalidResponse(e)


class VendorsSalesReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_sales_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_SALES_REPORT"
    selling_programs = ['BUSINESS','FRESH','RETAIL']

    @property
    def report_options(self) -> dict:
        return self.distributor_report_options

    correct_end_date_minus_days = 3
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("salesAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("salesByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()
    def get_max_date(self,data,date_key="endDate"):
        max_date_dict = max(data, key=lambda x: x[date_key])
        max_date_str = max_date_dict["endDate"]
        # Convert the maximum date to ISO date format
        max_date_iso = datetime.strptime(max_date_str, "%Y-%m-%d").date().isoformat()
        return max_date_iso

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        #For newly added sellingProgram(Fresh) this property is coming as empty list.
        if row.get("salesByAsin"):
            row['report_end_date'] = self.get_max_date(row.get("salesByAsin")) 
        return row
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Override the get_records function so we could yield all of sellingProgram type report for each time period
        """
        for program in self.selling_programs:
            # Reset current selling program so it could be used by the parent function when creating the report.
            self.current_selling_program = program
            yield from super().get_records(context)


class VendorsTrafficReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_traffic_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_TRAFFIC_REPORT"
    report_options = {"reportPeriod": "DAY"}
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("trafficAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("trafficByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()


class VendorsForecastingReportStream(VendorsReportStream):
    """Report stream for Forecasting report
       URL: https://developer-docs.amazon.com/sp-api/docs/report-type-values-analytics#vendor-forecasting-report
    """

    name = "vendor_forecasting_report"
    primary_keys = None
    replication_key = None
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_FORECASTING_REPORT"
    #Leaving it here we need to add support for iterating through selling programs
    selling_programs = ['RETAIL','FRESH'] 
    report_options = {"sellingProgram": "RETAIL"}
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("forecastByAsin", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:

            report_types = [self.report_name]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces

            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            items = self.get_reports_list(report, report_types, processing_status)

            if not items["reports"]:
                self.logger.info(f"Creating new report. ReportName:{self.report_name}, ReportOptions: {self.report_options}")
                reports = self.create_report(
                    reports=report,
                    type=self.report_name,
                    reportOptions=self.report_options,
                    report_type="json",
                )
                for row in reports:
                    yield row

            # If reports are form loop through, download documents and populate the data.txt
            for row in items["reports"]:
                self.logger.info(f"Pre-existing report of type: {self.report_name} found. Processing...")
                reports = self.check_report(row["reportId"], report, "json")
                for report_row in reports:
                    self.logger.info(f"Processing pre-existing report row: {report_row}")
                    yield report_row

        except Exception as e:
            raise InvalidResponse(e)


class VendorsSalesRealtimeReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_sales_realtime_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_REAL_TIME_SALES_REPORT"
    lookback_days = 13

    @property
    def report_options(self) -> dict:
        #Changing sellerProgram has no affect on this report it is ignored when creating a report.
        return self.distributor_report_options

    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("reportData", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    def get_current_datetime(self):
        current_time = datetime.utcnow()
        target_timezone = pytz.timezone("America/New_York")
        converted_time = current_time.astimezone(target_timezone)
        return converted_time.now()

    def format_end_date(self, end_date):
        today = datetime.today().date()
        end_date_f = end_date.date()
        if end_date_f == today:
            # Get everything until start of today.
            return end_date.strftime("%Y-%m-%dT00:06:59")
        return end_date.strftime("%Y-%m-%dT23:59:59")

    def get_start_date_formatted(self, start_date):
        today = datetime.today().date()
        start_date_f = start_date.date()
        if start_date_f == today:
            start_date = start_date - timedelta(days=1)
            # Get everything until start of today.
            return start_date.strftime("%Y-%m-%dT23:00:00")
        return start_date.strftime("%Y-%m-%dT00:00:00")


class VendorsInventoryRealtimeReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_inventory_realtime_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_REAL_TIME_INVENTORY_REPORT"
    lookback_days = 6

    @property
    def report_options(self) -> dict:
        return self.distributor_report_options

    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("reportData", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    def format_end_date(self, end_date):
        today = datetime.today().date()
        end_date_f = end_date.date()
        if end_date_f == today:
            # Get everything until start of today.
            return end_date.strftime("%Y-%m-%dT00:06:59")
        return end_date.strftime("%Y-%m-%dT23:59:59")

    def get_start_date_formatted(self, start_date):
        today = datetime.today().date()
        start_date_f = start_date.date()
        if start_date_f == today:
            start_date = start_date - timedelta(days=1)
            # Get everything until start of today.
            return start_date.strftime("%Y-%m-%dT23:00:00")
        return start_date.strftime("%Y-%m-%dT00:00:00")


class VendorsTrafficRealtimeReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_traffic_realtime_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_REAL_TIME_TRAFFIC_REPORT"
    report_options = {"reportPeriod": "DAY"}
    lookback_days = 13
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("reportData", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()
    # Need to add one more class to inherit from to make code DRY.
    def format_end_date(self, end_date):
        today = datetime.today().date()
        end_date_f = end_date.date()
        if end_date_f == today:
            # Get everything until start of today.
            return end_date.strftime("%Y-%m-%dT00:06:59")
        return end_date.strftime("%Y-%m-%dT23:59:59")

    def get_start_date_formatted(self, start_date):
        today = datetime.today().date()
        start_date_f = start_date.date()
        if start_date_f == today:
            start_date = start_date - timedelta(days=1)
            # Get everything until start of today.
            return start_date.strftime("%Y-%m-%dT23:00:00")
        return start_date.strftime("%Y-%m-%dT00:00:00")


class VendorsInventoryReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_inventory_report"
    primary_keys = None
    #No replication key here because we might miss updated products. The updates in products does not guarantee update in the inventory 
    replication_key = "report_end_date" 
    report_id = None
    document_id = None
    products = []
    report_name = "GET_VENDOR_INVENTORY_REPORT"
    correct_end_date_minus_days = 3
    #Leaving it here we need to add support for iterating through selling programs
    selling_programs = ['RETAIL','FRESH']

    @property
    def report_options(self) -> dict:
        return self.distributor_report_options

    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("inventoryAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("inventoryByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    def get_marketplace_code(self, marketplace_id):
        for marketplace in Marketplaces:
            if marketplace_id == marketplace.marketplace_id:
                return marketplace.name

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        marketplace_id = None

        if record.get("reportSpecification"):
            if record["reportSpecification"].get("marketplaceIds"):
                if len(record["reportSpecification"]["marketplaceIds"]) > 0:
                    marketplace_id = record["reportSpecification"]["marketplaceIds"][0]
                    marketplace_id = self.get_marketplace_code(marketplace_id)
        self.products = record["inventoryByAsin"]
        data = {
            "products": record["inventoryByAsin"],
            "marketplace_id": str(marketplace_id),
        }
        # store the context data in a parent class variable
        VendorsReportStream.products_context = data
        return {"marketplace_id": str(marketplace_id)}

    def get_products(self):
        return self.products

    # def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:

    #     with open("./vendor_inventory_report_sample.json") as file:
    #         data = json.load(file)

    #     data = [data]
    #     for row in data:
    #         yield row


class InventoryProductsListStream(VendorsReportStream):
    """Define custom stream."""

    name = "inventory_product_list"
    primary_keys = None
    report_id = None
    document_id = None
    previous_items = []
    replication_key = None
    parent_stream_type = VendorsInventoryReportStream
    ignore_parent_replication_key = True
    report_name = "GET_VENDOR_INVENTORY_REPORT"
    report_options = {}

    schema = th.PropertiesList(
        th.Property("asin", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"ASIN": record["asin"], "marketplace_id": context.get("marketplace_id")}

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        records = None
        content = VendorsReportStream.products_context

        if content.get("products"):
            records = content["products"]
        for record in records:
            transformed_record = self.post_process(record, context)
            if transformed_record["asin"] in self.previous_items:
                continue
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            self.previous_items.append(transformed_record["asin"])
            yield transformed_record.copy()

    def get_timestamp_for_files(self):
        return (
            datetime.now()
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "")
            .replace("-", "")
            .replace(":", "")
        )

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        # We do
        context.update({"products": [self.get_timestamp_for_files()]})
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                increment_state(
                    state_dict,
                    replication_key=self.replication_key,
                    latest_record=latest_record,
                    is_sorted=treat_as_sorted,
                )


class ProductDetails(AmazonSellerStream):
    """Define custom stream."""

    name = "product_details"
    primary_keys = ["ASIN"]
    replication_key = None
    asin = "{ASIN}"
    parent_stream_type = InventoryProductsListStream
    ignore_parent_replication_key = True
    schema = th.PropertiesList(
        th.Property("asin", th.StringType),
        th.Property("attributes", th.CustomType({"type": ["object", "string"]})),
        th.Property("identifiers", th.CustomType({"type": ["array", "string"]})),
        th.Property("productTypes", th.CustomType({"type": ["array", "string"]})),
        th.Property("ranks", th.CustomType({"type": ["array", "string"]})),
        th.Property("salesRanks", th.CustomType({"type": ["array", "string"]})),
        th.Property("summaries", th.CustomType({"type": ["array", "string"]})),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            includedData = ["attributes,summaries,identifiers,productTypes,salesRanks"]
            # if context is not None:
            asin = context.get("ASIN")
            catalog = self.get_sp_catalog(marketplace_id=context.get("marketplace_id"))
            if context.get("marketplace_id") == "JP":
                items = catalog.get_catalog_item(JAN=asin).payload
            elif context.get("marketplace_id") in ["FR"]:
                items = catalog.get_catalog_item(EAN=asin).payload
            else:
                items = catalog.get_catalog_item(
                    asin=asin, includedData=includedData
                ).payload
            if "Items" in items:
                if len(items["Items"]) > 0:
                    items = items["Items"][0]

            items.update({"marketplace_id": context.get("marketplace_id")})
            return [items]
            # else:
            #     return []
        except Exception as e:
            raise InvalidResponse(e)

class VendorsSalesSourcingReportStream(VendorsSalesReportStream):
    """Define custom stream."""

    name = "vendor_sales_sourcing_report"

    @property
    def report_options(self) -> dict:
        return self.distributor_report_options_sourcing

class VendorsInventorySourcingReportStream(VendorsInventoryReportStream):
    """Define custom stream."""

    name = "vendor_inventory_sourcing_report"
    products = []

    @property
    def report_options(self) -> dict:
        return self.distributor_report_options_sourcing   

class InventoryProductsSourcingListStream(InventoryProductsListStream):
    
    name = "inventory_product_sourcing_list"
    previous_items = []
    parent_stream_type = VendorsInventorySourcingReportStream
    report_options = {}     