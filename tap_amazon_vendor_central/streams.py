"""Stream type classes for tap-amazon-seller."""
from datetime import datetime, timedelta

from typing import Iterable, Optional

import backoff
from singer_sdk import typing as th
from sp_api.util import load_all_pages

from tap_amazon_vendor_central.client import AmazonSellerStream
from tap_amazon_vendor_central.utils import InvalidResponse, timeout
from sp_api.base.exceptions import SellingApiServerException
from dateutil.relativedelta import relativedelta
from sp_api.base import Marketplaces
from abc import abstractproperty

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


class ProductsIventoryStream(AmazonSellerStream):
    """Define custom stream."""

    name = "products_inventory"
    primary_keys = ["listing-id"]
    replication_key = None
    report_id = None
    document_id = None
    parent_stream_type = MarketplacesStream
    schema = th.PropertiesList(
        th.Property("marketplaceIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("item-name", th.StringType),
        th.Property("marketplace_id", th.StringType),
        th.Property("item-description", th.StringType),
        th.Property("listing-id", th.StringType),
        th.Property("seller-sku", th.StringType),
        th.Property("price", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("open-date", th.StringType),
        th.Property("image-url", th.StringType),
        th.Property("item-is-marketplace", th.StringType),
        th.Property("product-id-type", th.StringType),
        th.Property("zshop-shipping-fee", th.StringType),
        th.Property("item-note", th.StringType),
        th.Property("item-condition", th.StringType),
        th.Property("zshop-category1", th.StringType),
        th.Property("zshop-browse-path", th.StringType),
        th.Property("asin1", th.StringType),
        th.Property("asin2", th.StringType),
        th.Property("asin3", th.StringType),
        th.Property("will-ship-internationally", th.StringType),
        th.Property("zshop-boldface", th.StringType),
        th.Property("product-id", th.StringType),
        th.Property("bid-for-featured-placement", th.StringType),
        th.Property("add-delete", th.StringType),
        th.Property("pending-quantity", th.StringType),
        th.Property("fulfilment-channel", th.StringType),
        th.Property("merchant-shipping-group", th.StringType),
        th.Property("status", th.StringType),
        th.Property("Minimum order quantity", th.StringType),
        th.Property("Sell remainder", th.StringType),
        th.Property("product-id", th.StringType),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        if "asin1" in record:
            return {
                "ASIN": record["asin1"],
                "marketplace_id": context.get("marketplace_id"),
            }
        elif "product-id" in record:
            return {
                "ASIN": record["product-id"],
                "marketplace_id": context.get("marketplace_id"),
            }
        else:
            return []

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            end_date = None
            if self.config.get("start_date"):
                start_date = datetime.strptime(
                    self.config.get("start_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            if self.config.get("end_date"):
                end_date = datetime.strptime(
                    self.config.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            start_date = start_date.strftime("%Y-%m-%dT00:00:00")
            report_types = ["GET_MERCHANT_LISTINGS_ALL_DATA"]
            processing_status = self.config.get("processing_status")
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            if start_date and end_date is not None:
                end_date = end_date.strftime("%Y-%m-%dT23:59:59")
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                    dataEndTime=end_date,
                ).payload
            else:
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                ).payload

            if not items["reports"]:
                reports = self.create_report(
                    start_date, report, end_date, "GET_MERCHANT_LISTINGS_ALL_DATA"
                )
                for row in reports:
                    yield row

            # If reports are form loop through, download documents and populate the data.txt
            for row in items["reports"]:
                reports = self.check_report(row["reportId"], report)
                for report_row in reports:
                    if context is not None:
                        report_row.update(
                            {marketplace_id: context.get("marketplace_id")}
                        )
                    yield report_row

        except Exception as e:
            raise InvalidResponse(e)


class ProductDetails(AmazonSellerStream):
    """Define custom stream."""

    name = "product_details"
    primary_keys = ["ASIN"]
    replication_key = None
    asin = "{ASIN}"
    parent_stream_type = ProductsIventoryStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("ASIN", th.StringType),
        th.Property("Identifiers", th.CustomType({"type": ["object", "string"]})),
        th.Property("AttributeSets", th.CustomType({"type": ["array", "string"]})),
        th.Property("Relationships", th.CustomType({"type": ["array", "string"]})),
        th.Property("SalesRankings", th.CustomType({"type": ["array", "string"]})),
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
            # if context is not None:
            asin = context.get("ASIN")
            catalog = self.get_sp_catalog(context.get("marketplace_id"))
            if context.get("marketplace_id") == "JP":
                items = catalog.list_items(JAN=asin).payload
            elif context.get("marketplace_id") in ["FR"]:
                items = catalog.list_items(EAN=asin).payload
            else:
                items = catalog.get_item(asin=asin).payload
            if "Items" in items:
                if len(items["Items"]) > 0:
                    items = items["Items"][0]
            items.update({"ASIN": asin})
            items.update({"marketplace_id": context.get("marketplace_id")})
            return [items]
            # else:
            #     return []
        except Exception as e:
            raise InvalidResponse(e)


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
                    order_details = item.get("orderDetails",{})
                    if order_details.get("purchaseOrderStateChangedDate"):
                        item.update({"purchaseOrderStateChangedDate":order_details.get("purchaseOrderStateChangedDate")})
                    yield item
        except Exception as e:
            raise InvalidResponse(e)

class VendorsReportStream(AmazonSellerStream):
    """Define custom stream."""
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
    def correct_end_date(self,end_date,start_date,current_date):
        if end_date>current_date:
            #If end_date is greater than today then fetch report for yesterday.
            end_date = current_date - timedelta(days=2)

        if end_date <= start_date:
            end_date = start_date    
        return end_date    

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
                #Remove timezone info from replication date so we can compare it with other dates.
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = datetime.strptime(
                    self.config.get("start_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            current_date = datetime.now()
            minimum_start_date = current_date - timedelta(days=1460)
            if start_date < minimum_start_date:
                #Reset start date to days limit if it is greater than 1460 days
                start_date = current_date - timedelta(days=1460)
        
            end_date = start_date + timedelta(days=14)
            end_date = self.correct_end_date(end_date,start_date,current_date)
            
            report_types = [self.report_name]
            processing_status = self.config.get("processing_status")
            #Get list of valid marketplaces
            
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")
           
           
            report = self.get_sp_reports(marketplace_id=marketplace_id)
            while start_date <= current_date:
                start_date_f = start_date.strftime("%Y-%m-%dT00:00:00")
                end_date_f = end_date.strftime("%Y-%m-%dT23:59:59")
                items = self.get_reports_list(report,report_types,processing_status,start_date_f,end_date_f)
                
                if not items["reports"]:
                    reports = self.create_report(
                        report, start_date_f,  end_date_f, self.report_name,
                        reportOptions=self.report_options,
                        report_type="json"
                    )
                    for row in reports:
                        row.update({"report_end_date":end_date.isoformat()})
                        yield row

                # If reports are form loop through, download documents and populate the data.txt
                for row in items["reports"]:
                    reports = self.check_report(row["reportId"], report,"json")
                    for report_row in reports:
                        # if context is not None:
                        report_row.update({"report_end_date":end_date.isoformat()})
                        yield report_row
                # Move to the next time period
                start_date = end_date + timedelta(days=1)
                end_date += timedelta(days=14)
                end_date = self.correct_end_date(end_date,start_date,current_date)

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
    report_options = {"reportPeriod": "DAY","sellingProgram": "RETAIL","distributorView": "MANUFACTURING"}
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property("reportSpecification", th.CustomType({"type": ["object", "string"]})),
        th.Property("salesAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("salesByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

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
        th.Property("reportSpecification", th.CustomType({"type": ["object", "string"]})),
        th.Property("trafficAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("trafficByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

class VendorsInventoryReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_inventory_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_INVENTORY_REPORT"
    report_options = {"reportPeriod": "DAY","sellingProgram": "RETAIL","distributorView": "MANUFACTURING"}
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property("reportSpecification", th.CustomType({"type": ["object", "string"]})),
        th.Property("inventoryAggregate", th.CustomType({"type": ["array", "string"]})),
        th.Property("inventoryByAsin", th.CustomType({"type": ["array", "string"]})),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

class VendorsForecastingReportStream(VendorsReportStream):
    """Define custom stream."""

    name = "vendor_forecasting_report"
    primary_keys = None
    replication_key = None
    report_id = None
    document_id = None
    report_name = "GET_VENDOR_FORECASTING_REPORT"
    report_options = {"sellingProgram": "RETAIL"}
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property("reportSpecification", th.CustomType({"type": ["object", "string"]})),
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
            #Get list of valid marketplaces
            
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")
           
           
            report = self.get_sp_reports(marketplace_id=marketplace_id)
            items = self.get_reports_list(report,report_types,processing_status)
            
            if not items["reports"]:
                reports = self.create_report(
                    reports=report,
                    type=self.report_name,
                    reportOptions=self.report_options,
                    report_type="json"
                )
                for row in reports:
                    yield row

            # If reports are form loop through, download documents and populate the data.txt
            for row in items["reports"]:
                reports = self.check_report(row["reportId"], report,"json")
                for report_row in reports:
                    yield report_row

        except Exception as e:
            raise InvalidResponse(e)
