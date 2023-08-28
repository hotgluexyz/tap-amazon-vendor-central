"""Custom client handling, including Amazon-SellerStream base class."""


from typing import Any, List, Optional, cast

from singer_sdk.streams import Stream
from sp_api.api import (
    Finances,
    Inventories,
    Orders,
    ReportsV2,
    Reports,
    Catalog,
    VendorDirectFulfillmentOrders,
    VendorDirectFulfillmentShipping,
    VendorOrders,
    CatalogItems,
)
from sp_api.base import Marketplaces
import csv
import os
import time
from tap_amazon_vendor_central.utils import InvalidResponse
import json
import backoff

ROOT_DIR = os.environ.get("ROOT_DIR", ".")


def _find_in_partitions_list(
    partitions: List[dict], state_partition_context: dict
) -> Optional[dict]:
    found = [
        partition_state
        for partition_state in partitions
        if partition_state["context"] == state_partition_context
    ]
    if len(found) > 1:
        raise ValueError(
            f"State file contains duplicate entries for partition: "
            "{state_partition_context}.\n"
            f"Matching state values were: {str(found)}"
        )
    if found:
        return cast(dict, found[0])

    return None


def get_state_if_exists(
    tap_state: dict,
    tap_stream_id: str,
    state_partition_context: Optional[dict] = None,
    key: Optional[str] = None,
) -> Optional[Any]:
    if "bookmarks" not in tap_state:
        return None
    if tap_stream_id not in tap_state["bookmarks"]:
        return None

    skip_incremental_partitions = [
        "orderitems",
        "orderbuyerinfo",
        "orderaddress",
        "orderfinancialevents",
    ]
    stream_state = tap_state["bookmarks"][tap_stream_id]
    if tap_stream_id in skip_incremental_partitions and "partitions" in stream_state:
        # stream_state["partitions"] = []
        partitions = stream_state["partitions"][len(stream_state["partitions"]) - 1][
            "context"
        ]
        stream_state["partitions"] = [{"context": partitions}]

    if not state_partition_context:
        if key:
            return stream_state.get(key, None)
        return stream_state
    if "partitions" not in stream_state:
        return None  # No partitions defined

    matched_partition = _find_in_partitions_list(
        stream_state["partitions"], state_partition_context
    )
    if matched_partition is None:
        return None  # Partition definition not present
    if key:
        return matched_partition.get(key, None)
    return matched_partition


def get_state_partitions_list(
    tap_state: dict, tap_stream_id: str
) -> Optional[List[dict]]:
    """Return a list of partitions defined in the state, or None if not defined."""
    return (get_state_if_exists(tap_state, tap_stream_id) or {}).get("partitions", None)


class AmazonSellerStream(Stream):
    """Stream class for Amazon-Seller streams."""

    @property
    def partitions(self) -> Optional[List[dict]]:
        result: List[dict] = []
        for partition_state in (
            get_state_partitions_list(self.tap_state, self.name) or []
        ):
            result.append(partition_state["context"])
        if result is not None and len(result) > 0:
            result = [result[len(result) - 1]]
        return result or None

    def get_credentials(self):
        return dict(
            refresh_token=self.config.get("refresh_token"),
            lwa_app_id=self.config.get("lwa_client_id"),
            lwa_client_secret=self.config.get("client_secret"),
            aws_access_key=self.config.get("aws_access_key"),
            aws_secret_key=self.config.get("aws_secret_key"),
            role_arn=self.config.get("role_arn"),
        )

    def get_sp_orders(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return Orders(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def get_sp_finance(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return Finances(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def get_sp_reports(
        self,
        marketplace_id=None,
    ):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return ReportsV2(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def get_warehouse_object(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return Inventories(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=5,
    )
    def create_report(
        self,
        reports,
        start_date=None,
        end_date=None,
        type="GET_LEDGER_DETAIL_VIEW_DATA",
        reportOptions=None,
        report_type="csv",
    ):
        try:
            if start_date and end_date is not None:
                res = reports.create_report(
                    reportType=type,
                    dataStartTime=start_date,
                    dataEndTime=end_date,
                    reportOptions=reportOptions,
                ).payload
            elif start_date:
                res = reports.create_report(
                    reportType=type,
                    dataStartTime=start_date,
                    reportOptions=reportOptions,
                ).payload
            else:
                res = reports.create_report(
                    reportType=type, reportOptions=reportOptions
                ).payload

            if "reportId" in res:
                self.report_id = res["reportId"]
                return self.check_report(res["reportId"], reports, report_type)
        except Exception as e:
            raise InvalidResponse(e)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=5,
    )
    def get_report(self, report_id, reports):
        return reports.get_report(report_id)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=5,
    )
    def save_document(self, document_id, reports, report_type="csv"):
        res = reports.get_report_document(
            document_id,
            decrypt=True,
            file=f"{ROOT_DIR}/{document_id}_document.{report_type}",
            download=True,
        )
        self.reportDocumentId = document_id
        return res

    def read_csv(self, file):
        finalList = []
        file = f"{ROOT_DIR}/{file}"
        if os.path.isfile(file):
            with open(file, encoding="ISO-8859-1") as data:
                data_reader = csv.DictReader(data, delimiter="\t")
                for row in data_reader:
                    row["reportId"] = self.report_id
                    row = self.translate_report(row)
                    finalList.append(dict(row))
            # os.remove(file)
        return finalList

    def read_json(self, file):
        finalList = []
        file = f"{ROOT_DIR}/{file}"
        if os.path.isfile(file):
            with open(file) as data:
                data_reader = json.load(data)
                finalList = [data_reader]
            os.remove(file)
        return finalList

    def check_report(self, report_id, reports, report_type="csv"):
        res = []
        while True:
            report = self.get_report(report_id, reports).payload
            # Break the loop if the report processing is done
            if report["processingStatus"] == "DONE":
                document_id = report["reportDocumentId"]
                # save the document
                self.save_document(document_id, reports, report_type)
                if report_type == "csv":
                    res = self.read_csv(f"./{document_id}_document.{report_type}")
                else:
                    res = self.read_json((f"./{document_id}_document.{report_type}"))
                break
            elif report["processingStatus"] == "FATAL":
                self.logger.warning(
                    f"Report {report_id} failed with FATAL status. Skipping..."
                )
                break
            else:
                time.sleep(30)
                continue
        return res

    def get_sp_catalog(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return CatalogItems(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def translate_report(self, row):
        translate = {
            "\x8f¤\x95i\x96¼": "item-name",
            "\x8fo\x95iID": "listing-id",
            "\x8fo\x95i\x8eÒSKU": "seller-sku",
            "\x89¿\x8ai": "price",
            "\x90\x94\x97Ê": "quantity",
            "\x8fo\x95i\x93ú": "open-date",
            "\x8f¤\x95iID\x83^\x83C\x83v": "product-id-type",
            "\x8f¤\x95iID": "asin1",
            "\x83t\x83\x8b\x83t\x83B\x83\x8b\x83\x81\x83\x93\x83g\x81E\x83`\x83\x83\x83\x93\x83l\x83\x8b": "fulfilment-channel",
            "\x83X\x83e\x81[\x83^\x83X": "status",
            "\x8fo\x95i\x93ú": "open-date",
        }
        return_translated = False
        translated = {}
        for key in translate.keys():
            if key in row:
                return_translated = True
                translated[translate[key]] = row[key]
        if return_translated is True:
            return translated
        else:
            return row

    def get_sp_vendor_fulfilment(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return VendorDirectFulfillmentOrders(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def get_sp_vendor_fulfilment_shipping(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return VendorDirectFulfillmentShipping(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    def get_sp_vendor(self, marketplace_id=None):
        if marketplace_id is None:
            marketplace_id = self.config.get("marketplace", "US")
        return VendorOrders(
            credentials=self.get_credentials(), marketplace=Marketplaces[marketplace_id]
        )

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=5,
    )
    def get_reports_list(
        self,
        reports,
        report_types,
        processing_status,
        start_date_f=None,
        end_date_f=None,
    ):
        return reports.get_reports(
            reportTypes=report_types,
            processingStatuses=processing_status,
            dataStartTime=start_date_f,
            dataEndTime=end_date_f,
        ).payload
