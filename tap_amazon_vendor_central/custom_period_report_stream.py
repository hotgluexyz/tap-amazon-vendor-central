"""Custom report stream for dynamic report generation."""

import calendar
from datetime import datetime, timedelta
import backoff
from dateutil.parser import parse
from singer_sdk import typing as th
from tap_amazon_vendor_central.client import AmazonSellerStream
from tap_amazon_vendor_central.exceptions import InvalidMarketplace, ReportNotAvailable
from tap_amazon_vendor_central.streams import MarketplacesStream

class CustomPeriodReportStream(AmazonSellerStream):
    """Custom report stream that can be configured dynamically via report_config."""

    lookback_days = 1460
    correct_end_date_minus_days = 2
    products_context = []
    selling_programs = []
    current_selling_program = None

    parent_stream_type = MarketplacesStream
    
    def __init__(self, tap, report_config):
        # Set report_config first since name property depends on it
        self.report_config = report_config
        
        # Set attributes that Singer SDK expects
        self.primary_keys = report_config.get("primary_keys", None)
        self.replication_key = report_config.get("replication_key", "report_end_date")
        self.report_id = None
        
        # Now call parent init
        super().__init__(tap)
        
    @property
    def name(self):
        """Return the stream name, generating it if needed."""
        if not hasattr(self, '_name') or self._name is None:
            self._name = self.report_config["name"]
        return self._name

    @property
    def report_name(self):
        return self.report_config.get("report_name")
    
    @property
    def report_period(self):
        if not hasattr(self, '_report_period') or self._report_period is None:
            self._report_period = self.report_config.get("period", "WEEK")
        return self._report_period
    
    @property
    def report_options(self):
        if not hasattr(self, '_report_options') or self._report_options is None:
            base_options = self.report_config.get("report_options", {})
            self._report_options = {**base_options, "reportPeriod": self.report_period}
        return self._report_options

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
        giveup=lambda e: isinstance(e, InvalidMarketplace) 
    )
    def get_records(self, context):
        """Override get_records to use period-aware date validation."""
        # Get the standard date range from parent
        start_date = self.get_starting_timestamp(context)
        if start_date:
            start_date = start_date.replace(tzinfo=None)
        
        end_date = None
        if self.config.get("start_date") and not start_date:
            start_date = parse(self.config.get("start_date"))
            start_date = start_date.replace(tzinfo=None)
        
        current_date = self.get_current_datetime()
        global_end_date = current_date
        if self.config.get("end_date"):
            global_end_date = parse(self.config.get("end_date"))
            global_end_date = global_end_date.replace(tzinfo=None)

        minimum_start_date = current_date - timedelta(days=self.lookback_days)
        if start_date < minimum_start_date:
            start_date = current_date - timedelta(days=self.lookback_days)

        start_date = self.get_period_start_date(start_date, self.report_period)
        end_date = self.get_period_end_date(start_date, self.report_period)
        # Calculate proper end_date based on the report period
        end_date = self.correct_end_date(end_date, start_date, current_date)

        marketplace_id = None
        if context is not None:
            marketplace_id = context.get("marketplace_id")

        report = self.get_sp_reports(marketplace_id=marketplace_id)

        report_available = True
        
        while start_date <= current_date and start_date <= global_end_date:
            start_date_f = self.get_start_date_formatted(start_date)
            end_date_f = self.format_end_date(end_date)
            report_options = self.report_options.copy()
            
            if self.current_selling_program:
                report_options.update({"sellingProgram": self.current_selling_program})
            
            self.logger.info(
                f"Creating report {self.report_name} for period {self.report_period}. "
                f"StartDate: {start_date_f}, EndDate: {end_date_f}, "
                f"ReportOptions: {report_options}, marketplace_id: {marketplace_id}"
            )
            try:
                reports = self.create_report(
                    report,
                    start_date_f,
                    end_date_f,
                    self.report_name,
                    reportOptions=report_options,
                    report_type="json",
                    marketplace_id=marketplace_id
                )
            except ReportNotAvailable:
                report_available = False
                self.logger.info(f"No reports created for period {start_date_f} to {end_date_f}. Decreasing end date by 1 day.")
                end_date -= timedelta(days=1)
                if start_date > end_date:
                    self.logger.info(f"Start date {start_date} is greater than end date {end_date}. Breaking out of loop.")
                    break
                end_date = self.correct_end_date(end_date, start_date, current_date)
                continue

            if reports:
                for row in reports:
                    row.update({"report_end_date": end_date.isoformat()})
                    row = self.post_process(row, context)
                    yield row
            else:
                self.logger.info(f"No report available for period {start_date_f} to {end_date_f}")
                    
            if report_available:
                # Move to the next time period based on the report period
                start_date = self.get_next_period_start(start_date, self.report_period)
                period_end_date = self.get_period_end_date(start_date, self.report_period)
                end_date = self.correct_end_date(period_end_date, start_date, current_date)
            else:
                break
            
            # if end date is today and report_period is Day then break the loop
            if self.report_period == "DAY" and end_date == current_date:
                start_date_f = self.get_start_date_formatted(start_date)
                self.logger.info(f"Skipping incomplete {self.report_period} period: {start_date_f}")
                break

            if self.report_period in ["WEEK", "MONTH", "QUARTER"] and end_date < period_end_date:
                start_date_f = self.get_start_date_formatted(start_date)
                end_date_f = self.format_end_date(period_end_date)
                self.logger.info(f"Skipping incomplete {self.report_period} period: {start_date_f} to {end_date_f}")
                break

    @property
    def schema(self):
        """Generate schema from report config or use default."""
        if "schema" in self.report_config:
            return self.report_config["schema"]
        
        raise ValueError(f"Schema not found for {self.name}")
        
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

    def get_period_start_date(self, date, period):
        """Get the correct start date for a given period type."""
        if period == "DAY":
            return date
        elif period == "WEEK":
            # Move to the previous Sunday (start of week)
            days_since_sunday = date.weekday() + 1 if date.weekday() != 6 else 0
            return date - timedelta(days=days_since_sunday)
        elif period == "MONTH":
            # Move to the first day of the month
            return date.replace(day=1)
        elif period == "QUARTER":
            # Move to the first day of the quarter
            quarter_start_month = ((date.month - 1) // 3) * 3 + 1
            return date.replace(month=quarter_start_month, day=1)
        else:
            raise ValueError(f"Unsupported period type: {period}")

    def get_period_end_date(self, date, period):
        """Get the correct end date for a given period type."""
        if period == "DAY":
            return date
        elif period == "WEEK":
            # Move to the next Saturday (end of week)
            days_until_saturday = 5 - date.weekday() if date.weekday() != 6 else 6
            return date + timedelta(days=days_until_saturday)
        elif period == "MONTH":
            # Move to the last day of the month
            last_day = calendar.monthrange(date.year, date.month)[1]
            return date.replace(day=last_day)
        elif period == "QUARTER":
            # Move to the last day of the quarter
            quarter_start_month = ((date.month - 1) // 3) * 3 + 1
            quarter_end_month = quarter_start_month + 2
            if quarter_end_month > 12:
                quarter_end_month = 12
            last_day = calendar.monthrange(date.year, quarter_end_month)[1]
            return date.replace(month=quarter_end_month, day=last_day)
        else:
            raise ValueError(f"Unsupported period type: {period}")

    def get_next_period_start(self, date, period):
        """Get the start date of the next period."""
        if period == "DAY":
            return date + timedelta(days=1)
        elif period == "WEEK":
            # Next Sunday
            return date + timedelta(days=7)
        elif period == "MONTH":
            # First day of next month
            if date.month == 12:
                return date.replace(year=date.year + 1, month=1, day=1)
            else:
                return date.replace(month=date.month + 1, day=1)
        elif period == "QUARTER":
            # First day of next quarter
            current_quarter = ((date.month - 1) // 3) + 1
            if current_quarter == 4:
                return date.replace(year=date.year + 1, month=1, day=1)
            else:
                next_quarter_start_month = current_quarter * 3 + 1
                return date.replace(month=next_quarter_start_month, day=1)
        else:
            raise ValueError(f"Unsupported period type: {period}")

# Report configuration templates
REPORT_CONFIGS = {
    "NetPPM": {
        "name": "vendor_net_pure_product_margin_report",
        "report_name": "GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
        "schema": th.PropertiesList(
            th.Property("reportSpecification", th.CustomType({"type": ["object", "string"]})),
            th.Property("netPureProductMarginAggregate", th.CustomType({"type": ["array", "string"]})),
            th.Property("netPureProductMarginByAsin", th.CustomType({"type": ["array", "string"]})),
            th.Property("report_end_date", th.DateTimeType),
            th.Property("marketplace_id", th.StringType),
        ).to_dict(),
        "replication_key": "report_end_date"
    }
}

report_suffix = {
    "DAY": "daily",
    "WEEK": "weekly",
    "MONTH": "monthly",
    "QUARTER": "quarterly"
}

def create_report_config(report_type, period):
    """Create a complete report config from a report type and period."""
    if report_type not in REPORT_CONFIGS:
        raise ValueError(f"Unsupported report type: {report_type}. Available: {list(REPORT_CONFIGS.keys())}")
    
    base_config = REPORT_CONFIGS[report_type].copy()
    name = base_config.get("name")
    if name:
        name = f"{name}_{report_suffix[period]}"
        base_config["name"] = name

    config = {
        "period": period,
        **base_config,
    }
    return config
