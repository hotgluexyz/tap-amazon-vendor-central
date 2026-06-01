class InvalidMarketplace(Exception):
    pass

class ReportNotAvailable(Exception):
    pass

class PermissionError(Exception):
    pass

class InvalidReportParameter(Exception):
    pass


# Exception types that indicate a permanent, non-retryable report failure.
# Used as the giveup condition in all report backoff decorators.
REPORT_GIVEUP_EXCEPTIONS = (InvalidMarketplace, ReportNotAvailable, PermissionError, InvalidReportParameter)


def report_giveup(e: Exception) -> bool:
    return isinstance(e, REPORT_GIVEUP_EXCEPTIONS)