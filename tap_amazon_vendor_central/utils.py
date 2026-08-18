import signal
import time
from datetime import datetime, timedelta
from sp_api.base.marketplaces import Marketplaces
from typing import List


class Timeout(Exception):
    def __init__(self, value="Timed Out"):
        self.value = value

    def __str__(self):
        return repr(self.value)


class InvalidResponse(Exception):
    pass


def timeout(seconds_before_timeout):
    def decorate(f):
        def handler(signum, frame):
            raise Timeout()

        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            old_time_left = signal.alarm(seconds_before_timeout)
            if 0 < old_time_left < seconds_before_timeout:
                signal.alarm(old_time_left)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
            finally:
                if old_time_left > 0:
                    old_time_left -= time.time() - start_time
                signal.signal(signal.SIGALRM, old)
                signal.alarm(old_time_left)
            return result

        new_f.__name__ = f.__name__
        return new_f

    return decorate

# "label": "Amazon Seller Marketplace Region",
# "description": "The region of the Amazon Seller marketplace to connect",
VALID_URIS = [
          {
            "name": "CA",
            "value": "https://vendorcentral.amazon.ca"
          },
          {
            "name": "US",
            "value": "https://vendorcentral.amazon.com"
          },
          {
            "name": "MX",
            "value": "https://vendorcentral.amazon.com.mx"
          },
          {
            "name": "BR",
            "value": "https://vendorcentral.amazon.com.br"
          },
          {
            "name": "ES",
            "value": "https://vendorcentral.amazon.es"
          },
          {
            "name": "UK",
            "value": "https://vendorcentral.amazon.co.uk"
          },
          {
            "name": "FR",
            "value": "https://vendorcentral.amazon.fr"
          },
          {
            "name": "BE",
            "value": "https://vendorcentral.amazon.com.be"
          },
          {
            "name": "NL",
            "value": "https://vendorcentral.amazon.nl"
          },
          {
            "name": "DE",
            "value": "https://vendorcentral.amazon.de"
          },
          {
            "name": "IT",
            "value": "https://vendorcentral.amazon.it"
          },
          {
            "name": "SE",
            "value": "https://vendorcentral.amazon.se"
          },
          {
            "name": "PL",
            "value": "https://vendorcentral.amazon.pl"
          },
          {
            "name": "EG",
            "value": "https://vendorcentral.amazon.me"
          },
          {
            "name": "TR",
            "value": "https://vendorcentral.amazon.com.tr"
          },
          {
            "name": "AE",
            "value": "https://vendorcentral.amazon.me"
          },
          {
            "name": "IN",
            "value": "https://vendorcentral.amazon.in"
          },
          {
            "name": "SG",
            "value": "https://vendorcentral.amazon.com.sg"
          },
          {
            "name": "AU",
            "value": "https://vendorcentral.amazon.com.au"
          },
          {
            "name": "JP",
            "value": "https://vendorcentral.amazon.co.jp"
          }
        ]

def get_country_code_from_uri(uri: str) -> str:
    for country_code in VALID_URIS:
        if country_code["value"] == uri:
            return country_code["name"]
    return None

def get_valid_marketplaces_from_uri(uri: str) -> List[str]:
    country_code = get_country_code_from_uri(uri)
    if country_code in Marketplaces.__members__:  # Ensure the name exists in the enum
        try:
            region = Marketplaces[country_code].value[2]
        except Exception as e:
            raise ValueError(f"Country {country_code} doesn't have a valid region in AWS Marketplaces list.")
    else:
        raise ValueError(f"Country code {country_code} is not a valid country in AWS Marketplaces list.")
        
    valid_marketplaces = []
    for marketplace in Marketplaces:
        if marketplace.value[2] == region:
            valid_marketplaces.append(marketplace.name)
    return valid_marketplaces


def align_to_week_start(dt: datetime) -> datetime:
    """Return the Sunday on or before dt (Amazon WEEK reportPeriod start)."""
    days_since_sunday = (dt.weekday() + 1) % 7
    return dt - timedelta(days=days_since_sunday)


def align_to_week_end(dt: datetime) -> datetime:
    """Return the Saturday on or before dt (Amazon WEEK reportPeriod end)."""
    days_since_saturday = (dt.weekday() - 5) % 7
    return dt - timedelta(days=days_since_saturday)
