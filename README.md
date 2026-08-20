# tap-amazon-vendor-central

`tap-amazon-vendor-central` is a Singer tap for [Amazon Vendor Central](https://vendorcentral.amazon.com/) via the [Selling Partner API](https://developer-docs.amazon.com/sp-api/). It is built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Installation

Clone the repo, create a virtual environment, and install with Poetry:

```bash
git clone https://github.com/hotgluexyz/tap-amazon-vendor-central.git
cd tap-amazon-vendor-central
python -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

## Configuration

| Field | Required | Description |
|---|---|---|
| `lwa_client_id` | Yes | Login with Amazon client ID |
| `client_secret` | Yes | Login with Amazon client secret |
| `refresh_token` | Yes | OAuth refresh token for the vendor account |
| `aws_access_key` | No | AWS access key for SP-API signing (if not using role-based auth) |
| `aws_secret_key` | No | AWS secret key for SP-API signing |
| `role_arn` | No | IAM role ARN to assume for SP-API requests |
| `sandbox` | No | Use the SP-API sandbox. Defaults to `false` |
| `marketplaces` | No | List of marketplace IDs to sync. When omitted, marketplaces are derived from `uri` |
| `uri` | No | Vendor Central portal URL for the account region (for example `https://vendorcentral.amazon.com`). Used to resolve marketplaces when `marketplaces` is not set |
| `report_types` | No | Legacy report type filter (defaults vary by stream) |
| `processing_status` | No | Report processing statuses to poll. Defaults to `IN_QUEUE`, `IN_PROGRESS` |

Example `config.json`:

```json
{
  "lwa_client_id": "amzn1.application-oa2-client.xxxx",
  "client_secret": "xxxx",
  "refresh_token": "Atzr|xxxx",
  "sandbox": false,
  "uri": "https://vendorcentral.amazon.com"
}
```

Run `tap-amazon-vendor-central --about` for the full JSON schema.

### Authentication

The tap uses Login with Amazon (LWA) plus AWS SigV4 signing against the Selling Partner API. You need a registered SP-API application, vendor authorization, and either IAM user keys or a role ARN that can call SP-API on your behalf.

Hotglue connections typically include `uri` from the linked Vendor Central account. The tap uses it to pick marketplaces in the same AWS region when `marketplaces` is not configured.

## Usage

```bash
tap-amazon-vendor-central --version
tap-amazon-vendor-central --help
tap-amazon-vendor-central --config config.json --discover > catalog.json
tap-amazon-vendor-central --config config.json --catalog catalog.json
```

## Streams

| Stream | Type | Notes |
|---|---|---|
| `vendor_marketplaces` | Core | Parent stream for marketplace-scoped sync |
| `vendor_purchase_orders` | Core | Vendor purchase orders |
| `vendor_fulfilment_purchase_orders` | Core | Direct fulfilment purchase orders |
| `vendor_fulfilment_customer_invoices` | Core | Direct fulfilment customer invoices |
| `vendor_sales_report` | Report | `GET_VENDOR_SALES_REPORT` (manufacturing, per selling program) |
| `vendor_traffic_report` | Report | `GET_VENDOR_TRAFFIC_REPORT` |
| `vendor_repeat_purchase_report` | Report | `GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT`, weekly |
| `vendor_inventory_report` | Report | `GET_VENDOR_INVENTORY_REPORT` (manufacturing, per selling program) |
| `vendor_forecasting_report` | Report | `GET_VENDOR_FORECASTING_REPORT` |
| `vendor_sales_realtime_report` | Report | Real-time sales |
| `vendor_inventory_realtime_report` | Report | Real-time inventory |
| `vendor_traffic_realtime_report` | Report | Real-time traffic |
| `vendor_sales_sourcing_report` | Report | Sourcing sales |
| `vendor_inventory_sourcing_report` | Report | Sourcing inventory |
| `inventory_product_list` | Report | Product list from inventory report |
| `inventory_product_sourcing_list` | Report | Sourcing product list |
| `product_details` | Core | Product detail lookup (child of inventory product lists) |

Report streams are child streams of `vendor_marketplaces`. Most replicate on `report_end_date`. Sales, inventory, and forecasting reports iterate selling programs (`RETAIL`, `FRESH`, `BUSINESS`) and skip programs the account does not support.

## Developer resources

Requires Python 3.7 through 3.10 (see `pyproject.toml`).

Lint and test with tox:

```bash
tox
```

Or run tools directly:

```bash
poetry run ruff check .
poetry run pytest tap_amazon_vendor_central/tests/
poetry run tap-amazon-vendor-central --about
```

CI runs ruff and pytest on pull requests (`.github/workflows/lint.yml`).

Related repos:

- [`tap-amazon-seller`](https://github.com/hotgluexyz/tap-amazon-seller): Seller Central variant of this tap family
- [Amazon SP-API docs](https://developer-docs.amazon.com/sp-api/)
