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
| `marketplaces` | No | List of marketplace IDs to sync. When omitted, all vendor marketplaces are used |
| `report_types` | No | Legacy report type filter (defaults vary by stream) |
| `processing_status` | No | Report processing statuses to poll. Defaults to `IN_QUEUE`, `IN_PROGRESS` |
| `custom_reports` | No | Dynamic analytics reports. Each entry needs `report` and `period` (see below) |

Example `config.json`:

```json
{
  "lwa_client_id": "amzn1.application-oa2-client.xxxx",
  "client_secret": "xxxx",
  "refresh_token": "Atzr|xxxx",
  "sandbox": false,
  "custom_reports": [
    {"report": "NetPPM", "period": "DAY"}
  ]
}
```

Run `tap-amazon-vendor-central --about` for the full JSON schema.

### Authentication

The tap uses Login with Amazon (LWA) plus AWS SigV4 signing against the Selling Partner API. You need a registered SP-API application, vendor authorization, and either IAM user keys or a role ARN that can call SP-API on your behalf.

### Custom period reports

Set `custom_reports` to add analytics streams at runtime. Each object needs:

- `report`: report type key (currently `NetPPM`)
- `period`: `DAY`, `WEEK`, `MONTH`, or `QUARTER`

When omitted, the tap defaults to `[{"report": "NetPPM", "period": "DAY"}]`.

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
| `vendor_purchase_orders_status` | Core | Purchase order status |
| `vendor_fulfilment_purchase_orders` | Core | Direct fulfilment purchase orders |
| `vendor_fulfilment_customer_invoices` | Core | Direct fulfilment customer invoices |
| `vendor_sales_report` | Report | `GET_VENDOR_SALES_REPORT` |
| `vendor_traffic_report` | Report | `GET_VENDOR_TRAFFIC_REPORT` |
| `vendor_inventory_report` | Report | `GET_VENDOR_INVENTORY_REPORT` |
| `vendor_forecasting_report` | Report | `GET_VENDOR_FORECASTING_REPORT` |
| `vendor_sales_realtime_report` | Report | Real-time sales |
| `vendor_inventory_realtime_report` | Report | Real-time inventory |
| `vendor_traffic_realtime_report` | Report | Real-time traffic |
| `vendor_sales_sourcing_report` | Report | Sourcing sales |
| `vendor_inventory_sourcing_report` | Report | Sourcing inventory |
| `inventory_product_list` | Report | Product list from inventory report |
| `inventory_product_sourcing_list` | Report | Sourcing product list |
| `product_details` | Core | Product detail lookup |
| `vendor_net_pure_product_margin_report_*` | Custom | Created from `custom_reports` (`NetPPM`) |

Report streams are child streams of `vendor_marketplaces` and replicate on report end dates.

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
