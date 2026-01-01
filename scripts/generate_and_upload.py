import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from faker import Faker

fake = Faker()


def utc_now_iso() -> str:
    """Current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def as_money(x: float) -> float:
    """Round to 2 decimals (money-like)."""
    return float(Decimal(str(x)).quantize(Decimal("0.01")))


def base_event() -> Dict[str, Any]:
    """Generate a clean (valid) e-commerce sales event."""
    quantity = fake.random_int(min=1, max=5)
    unit_price = as_money(fake.pyfloat(min_value=5, max_value=500, right_digits=2))
    total_amount = as_money(quantity * unit_price)

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": utc_now_iso(),
        "order_id": str(uuid.uuid4()),
        "customer_id": fake.random_int(min=1, max=5000),
        "session_id": str(uuid.uuid4()),
        "product_id": fake.random_int(min=1, max=2000),
        "category": fake.random_element(elements=("electronics", "fashion", "home", "sports", "beauty")),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "currency": "USD",
        "payment_method": fake.random_element(elements=("card", "paypal", "apple_pay", "bank_transfer")),
        "country": fake.country_code(),
        "device": fake.random_element(elements=("mobile", "desktop", "tablet")),
        "marketing_channel": fake.random_element(elements=("organic", "paid_search", "email", "social", "affiliate")),
    }


def corrupt_event(e: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce controlled "bad" data to trigger rejects in Snowflake STAGING rules.
    This will generate different reject reasons such as:
      - Missing event_id
      - Invalid event_ts
      - Invalid quantity (non-numeric)
      - Non-positive quantity (<= 0)
      - Invalid total_amount (non-numeric / null)
      - Negative total_amount (< 0)
    """
    bad_type = fake.random_element(
        elements=(
            "missing_event_id",
            "bad_ts",
            "qty_non_numeric",
            "qty_zero",
            "total_non_numeric",
            "total_null",
            "total_negative",
        )
    )

    if bad_type == "missing_event_id":
        e["event_id"] = None

    elif bad_type == "bad_ts":
        # Not parseable as timestamp
        e["event_ts"] = "not-a-timestamp"

    elif bad_type == "qty_non_numeric":
        # Will fail TRY_TO_NUMBER(...) -> NULL in Snowflake
        e["quantity"] = "two"

    elif bad_type == "qty_zero":
        # Will fail quantity > 0 rule
        e["quantity"] = 0

    elif bad_type == "total_non_numeric":
        # Will fail TRY_TO_NUMBER(...) -> NULL
        e["total_amount"] = "NaN"

    elif bad_type == "total_null":
        # Will fail TRY_TO_NUMBER(...) -> NULL
        e["total_amount"] = None

    elif bad_type == "total_negative":
        # Will fail total_amount >= 0 rule
        e["total_amount"] = -1.00

    return e


def generate_event(bad_rate: float) -> Dict[str, Any]:
    """
    Generate an event. With probability bad_rate, corrupt the event to create rejects.
    """
    e = base_event()
    if fake.pyfloat(min_value=0, max_value=1) < bad_rate:
        e = corrupt_event(e)
    return e


def upload_json_lines(s3_client, bucket: str, key: str, events: List[Dict[str, Any]]) -> None:
    """
    Upload JSON Lines (JSONL): one JSON object per line.
    """
    body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def main() -> None:
    load_dotenv()

    # Config from .env
    region = os.getenv("AWS_REGION", "us-east-2")
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "raw/sales")
    events_per_file = int(os.getenv("EVENTS_PER_FILE", "200"))
    files = int(os.getenv("FILES", "5"))
    bad_rate = float(os.getenv("BAD_RATE", "0.05"))  # 5% bad records by default

    if not bucket:
        raise ValueError("Missing S3_BUCKET in .env")

    s3 = boto3.client("s3", region_name=region)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dt = ts[:8]

    print("=== Generator Settings ===")
    print(f"AWS_REGION       : {region}")
    print(f"S3_BUCKET        : {bucket}")
    print(f"S3_PREFIX        : {prefix}")
    print(f"FILES            : {files}")
    print(f"EVENTS_PER_FILE  : {events_per_file}")
    print(f"BAD_RATE         : {bad_rate} (e.g., 0.05 = 5%)")
    print("==========================")

    for i in range(files):
        events = [generate_event(bad_rate=bad_rate) for _ in range(events_per_file)]
        key = f"{prefix}/dt={dt}/sales_{ts}_{i+1}.jsonl"

        try:
            upload_json_lines(s3, bucket, key, events)
            print(f"Uploaded: s3://{bucket}/{key} ({events_per_file} events)")
        except ClientError as e:
            raise RuntimeError(f"Failed upload to S3: {e}") from e


if __name__ == "__main__":
    main()
