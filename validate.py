import numpy as np
import pandas as pd


def validate_transformed(df):
    """
    Validates a batch of transformed accepted records.
    Returns:
        results: list of dicts with keys: check, passed, details
    """
    results = []

    def add(check, passed, details):
        results.append({
            "check": check,
            "passed": bool(passed),
            "details": str(details)
        })

    required_columns = [
        "source_file_hash",
        "source_row_number",
        "order_id",
        "order_datetime",
        "delivery_datetime",
        "order_value",
        "delivery_time_minutes",
        "platform_net_revenue",
        "payment_method",
        "discounts_and_offers"
    ]

    if df is None or df.empty:
        add("Empty Batch Check", True, "Batch has 0 accepted rows to validate.")
        return results

    # 1. Required columns presence
    missing = [c for c in required_columns if c not in df.columns]
    add("Required columns present", len(missing) == 0, f"Missing: {missing}" if missing else "All present")

    # 2. No null or blank Order IDs
    null_order_ids = df["order_id"].isna().sum() + (df["order_id"].astype(str).str.strip() == "").sum()
    add("Order ID not null/blank", null_order_ids == 0, f"{null_order_ids} null or blank order IDs found")

    # 3. Order ID uniqueness within batch
    duplicates = df["order_id"].duplicated().sum()
    add("Order ID unique within batch", duplicates == 0, f"{duplicates} duplicate order IDs in batch")

    # 4. Delivery time non-negative
    if "delivery_time_minutes" in df.columns:
        negative_delivery = (df["delivery_time_minutes"] < 0).sum()
        add("Delivery time non-negative", negative_delivery == 0, f"{negative_delivery} negative delivery times found")

    # 5. Order value non-negative
    if "order_value" in df.columns:
        negative_val = (df["order_value"] < 0).sum()
        add("Order value positive/non-negative", negative_val == 0, f"{negative_val} negative order values found")

    # 6. Chronological timestamps
    if "order_datetime" in df.columns and "delivery_datetime" in df.columns:
        invalid_time_order = (df["delivery_datetime"] < df["order_datetime"]).sum()
        add("Delivery time >= Order time", invalid_time_order == 0, f"{invalid_time_order} chronological timestamp violations")

    # 7. Platform Net Revenue calculation integrity
    if "platform_net_revenue" in df.columns:
        null_rev = df["platform_net_revenue"].isna().sum()
        add("Platform net revenue populated", null_rev == 0, f"{null_rev} null revenue values")

    return results

