import hashlib
import json
import numpy as np
import pandas as pd
from config import SOURCE_COLUMNS

FINANCIAL_COLUMNS = [
    "Order Value",
    "Delivery Fee",
    "Commission Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks"
]

REVENUE_COMPONENTS = [
    "Commission Fee",
    "Delivery Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks"
]

# Stateful tracking across pipeline execution
_seen_fingerprints = set()
_seen_order_signatures = {}


def reset_transformation_state():
    """Reset duplicate and conflicting ID caches between pipeline runs."""
    global _seen_fingerprints, _seen_order_signatures
    _seen_fingerprints.clear()
    _seen_order_signatures.clear()


def is_blank(value):
    return (
        value is None
        or pd.isna(value)
        or str(value).strip() == ""
    )


def source_fingerprint(row):
    raw_str = "|".join(str(row.get(col, "")).strip() for col in SOURCE_COLUMNS)
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def parse_numeric(value, default=0.0):
    if is_blank(value):
        return default
    cleaned = str(value).replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def transform_batch(raw_df):
    """
    Transforms a batch of raw records.
    Returns:
        (accepted_df, rejected_df)
    """
    accepted_rows = []
    rejected_rows = []

    for _, row in raw_df.iterrows():
        source_hash = str(row.get("source_file_hash", ""))
        source_row = int(row.get("source_row_number", 0))
        batch_num = int(row.get("batch_number", 1))

        # Capture raw record JSON for auditing
        raw_dict = {col: row.get(col) for col in SOURCE_COLUMNS}
        raw_json = json.dumps(raw_dict, default=str)

        # 1. Missing Order ID Check
        order_id_val = row.get("Order ID")
        if is_blank(order_id_val):
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": None,
                "rejection_reason": "Missing Order ID",
                "raw_record_json": raw_json
            })
            continue

        order_id = str(order_id_val).strip()

        # 2. Exact Duplicate Check
        fp = source_fingerprint(row)
        if fp in _seen_fingerprints:
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": order_id,
                "rejection_reason": "Duplicate record",
                "raw_record_json": raw_json
            })
            continue

        # 3. Conflicting Order ID Check (same Order ID with different customer or restaurant)
        cust_id = str(row.get("Customer ID", "")).strip()
        rest_id = str(row.get("Restaurant ID", "")).strip()
        order_sig = (cust_id, rest_id)

        if order_id in _seen_order_signatures:
            if _seen_order_signatures[order_id] != order_sig:
                rejected_rows.append({
                    "source_file_hash": source_hash,
                    "source_row_number": source_row,
                    "source_batch_number": batch_num,
                    "order_id": order_id,
                    "rejection_reason": "Conflicting Order ID",
                    "raw_record_json": raw_json
                })
                continue
        else:
            _seen_order_signatures[order_id] = order_sig

        # 4. Timestamp Conversion & Validation
        order_dt_val = row.get("Order Date and Time")
        deliv_dt_val = row.get("Delivery Date and Time")

        if is_blank(order_dt_val) or is_blank(deliv_dt_val):
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": order_id,
                "rejection_reason": "Missing timestamp",
                "raw_record_json": raw_json
            })
            continue

        order_dt = pd.to_datetime(order_dt_val, errors="coerce")
        deliv_dt = pd.to_datetime(deliv_dt_val, errors="coerce")

        if pd.isna(order_dt) or pd.isna(deliv_dt):
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": order_id,
                "rejection_reason": "Invalid timestamp format",
                "raw_record_json": raw_json
            })
            continue

        if deliv_dt < order_dt:
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": order_id,
                "rejection_reason": "Delivery timestamp prior to order timestamp",
                "raw_record_json": raw_json
            })
            continue

        # 5. Financial Data Conversion & Validation
        order_val = parse_numeric(row.get("Order Value"), default=None)
        if order_val is None or order_val < 0:
            rejected_rows.append({
                "source_file_hash": source_hash,
                "source_row_number": source_row,
                "source_batch_number": batch_num,
                "order_id": order_id,
                "rejection_reason": "Invalid or missing order value",
                "raw_record_json": raw_json
            })
            continue

        delivery_fee = parse_numeric(row.get("Delivery Fee"), default=0.0) or 0.0
        commission_fee = parse_numeric(row.get("Commission Fee"), default=0.0) or 0.0
        payment_fee = parse_numeric(row.get("Payment Processing Fee"), default=0.0) or 0.0
        refunds = parse_numeric(row.get("Refunds/Chargebacks"), default=0.0) or 0.0

        # Mark fingerprint as seen after passing primary sanity checks
        _seen_fingerprints.add(fp)

        # 6. Delivery Duration Calculation
        delivery_time_minutes = round((deliv_dt - order_dt).total_seconds() / 60.0, 2)

        # 7. Categorical Handling
        pm_val = row.get("Payment Method")
        payment_method = "Unknown" if is_blank(pm_val) else str(pm_val).strip()

        oc_val = row.get("Order Channel")
        order_channel = "Unknown" if is_blank(oc_val) else str(oc_val).strip()

        wc_val = row.get("Weather Condition")
        weather_condition = "Normal" if is_blank(wc_val) else str(wc_val).strip()

        do_val = row.get("Discounts and Offers")
        discounts_offers = "None" if is_blank(do_val) else str(do_val).strip()

        # 8. Platform Net Revenue Calculation
        # Platform Net Revenue = Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
        net_revenue = round(commission_fee + delivery_fee - payment_fee - refunds, 2)

        # 9. Anomaly & Long Delivery Flags
        long_delivery_flag = bool(delivery_time_minutes > 60.0)
        financial_anomaly = bool(net_revenue < 0 or refunds > order_val)

        accepted_rows.append({
            "source_file_hash": source_hash,
            "source_row_number": source_row,
            "source_batch_number": batch_num,
            "order_id": order_id,
            "customer_id": cust_id if cust_id else None,
            "restaurant_id": rest_id if rest_id else None,
            "order_datetime": order_dt,
            "delivery_datetime": deliv_dt,
            "order_value": round(float(order_val), 2),
            "delivery_fee": round(float(delivery_fee), 2),
            "payment_method": payment_method,
            "discounts_and_offers": discounts_offers,
            "commission_fee": round(float(commission_fee), 2),
            "payment_processing_fee": round(float(payment_fee), 2),
            "refunds_chargebacks": round(float(refunds), 2),
            "weather_condition": weather_condition,
            "order_channel": order_channel,
            "delivery_time_minutes": delivery_time_minutes,
            "order_date": order_dt.date(),
            "order_hour": int(order_dt.hour),
            "platform_net_revenue": net_revenue,
            "long_delivery_flag": long_delivery_flag,
            "financial_anomaly_flag": financial_anomaly
        })

    accepted_df = pd.DataFrame(accepted_rows)
    rejected_df = pd.DataFrame(rejected_rows)
    return accepted_df, rejected_df

