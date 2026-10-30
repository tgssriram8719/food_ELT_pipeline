import math
import json
import pandas as pd
from sqlalchemy import text
from config import database_engine, ANALYTICS_DB

CLEANED_COLUMNS = [
    "source_file_hash",
    "source_row_number",
    "source_batch_number",
    "order_id",
    "customer_id",
    "restaurant_id",
    "order_datetime",
    "delivery_datetime",
    "order_value",
    "delivery_fee",
    "payment_method",
    "discounts_and_offers",
    "commission_fee",
    "payment_processing_fee",
    "refunds_chargebacks",
    "weather_condition",
    "order_channel",
    "delivery_time_minutes",
    "order_date",
    "order_hour",
    "platform_net_revenue",
    "long_delivery_flag",
    "financial_anomaly_flag"
]


def create_analytics_tables(engine):
    create_cleaned_query = """
    CREATE TABLE IF NOT EXISTS cleaned_food_orders (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_file_hash CHAR(64) NOT NULL,
        source_row_number BIGINT NOT NULL,
        source_batch_number INT NOT NULL,
        order_id VARCHAR(100) NOT NULL,
        customer_id VARCHAR(100) NULL,
        restaurant_id VARCHAR(100) NULL,
        order_datetime DATETIME NULL,
        delivery_datetime DATETIME NULL,
        order_value DECIMAL(10, 2) NULL,
        delivery_fee DECIMAL(10, 2) NULL,
        payment_method VARCHAR(100) NULL,
        discounts_and_offers VARCHAR(255) NULL,
        commission_fee DECIMAL(10, 2) NULL,
        payment_processing_fee DECIMAL(10, 2) NULL,
        refunds_chargebacks DECIMAL(10, 2) NULL,
        weather_condition VARCHAR(100) NULL,
        order_channel VARCHAR(100) NULL,
        delivery_time_minutes DECIMAL(10, 2) NULL,
        order_date DATE NULL,
        order_hour INT NULL,
        platform_net_revenue DECIMAL(10, 2) NULL,
        long_delivery_flag BOOLEAN DEFAULT FALSE,
        financial_anomaly_flag BOOLEAN DEFAULT FALSE,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_source_clean (source_file_hash, source_row_number),
        UNIQUE KEY uq_order_id (order_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    create_rejected_query = """
    CREATE TABLE IF NOT EXISTS rejected_food_orders (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_file_hash CHAR(64) NOT NULL,
        source_row_number BIGINT NOT NULL,
        source_batch_number INT NOT NULL,
        order_id VARCHAR(100) NULL,
        rejection_reason VARCHAR(255) NOT NULL,
        raw_record_json LONGTEXT NULL,
        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_source_reject (source_file_hash, source_row_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    with engine.begin() as conn:
        conn.execute(text(create_cleaned_query))
        conn.execute(text(create_rejected_query))


def load_analytics_batches(accepted_df, rejected_df):
    """
    Inserts accepted records into cleaned_food_orders and rejected into rejected_food_orders.
    """
    engine = database_engine(ANALYTICS_DB)
    create_analytics_tables(engine)

    accepted_inserted = 0
    rejected_inserted = 0

    try:
        # Load Accepted
        if accepted_df is not None and not accepted_df.empty:
            cols = list(CLEANED_COLUMNS)
            cols_clause = ", ".join(f"`{c}`" for c in cols)
            vals_clause = ", ".join(f":{c}" for c in cols)

            insert_accepted = text(f"""
                INSERT IGNORE INTO cleaned_food_orders (
                    {cols_clause}
                ) VALUES (
                    {vals_clause}
                )
            """)

            accepted_records = []
            for _, row in accepted_df.iterrows():
                rec = {}
                for col in CLEANED_COLUMNS:
                    val = row.get(col)
                    if pd.isna(val):
                        rec[col] = None
                    elif col in ["long_delivery_flag", "financial_anomaly_flag"]:
                        rec[col] = bool(val)
                    elif col in ["source_row_number", "source_batch_number", "order_hour"]:
                        rec[col] = int(val)
                    elif col in ["order_value", "delivery_fee", "commission_fee", "payment_processing_fee", "refunds_chargebacks", "delivery_time_minutes", "platform_net_revenue"]:
                        rec[col] = float(val) if val is not None else None
                    elif col in ["order_datetime", "delivery_datetime"]:
                        rec[col] = val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(val, "strftime") else str(val)
                    elif col == "order_date":
                        rec[col] = str(val)
                    else:
                        rec[col] = str(val)
                accepted_records.append(rec)

            with engine.begin() as conn:
                res = conn.execute(insert_accepted, accepted_records)
                accepted_inserted = res.rowcount

        # Load Rejected
        if rejected_df is not None and not rejected_df.empty:
            insert_rejected = text("""
                INSERT IGNORE INTO rejected_food_orders (
                    source_file_hash,
                    source_row_number,
                    source_batch_number,
                    order_id,
                    rejection_reason,
                    raw_record_json
                ) VALUES (
                    :source_file_hash,
                    :source_row_number,
                    :source_batch_number,
                    :order_id,
                    :rejection_reason,
                    :raw_record_json
                )
            """)

            rejected_records = []
            for _, row in rejected_df.iterrows():
                rejected_records.append({
                    "source_file_hash": str(row["source_file_hash"]),
                    "source_row_number": int(row["source_row_number"]),
                    "source_batch_number": int(row["source_batch_number"]),
                    "order_id": str(row["order_id"]) if pd.notna(row["order_id"]) else None,
                    "rejection_reason": str(row["rejection_reason"]),
                    "raw_record_json": str(row["raw_record_json"]) if pd.notna(row["raw_record_json"]) else None
                })

            with engine.begin() as conn:
                res = conn.execute(insert_rejected, rejected_records)
                rejected_inserted = res.rowcount

    finally:
        engine.dispose()

    return {
        "accepted_inserted": accepted_inserted,
        "rejected_inserted": rejected_inserted
    }

