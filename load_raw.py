from sqlalchemy import text
from config import (
    create_databases,
    database_engine,
    RAW_DB,
    SOURCE_COLUMNS,
    BATCH_SIZE
)
from extract import extract_batches

PARAM_NAMES = {
    column: f"p{i}"
    for i, column in enumerate(SOURCE_COLUMNS)
}


def create_raw_table(engine):
    source_fields = ",\n    ".join(
        f"`{column}` LONGTEXT NULL"
        for column in SOURCE_COLUMNS
    )
    query = f"""
    CREATE TABLE IF NOT EXISTS raw_food_orders (
        ingestion_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_row_number BIGINT NOT NULL,
        source_file_hash CHAR(64) NOT NULL,
        batch_number INT NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        {source_fields},
        UNIQUE KEY uq_source_row (source_file_hash, source_row_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def load_raw_data(batch_size=BATCH_SIZE):
    create_databases()
    engine = database_engine(RAW_DB)
    create_raw_table(engine)

    total_records = 0
    total_inserted = 0
    total_skipped = 0
    batch_count = 0
    source_file_hash = None

    columns_str = ", ".join(f"`{col}`" for col in SOURCE_COLUMNS)
    values_str = ", ".join(f":{PARAM_NAMES[col]}" for col in SOURCE_COLUMNS)

    insert_query = text(f"""
        INSERT IGNORE INTO raw_food_orders (
            source_row_number,
            source_file_hash,
            batch_number,
            {columns_str}
        ) VALUES (
            :source_row_number,
            :source_file_hash,
            :batch_number,
            {values_str}
        )
    """)

    try:
        for batch in extract_batches(batch_size=batch_size):
            batch_count += 1
            source_file_hash = batch["source_hash"]
            df = batch["df"]
            batch_records = []

            for _, row in df.iterrows():
                record = {
                    "source_row_number": int(row["source_row_number"]),
                    "source_file_hash": str(row["source_file_hash"]),
                    "batch_number": int(row["batch_number"]),
                }
                for col in SOURCE_COLUMNS:
                    record[PARAM_NAMES[col]] = str(row[col]) if row[col] is not None else None
                batch_records.append(record)

            with engine.begin() as conn:
                result = conn.execute(insert_query, batch_records)
                inserted = result.rowcount
                skipped = len(batch_records) - inserted
                total_inserted += inserted
                total_skipped += skipped
                total_records += len(batch_records)

            print(f"[Raw Load] Batch {batch_count}: {len(batch_records)} read, {inserted} inserted, {skipped} duplicate/skipped.")

    finally:
        engine.dispose()

    return {
        "status": "SUCCESS",
        "batches_processed": batch_count,
        "records_processed": total_records,
        "records_inserted": total_inserted,
        "records_skipped": total_skipped,
        "source_file_hash": source_file_hash
    }


if __name__ == "__main__":
    summary = load_raw_data()
    print("Raw load complete:", summary)

