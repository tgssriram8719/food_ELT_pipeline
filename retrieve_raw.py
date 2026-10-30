import pandas as pd
from sqlalchemy import text
from config import (
    database_engine,
    RAW_DB,
    SOURCE_COLUMNS,
    BATCH_SIZE
)


def retrieve_raw_batches(batch_size=BATCH_SIZE, source_file_hash=None):
    engine = database_engine(RAW_DB)
    offset = 0
    batch_number = 0
    columns = ", ".join(f"`{column}`" for column in SOURCE_COLUMNS)

    where_clause = ""
    base_params = {"limit": int(batch_size)}
    if source_file_hash:
        where_clause = "WHERE source_file_hash = :source_file_hash"
        base_params["source_file_hash"] = source_file_hash

    try:
        while True:
            query = text(f"""
                SELECT
                    ingestion_id,
                    source_row_number,
                    source_file_hash,
                    batch_number,
                    {columns}
                FROM raw_food_orders
                {where_clause}
                ORDER BY ingestion_id ASC
                LIMIT :limit OFFSET :offset
            """)
            params = {**base_params, "offset": int(offset)}

            with engine.connect() as connection:
                df = pd.read_sql_query(
                    query,
                    connection,
                    params=params
                )

            if df.empty:
                break

            batch_number += 1
            yield batch_number, df
            offset += batch_size
    finally:
        engine.dispose()


if __name__ == "__main__":
    for b_num, b_df in retrieve_raw_batches(10):
        print(f"Retrieved batch {b_num} with {len(b_df)} rows.")
        break

