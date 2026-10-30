import hashlib
from pathlib import Path
import pandas as pd
from config import SOURCE_CSV, BATCH_SIZE, SOURCE_COLUMNS


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_batches(path=None, batch_size=None):
    if isinstance(path, int) and batch_size is None:
        batch_size = path
        path = SOURCE_CSV
    if path is None:
        path = SOURCE_CSV
    if batch_size is None:
        batch_size = BATCH_SIZE

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    source_hash = file_sha256(csv_path)
    total_records = 0
    batch_number = 0

    # Ensure all columns are read as text without losing precision or converting blanks to NaN
    reader = pd.read_csv(
        csv_path,
        chunksize=batch_size,
        dtype=str,
        keep_default_na=False
    )

    for chunk in reader:
        batch_number += 1
        chunk_df = chunk.copy()

        # Validate that required source columns exist
        for col in SOURCE_COLUMNS:
            if col not in chunk_df.columns:
                chunk_df[col] = ""

        # Maintain 1-based source row numbers
        start_row = total_records + 1
        end_row = total_records + len(chunk_df)
        chunk_df["source_row_number"] = list(range(start_row, end_row + 1))
        chunk_df["source_file_hash"] = source_hash
        chunk_df["batch_number"] = batch_number

        total_records += len(chunk_df)

        yield {
            "batch_number": batch_number,
            "df": chunk_df,
            "source_hash": source_hash,
            "records_in_batch": len(chunk_df),
            "total_records": total_records
        }
