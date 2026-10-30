import json
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
from config import (
    BATCH_SIZE,
    SOURCE_CSV,
    RAW_DB,
    ANALYTICS_DB,
    create_databases
)
from load_raw import load_raw_data
from retrieve_raw import retrieve_raw_batches
from transform import transform_batch, reset_transformation_state
from validate import validate_transformed
from load_transformed import load_analytics_batches

BASE_DIR = Path(__file__).resolve().parent
REPORT_DIR = BASE_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)


def run_pipeline():
    started = datetime.now().isoformat(timespec="seconds")
    start_time = time.time()
    print("\n" + "=" * 65)
    print("STARTING FOOD ORDERS BATCH-BASED ELT PIPELINE")
    print(f"Timestamp: {started}")
    print(f"Source file: {SOURCE_CSV}")
    print(f"Batch size: {BATCH_SIZE}")
    print("=" * 65 + "\n")

    # Step 1: Initialize databases
    print("[1/4] Ensuring MySQL Databases and schemas exist...")
    create_databases()

    # Step 2: Extract & Load Raw Data
    print("\n[2/4] Ingesting CSV records into Raw Database...")
    raw_summary = load_raw_data(batch_size=BATCH_SIZE)
    source_file_hash = raw_summary.get("source_file_hash")
    print(f"Raw Ingestion Summary: {raw_summary}")

    # Step 3: Retrieve, Transform, Validate and Load Analytics
    print("\n[3/4] Processing Batches from Raw to Analytics Database...")
    reset_transformation_state()

    batch_logs = []
    all_validation_results = []
    total_raw = 0
    total_accepted = 0
    total_rejected = 0
    total_accepted_inserted = 0
    total_rejected_inserted = 0

    for batch_num, raw_batch in retrieve_raw_batches(batch_size=BATCH_SIZE, source_file_hash=source_file_hash):
        batch_start = time.time()
        num_raw = len(raw_batch)
        total_raw += num_raw

        # Transform
        accepted_df, rejected_df = transform_batch(raw_batch)
        num_accepted = len(accepted_df)
        num_rejected = len(rejected_df)
        total_accepted += num_accepted
        total_rejected += num_rejected

        # Validate accepted records
        validation_results = validate_transformed(accepted_df)
        batch_all_passed = True
        for v in validation_results:
            if not v["passed"]:
                batch_all_passed = False
            all_validation_results.append({
                "batch_number": batch_num,
                "check": v["check"],
                "passed": v["passed"],
                "details": v["details"],
                "timestamp": datetime.now().isoformat(timespec="seconds")
            })

        # Load into Analytics Database
        load_res = load_analytics_batches(accepted_df, rejected_df)
        acc_ins = load_res["accepted_inserted"]
        rej_ins = load_res["rejected_inserted"]
        total_accepted_inserted += acc_ins
        total_rejected_inserted += rej_ins

        batch_duration = round(time.time() - batch_start, 3)

        print(
            f"Batch {batch_num:03d}: Raw={num_raw:3d} | "
            f"Accepted={num_accepted:3d} (Ins={acc_ins:3d}) | "
            f"Rejected={num_rejected:3d} (Ins={rej_ins:3d}) | "
            f"Validation={'PASS' if batch_all_passed else 'WARN'} | "
            f"Time={batch_duration}s"
        )

        batch_logs.append({
            "batch_number": batch_num,
            "raw_records": num_raw,
            "accepted_records": num_accepted,
            "rejected_records": num_rejected,
            "accepted_inserted": acc_ins,
            "rejected_inserted": rej_ins,
            "validation_passed": batch_all_passed,
            "duration_seconds": batch_duration
        })

    ended = datetime.now().isoformat(timespec="seconds")
    total_duration = round(time.time() - start_time, 2)

    # Step 4: Generate Reports
    print("\n[4/4] Generating Pipeline Execution Reports...")

    # 1. Validation Report
    val_report_path = REPORT_DIR / "validation_report.csv"
    pd.DataFrame(all_validation_results).to_csv(val_report_path, index=False)

    # 2. Batch Execution Log
    batch_log_path = REPORT_DIR / "batch_execution_log.csv"
    pd.DataFrame(batch_logs).to_csv(batch_log_path, index=False)

    # 3. Pipeline Execution Summary CSV
    summary_data = [
        {"metric": "pipeline_started_at", "value": started},
        {"metric": "pipeline_ended_at", "value": ended},
        {"metric": "total_duration_seconds", "value": total_duration},
        {"metric": "source_csv", "value": str(SOURCE_CSV)},
        {"metric": "source_file_hash", "value": source_file_hash},
        {"metric": "batches_processed", "value": len(batch_logs)},
        {"metric": "total_raw_records_processed", "value": total_raw},
        {"metric": "total_accepted_records", "value": total_accepted},
        {"metric": "total_rejected_records", "value": total_rejected},
        {"metric": "total_accepted_inserted", "value": total_accepted_inserted},
        {"metric": "total_rejected_inserted", "value": total_rejected_inserted},
    ]
    summary_path = REPORT_DIR / "pipeline_execution_summary.csv"
    pd.DataFrame(summary_data).to_csv(summary_path, index=False)

    # 4. Pipeline Execution Report JSON
    report_dict = {
        "pipeline_name": "Food Orders Batch-Based ELT Pipeline",
        "started_at": started,
        "ended_at": ended,
        "duration_seconds": total_duration,
        "source_file": str(SOURCE_CSV),
        "source_file_hash": source_file_hash,
        "raw_stage_summary": raw_summary,
        "analytics_stage_summary": {
            "batches_processed": len(batch_logs),
            "total_raw_retrieved": total_raw,
            "total_accepted": total_accepted,
            "total_rejected": total_rejected,
            "total_accepted_inserted": total_accepted_inserted,
            "total_rejected_inserted": total_rejected_inserted
        },
        "batches": batch_logs,
        "status": "COMPLETED"
    }
    json_report_path = REPORT_DIR / "pipeline_execution_report.json"
    with open(json_report_path, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=4)

    print("\n" + "=" * 65)
    print("PIPELINE EXECUTION COMPLETED")
    print(f"Total Raw Processed: {total_raw}")
    print(f"Total Accepted: {total_accepted} (Inserted: {total_accepted_inserted})")
    print(f"Total Rejected: {total_rejected} (Inserted: {total_rejected_inserted})")
    print(f"Reports saved to: {REPORT_DIR}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    run_pipeline()

