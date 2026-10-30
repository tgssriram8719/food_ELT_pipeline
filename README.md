# Food Orders Batch-Based ELT Pipeline

## 1. Project Overview

This project implements a batch-based ELT pipeline using Python,
Pandas, MySQL, SQLAlchemy and PyMySQL.

The pipeline extracts food order records from a CSV dataset,
loads the original records into a raw MySQL database,
retrieves the records, transforms and validates them,
and loads the processed records into a separate analytics database.

## 2. Technologies Used

- Python
- Pandas
- NumPy
- MySQL
- SQLAlchemy
- PyMySQL
- VS Code

## 3. Pipeline Architecture

```
CSV Dataset
     |
     v
EXTRACT
     |
     v
RAW DATABASE
food_orders_raw_db
     |
     v
RETRIEVE RAW DATA
     |
     v
TRANSFORM
     |
     v
VALIDATE
     |
     v
ANALYTICS DATABASE
food_orders_analytics_db
     |
     v
SQL BUSINESS ANALYSIS
     |
     v
EXECUTION REPORTS
```

## 4. Database Design

### Raw Database

- **Database:** `food_orders_raw_db`
- **Table:** `raw_food_orders`

Stores original CSV records without transformation.

### Analytics Database

- **Database:** `food_orders_analytics_db`
- **Table:** `cleaned_food_orders`

Stores cleaned and transformed records.

- **Table:** `rejected_food_orders`

Stores rejected records and their rejection reasons.

## 5. Batch Processing

- **Batch size:** 100 records.
- Each batch is extracted, loaded and processed separately.
- The final batch may contain fewer than 100 records.

## 6. Transformation Rules

1. Detect exact duplicate records.
2. Detect conflicting Order IDs.
3. Handle missing Order IDs.
4. Validate order and delivery timestamps.
5. Calculate delivery duration.
6. Convert financial columns into numeric values.
7. Handle missing financial values.
8. Handle missing categorical values.
9. Calculate platform net revenue.
10. Identify long deliveries.
11. Identify financial anomalies.
12. Store rejected records with rejection reasons.

## 7. Platform Net Revenue

Platform Net Revenue =

Commission Fee + Delivery Fee
- Payment Processing Fee
- Refunds/Chargebacks

## 8. Business Analysis

The pipeline supports six business questions:

1. What is the total order value?
2. What is the average order value?
3. Which payment method has the highest number of orders?
4. Which order channel has the highest number of orders?
5. What is the average delivery time?
6. What is the total platform net revenue?

SQL queries are available in:

`sql/business_analysis.sql`

## 9. Execution

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure MySQL credentials in `.env`.

Run:

```bash
python elt_batch_pipeline.py
```

## 10. Execution Reports

The `reports/` folder contains:

- `validation_report.csv`
- `batch_execution_log.csv`
- `pipeline_execution_report.json`
- `pipeline_execution_summary.csv`

## 11. Expected Outcome

A complete batch-based ELT workflow with:

- Raw data preservation.
- Batch-wise extraction and loading.
- Data transformation.
- Data quality validation.
- Rejected record tracking.
- Analytics database loading.
- Business analysis queries.
- Execution monitoring and reporting.
