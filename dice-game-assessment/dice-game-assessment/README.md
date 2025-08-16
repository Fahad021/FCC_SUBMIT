
# Dice Game Data Processing – Star Schema

This repository contains a lightweight Python/pandas application that ingests the provided CSVs, builds a dimensional (star) schema, runs data quality checks, and produces a few business insights for 2024.

## Project Structure
```
.
├── data
│   ├── raw/                # Provided CSVs
│   └── warehouse/          # Parquet + CSV outputs (dim_*, fact_*)
├── src/
│   ├── dice_dw/etl.py      # ETL/transform logic
│   └── main.py             # Pipeline entry point
├── tests/
│   └── test_dw.py          # Basic data quality tests
└── analysis/               # Notebooks/insights if needed
```
## How to run
```bash
python -m src.main
# or
python -c "from src.dice_dw.etl import run_pipeline; print(run_pipeline())"
```
## Dimensional Model (Star Schema)
**Dimensions**
- `dim_user(user_id, user_registration_id, username, registration_email, first_name, last_name, ip_address, social_media_handle, email)`
- `dim_plan(plan_id, payment_frequency_code, payment_frequency_desc_en, cost_amount)`
- `dim_channel(channel_code, channel_desc_en)`
- `dim_status(status_code, status_desc_en)`
- `dim_date(date_key, date, year, quarter, month, day, day_of_week, week_of_year, is_weekend)`

**Facts**
- `fact_play_session(play_session_id, user_id, date_key, start_ts, end_ts, duration_seconds, channel_code, status_code, total_score)`
- `fact_user_plan(user_registration_id, payment_detail_id, plan_id, start_date_key, end_date_key, ...)`

## Data Quality Checks
- Keys are unique and non-null (`plan_id`, `channel_code`, etc.)
- Foreign-key coverage from facts to dims (`channel_code`, `status_code`)
- Timestamps parse cleanly and durations are non-negative

## Insights (examples)
- Sessions by channel (Browser vs Mobile App)
- One-time vs Subscription uptake
- Estimated gross revenue for 2024 (subscriptions × cycles × plan price)

> Note: Revenue is derived from `plan.cost_amount` and plan cycles within 2024 (`MONTHLY`, `ANNUALLY`, `ONETIME`).
