# Data Pipeline Architecture - MCQ Test Analytics

## Overview

This data pipeline supports the transformation of raw MCQ test platform activity data into analytics-ready tables. It is built to be modular, scalable, and fault-tolerant using Python and PySpark.

---

## Objectives

* Extract raw student test activity (answers, sessions, submissions).
* Transform into structured analytics models.
* Load into fact tables for dashboarding.
* Handle failures gracefully and support backfills.

---

## Architecture Components

### 1. **Source Systems**

* PostgreSQL Database with operational tables:

  * `students`, `teachers`, `tests`, `questions`, `answers`, `sessions`, `submissions`

### 2. **ETL Pipelines**

* **Batch Pipelines (Python + Pandas + SQLAlchemy)**

  * Located in `python_batch_pipelines/`
  * Best for small to medium volumes.
  * Run hourly or on demand.

* **Scalable Pipelines (PySpark)**

  * Located in `spark_scalable_pipeline/`
  * Handles large test datasets efficiently.
  * Supports AQE and dynamic scaling.

### 3. **Transformation Logic**

* Fact table generation:

  * `question_interaction_fact`
  * `test_summary_fact`
  * `test_funnel_fact`

### 4. **Metadata Tracking**

* Table: `etl_metadata`
* Tracks:

  * `pipeline_name`
  * `last_run_time`
  * `status` (success/failed)

### 5. **Orchestration & CLI Support**

* Supports command-line overrides:

  * `--start` and `--end` for custom backfills.
  * `--env` for environment selection (e.g., dev, prod).

### 6. **Alerting**

* Integrated alert mechanism (Slack/email).
* Alerts on critical failures (e.g., two consecutive ETL failures).

---

## Data Flow Diagram (Textual)

```
[Raw Tables: answers, questions, sessions, submissions]
      │
      ▼
[Batch/Scalable ETL Pipeline] ──▶ Transform Logic (fact tables)
      │                                  │
      ▼                                  ▼
[etl_metadata]                    [Analytics Tables]
                                     ├── question_interaction_fact
                                     ├── test_summary_fact
                                     └── test_funnel_fact
```

---

## Key Features

* Modular structure with shared utils.
* Backfill support via CLI.
* Metadata-driven execution.
* Environment-based configuration via `.env`
* Alerting system for pipeline reliability.

---

## How to Run

```bash
python run_etl.py --env dev
python run_etl.py --env dev --start "2024:06:01 10" --end "2024:06:01 11"
```
