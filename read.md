# MCQ Test Analytics ETL Pipeline

## 1. Introduction

This project is designed to support analytics and reporting on a multiple-choice (MCQ) based test platform. The system captures test attempts, submissions, and user interactions with each question. The primary goal is to transform raw activity logs into clean, analytical datasets that can be used for performance monitoring, test funnel analysis, and learning behavior evaluation.

The pipeline is built using Python and SQLAlchemy, connecting to a PostgreSQL database. It is designed to run hourly and can recover from failures and backfill historical data based on command-line overrides.

---

## 2. Base Tables

These are the core operational tables where raw test data is recorded.

### 2.1 `answers`
- **Description**: Stores every answer submitted by a student to a question.
- **Columns**:
  - `answer_id`: Unique ID of the answer attempt.
  - `student_id`: Identifier for the student.
  - `question_id`: The question being answered.
  - `test_id`: The test to which the question belongs.
  - `session_id`: The session during which the answer was submitted.
  - `submitted_option`: Option chosen by the student.
  - `timestamp`: Time of the answer submission.

### 2.2 `questions`
- **Description**: Master table containing correct options for each question.
- **Columns**:
  - `question_id`: Unique ID of the question.
  - `correct_option`: The correct answer for the question.

### 2.3 `sessions`
- **Description**: Tracks each test session including expiry times.
- **Columns**:
  - `session_id`: Unique session identifier.
  - `expiry_time`: The time after which a session is no longer valid.

### 2.4 `submissions`
- **Description**: Final submission record for each test attempt by a student.
- **Columns**:
  - `student_id`: Student attempting the test.
  - `test_id`: The test being submitted.
  - `submit_time`: Time of submission.
  - `completion_status`: Whether the test was completed.

---

## 3. Derived Tables

These are the analytics-ready tables created via the ETL process.

### 3.1 `question_interaction_fact`
- **Granularity**: One record per student-question-test combination.
- **Purpose**: Analyzes how a student interacted with a question (e.g., accuracy, attempts, time spent).
- **Columns**:
  - `student_id`, `test_id`, `question_id`
  - `first_answer_time`, `last_answer_time`
  - `answer_count`: How many times the question was answered.
  - `final_answer_option`: Last option chosen.
  - `is_correct`: Whether the final answer was correct.
  - `revisited`: Whether the student attempted the question more than once.
  - `time_spent`: Time spent between first and last attempts.

### 3.2 `test_summary_fact`
- **Granularity**: One record per student per test.
- **Purpose**: Summarizes performance of a student on a test.
- **Columns**:
  - `student_id`, `test_id`
  - `total_questions`
  - `questions_attempted`
  - `correct_answers`
  - `accuracy_percent`
  - `time_spent`

### 3.3 `test_funnel_fact`
- **Granularity**: One record per test.
- **Purpose**: Used to build funnel metrics on test completion rates.
- **Columns**:
  - `test_id`
  - `total_submissions`
  - `completed_submissions`
  - `incomplete_submissions`
  - `completion_rate`
  - `avg_submit_time`

---

## 4. Project Structure

MCQTestETL/
│
├── .env # Environment variables (DB creds, Slack alert URL, etc.)
├── create_tables.py # SQL schema for base and derived tables
├── run_etl.py # Main ETL driver script
│
├── utils/
│ ├── init.py
│ ├── alerting.py # send_alert function via Slack/email
│ ├── db.py # get_engine based on env (dev/prod/test)
│
├── etl_utils/
│ ├── init.py
│ ├── time_utils.py # get_time_range logic with override support
│ ├── metadata.py # Functions for metadata tracking


---

## 5. Python Function Highlights

### utils.py
- `get_engine(env: str)`: Returns SQLAlchemy engine for a given environment (reads from `.env`).
- `send_alert(message: str)`: Sends error/critical alert to Slack or email.

### etl_utils.py
- `get_time_range(engine, pipeline_name, start_override, end_override)`: Computes the time window for each ETL run. Uses overrides if passed or defaults to metadata fallback. Sends alert if last two runs failed.
- `get_last_successful_runs(engine, pipeline_name, limit=2)`: Queries `etl_metadata` table to find last successful or failed runs.

- `initialize_spark_session_with_scaling(cluster_name,record_count)`: Initializes and returns a SparkSession instance configured for dynamic resource allocation and adaptive query execution based on the input data load.
- Enables **Dynamic Allocation** to scale executor resources based on the load.
- Activates **Adaptive Query Execution (AQE)** for automatic optimization of joins and partition sizes.
- Configures Spark to:
    - Coalesce small partitions dynamically.
    - Handle skewed joins efficiently.
    - Use reasonable defaults for executor memory and cores
---

