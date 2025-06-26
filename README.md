# MCQ Test Analytics ETL Pipeline

## 1. Introduction

This project supports analytics and performance monitoring on an MCQ-based test platform. The system is used by students to take tests created and assigned by teachers. The raw logs generated during test attempts are stored in normalized base tables. Our ETL pipeline transforms this data into analytics-ready datasets to support dashboards and decision-making.

The pipeline is written in Python using SQLAlchemy and Pandas for lightweight loads, with PySpark support for large-scale execution. The system is designed to run hourly, recover from failed runs, and allow backfills using CLI arguments. Metadata about pipeline runs is tracked for reliability and transparency.

---

## 2. Base Tables (Operational Data)

These are normalized source tables representing the raw data captured in the platform.

### 2.1 `students`

* **Description**: Stores student information.
* **Columns**:

  * `student_id` (PK): Unique identifier for each student.
  * `student_name`: Full name.
  * `email`: Contact email.
  * `created_at`: Timestamp of account creation.

### 2.2 `teachers`

* **Description**: Stores teacher/creator details.
* **Columns**:

  * `teacher_id` (PK): Unique identifier for each teacher.
  * `teacher_name`: Full name.
  * `email`: Contact email.
  * `department`: Department of the teacher.

### 2.3 `tests`

* **Description**: Metadata of each test created.
* **Columns**:

  * `test_id` (PK): Unique test identifier.
  * `teacher_id` (FK): References `teachers`.
  * `test_name`: Name/title of the test.
  * `created_at`: Creation time.
  * `total_questions`: Number of questions.

### 2.4 `questions`

* **Description**: List of questions tied to a test.
* **Columns**:

  * `question_id` (PK): Unique question identifier.
  * `test_id` (FK): Links to `tests`.
  * `correct_option`: The correct option key (A/B/C/D/etc).
  * `question_text`: Text of the question.

### 2.5 `sessions`

* **Description**: Represents a test-taking session.
* **Columns**:

  * `session_id` (PK): Unique session identifier.
  * `student_id` (FK): Links to `students`.
  * `test_id` (FK): Links to `tests`.
  * `expiry_time`: Validity end of session.

### 2.6 `answers`

* **Description**: Records each answer attempt by a student.
* **Columns**:

  * `answer_id` (PK): Unique ID.
  * `student_id` (FK): Links to `students`.
  * `question_id` (FK): Links to `questions`.
  * `test_id` (FK): Redundant for fast access.
  * `session_id` (FK): Links to `sessions`.
  * `submitted_option`: Option selected.
  * `timestamp`: Time of the answer.

### 2.7 `submissions`

* **Description**: Final test submissions.
* **Columns**:

  * `student_id` (FK): Links to `students`.
  * `test_id` (FK): Links to `tests`.
  * `submit_time`: When the test was submitted.
  * `completion_status`: 'completed' or 'incomplete'.

---

## 3. Derived Tables (Analytics Layer)

Transformed, aggregated tables used to support the dashboard and KPIs.

### 3.1 `question_interaction_fact`

* **Grain**: One record per student-question-test.
* **Columns**:

  * `student_id`, `test_id`, `question_id`
  * `first_answer_time`, `last_answer_time` (timestamp)
  * `answer_count` (int)
  * `final_answer_option` (str)
  * `is_correct` (bool)
  * `revisited` (bool): If student attempted more than once.
  * `time_spent` (float): Seconds between first and last answer.

### 3.2 `test_summary_fact`

* **Grain**: One record per student per test.
* **Columns**:

  * `student_id`, `test_id`
  * `total_questions`
  * `questions_attempted`
  * `questions_revisited`
  * `correct_answers`
  * `avg_time_per_question`
  * `accuracy_percent` (float)
  * `total_sessions`
  * `test_start_time`, `test_submit_time`
  * `completion_status`
  * `time_spent` (float): Total test duration in seconds.

### 3.3 `test_funnel_fact`

* **Grain**: One record per test.
* **Purpose**: Funnel KPIs for all test-takers.
* **Columns**:

  * `test_id`
  * `total_submissions`
  * `completed_submissions`
  * `incomplete_submissions`
  * `completion_rate` (float)
  * `avg_submit_time` (float)

---

## 4. Relationships Summary

* `teachers` → `tests` (1\:M)
* `tests` → `questions` (1\:M)
* `students` → `sessions` (1\:M)
* `sessions` → `answers` (1\:M)
* `answers` → `questions` (M:1)
* `answers` → `submissions` (M:1)
* `students` + `tests` = composite key in `submissions`

---

## 5. Project Structure

```
MCQTestETL/
├── .env                          # Contains database credentials, alert configs
├── python_batch_pipelines/      # Hourly Python ETL pipelines
├── spark_scalable_pipeline/     # Scalable PySpark pipelines
├── utils.py                     # Utility functions (alerts, engine)
├── etl_utils.py                 # ETL metadata handling, time window calc
├── sample_data/                 # CSV files for testing and dashboard
```

---

## 6. Python Function Highlights

### utils.py

* `get_engine(env: str)` – Creates SQLAlchemy engine from `.env`.
* `send_alert(msg: str)` – Sends Slack/email alert on failure.

### etl\_utils.py

* `get_last_successful_runs(engine, pipeline_name, limit)` – Pulls last ETL statuses.
* `get_time_range(engine, pipeline_name, start_override, end_override)` – Determines ETL window dynamically.
* `initialize_spark_session_with_scaling()` – Scales Spark job dynamically based on input size.

---

## 7. How to Run

Run the ETL pipeline manually:

```bash
python run_etl.py --env dev
```

To backfill a specific hour:

```bash
python run_etl.py --env dev --start "2024:06:01 10" --end "2024:06:01 11"
```

```
