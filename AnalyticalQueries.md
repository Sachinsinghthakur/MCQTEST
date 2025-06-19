# MCQ Test Analytics Dashboard - SQL Query Documentation

This document outlines the SQL queries used to power the dashboard built for the MCQ Test Analytics platform. Each query supports a specific KPI or analytical question.

---

## 1. KPI: How many tests were started?

```sql
SELECT COUNT(DISTINCT session_id) AS tests_started
FROM sessions;
```

---

## 2. KPI: How many tests were completed?

```sql
SELECT COUNT(*) AS tests_completed
FROM submissions
WHERE completion_status = 'completed';
```

---

## 3. KPI: How many tests were not completed?

```sql
SELECT COUNT(*) AS tests_not_completed
FROM submissions
WHERE completion_status != 'completed';
```

---

## 4. KPI: Reasons for incomplete tests (based on session expiry)

```sql
SELECT s.test_id, COUNT(*) AS expired_sessions
FROM sessions s
LEFT JOIN submissions sub ON s.test_id = sub.test_id AND s.student_id = sub.student_id
WHERE sub.completion_status IS NULL AND NOW() > s.expiry_time
GROUP BY s.test_id;
```

---

## 5. KPI: Average timeline for each question answered (per student or globally)

```sql
SELECT
    question_id,
    AVG(EXTRACT(EPOCH FROM (last_answer_time - first_answer_time))) AS avg_time_spent_seconds
FROM question_interaction_fact
GROUP BY question_id;
```

---

## 6. KPI: Funnel View of Test Performance

```sql
SELECT
    test_id,
    total_submissions,
    completed_submissions,
    incomplete_submissions,
    ROUND((completed_submissions::decimal / NULLIF(total_submissions, 0)) * 100, 2) AS completion_rate
FROM test_funnel_fact;
```

---

## 7. KPI: Session-level stats

```sql
SELECT
    student_id,
    test_id,
    SUM(time_spent) AS total_time_spent,
    COUNT(DISTINCT question_id) AS questions_touched,
    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS total_correct
FROM question_interaction_fact
GROUP BY student_id, test_id;
```

---

## 8. KPI: Question-level stats

### a. Most time-consuming questions

```sql
SELECT
    question_id,
    AVG(time_spent) AS avg_time_spent
FROM question_interaction_fact
GROUP BY question_id
ORDER BY avg_time_spent DESC
LIMIT 10;
```

### b. Most correctly answered questions

```sql
SELECT
    question_id,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct_attempts,
    ROUND((SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)::decimal / COUNT(*)) * 100, 2) AS accuracy_percent
FROM question_interaction_fact
GROUP BY question_id
ORDER BY accuracy_percent DESC
LIMIT 10;
```

### c. Most revisited questions

```sql
SELECT
    question_id,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN revisited THEN 1 ELSE 0 END) AS revisits,
    ROUND((SUM(CASE WHEN revisited THEN 1 ELSE 0 END)::decimal / COUNT(*)) * 100, 2) AS revisit_rate_percent
FROM question_interaction_fact
GROUP BY question_id
ORDER BY revisit_rate_percent DESC
LIMIT 10;
```

---

## Notes

* Time units are in seconds
