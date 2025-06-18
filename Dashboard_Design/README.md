# MCQ Test Analytics Dashboard

## 1. Dashboard Objective

The MCQ Test Analytics Dashboard is designed to provide actionable insights into student test-taking behavior, test funnel performance, and question-level analytics. The dashboard is built to help educators, administrators, and test designers monitor how tests are performed, identify drop-offs, and understand question difficulty and engagement.

---

## 2. Target Users

- Teachers/Test Designers
- Academic Coordinators
- EdTech Analysts
- Product Managers

---

## 3. Analytical Questions Answered

This dashboard addresses the following core questions:

- How many tests were started and completed?
- What is the test completion rate?
- Which tests have high drop-off rates?
- What is the time taken to complete tests?
- What questions are most frequently revisited?
- Which questions are most time-consuming?
- What is the overall accuracy of students per test/question?
- What sessions or tests are currently active or expired?

---

## 4. Key KPIs

### Global KPIs (Top of Dashboard):
- **Total Tests Started**
- **Total Tests Completed**
- **Completion Rate (%)**
- **Avg Time to Submit a Test**
- **Total Submissions**
- **Number of Incomplete Submissions**

### Question-Level KPIs:
- **Most Revisited Questions**
- **Most Incorrectly Answered Questions**
- **Most Time-Consuming Questions**
- **Average Accuracy per Question**

---

## 5. Data Sources

All metrics are derived from the following ETL-processed tables:

- `question_interaction_fact`
- `test_summary_fact`
- `test_funnel_fact`

These CSVs are assumed to be loaded into Sigma for interactive visualization.

---

## 6. Dashboard Structure and Visualizations

### 🧭 Tab 1: Test Funnel Overview

**Purpose**: Understand how users are progressing through the test journey.

- **Bar Chart**: Tests vs. Number of Submissions (completed & incomplete)
- **Funnel Chart**: Total → Attempted → Completed submissions
- **Line Chart**: Completion Rate Over Time
- **Box Plot**: Distribution of Submission Times (per test)

---

### 📊 Tab 2: Test Performance Summary

**Purpose**: High-level summary of how students performed in each test.

- **Table**: `test_summary_fact` with conditional formatting:
  - Accuracy % (green → red scale)
  - Time Spent (hover to display in minutes)
- **Heatmap**: Student vs. Test (Accuracy %)
- **Bar Chart**: Average Time Spent vs. Total Correct Answers per Test

---

### 🧠 Tab 3: Question Behavior Analytics

**Purpose**: Understand which questions are problematic or time-consuming.

- **Bar Chart**: Top 10 Questions by Revisit Count
- **Bar Chart**: Top 10 Most Time-Consuming Questions
- **Scatter Plot**: Accuracy vs. Time Spent (each point = a question)
- **Table**: 
  - Question ID
  - Attempt Count
  - Correct Rate
  - Final Answer Accuracy
  - Time Spent

---

### 🧪 Tab 4: Session and Test Monitoring

**Purpose**: Track session expirations and active test windows.

- **Table**: Sessions nearing expiry (from `sessions.csv`)
- **Bar Chart**: Tests with most expired sessions
- **Timeline**: Test start vs. session expiry

---

## 7. Filters and Interactivity

Add the following global filters:

- Date Range Picker (on `submit_time` or `last_answer_time`)
- Test Selector Dropdown
- Student Selector Dropdown
- Question ID Multi-select
- Completion Status Toggle (Completed / Incomplete)

All visualizations should be drillable and interactive where possible.

---

## 8. Notes

- Update the dashboard daily/hourly using the ETL pipeline outputs.
- Use color gradients and conditional formatting to quickly highlight insights.
- Add descriptions to each tab to make the dashboard self-explanatory.

---

## 9. Next Steps

- Add trend charts for longitudinal student improvement.
- Add export options (PDF, CSV snapshots).
- Include benchmarking between different batches/classes if metadata allows.

