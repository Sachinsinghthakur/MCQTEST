# MCQ Test Analytics Dashboard Proposal

## 1. Dashboard Objective

The MCQ Test Analytics Dashboard provides a high-level and detailed view of how students interact with multiple-choice tests. It helps identify engagement patterns, student accuracy, question difficulty, and session activity. The goal is to enable educators and analysts to make informed decisions based on student behavior and test outcomes.

---

## 2. Analytical Questions Answered

- How many tests are started and completed?
- What is the average time to complete a test?
- Which students are performing well or struggling?
- Which questions are causing confusion or delay?
- Which sessions are active, inactive, or expired?
- Are students improving their scores over time?

---

## 3. Key KPIs (All Tabs Combined)

- Total Tests Started  
- Total Tests Completed  
- Completion Rate (%)  
- Avg Time to Submit a Test  
- Number of Incomplete Submissions  
- Avg Score per Test  
- Most Frequently Attempted Test  
- Peak Submission Hour  
- Most Revisited Question  
- Most Incorrect Question  
- Most Time-Consuming Question  
- Average Question Accuracy  
- Number of Active Sessions  
- Number of Expired Sessions  

---

## 4. Dashboard Structure and Visualizations

### Tab 1: Global & Student KPIs

**Purpose**: View key metrics and progress over time, either overall or for a selected student.

**KPIs Displayed**:
- Total Tests Started
- Total Tests Completed
- Completion Rate
- Avg Score
- Avg Time to Complete Test
- Incomplete Test Count

**Visualizations**:
- Line Chart: Test Accuracy over Time (per student or overall)
- Bar Chart: Number of Tests Attempted (per test)

**Filters**:
- Date Range
- Student ID (default: All Students)
- Test Name

---

### Tab 2: Test Overview

**Purpose**: Provide a test-level view of engagement and performance.

**KPIs Displayed**:
- Tests with Highest/Lowest Completion Rate
- Avg Time per Test
- Number of Test Starters
- Number of Test Completions
- Avg Score per Test

**Visualizations**:
- Bar Chart: Tests vs. Completion Rate
- Bar Chart: Tests vs. Avg Time
- Simple Table: Test Name | Total Attempts | Avg Accuracy

**Filters**:
- Date Range
- Test Name
- Completion Status (Completed / Incomplete)

---

### Tab 3: Question Insights

**Purpose**: Highlight performance and behavior around individual questions.

**KPIs Displayed**:
- Most Revisited Question
- Most Incorrect Question
- Most Time-Consuming Question
- Avg Accuracy Across All Questions

**Visualizations**:
- Bar Chart: Top Questions by Revisit Count
- Bar Chart: Top Questions by Incorrect Rate
- Table: Question ID | Accuracy | Revisit Count | Avg Time

**Filters**:
- Test Name
- Question ID
- Difficulty Level (if available)

---

### Tab 4: Session Monitoring

**Purpose**: Monitor active/inactive test sessions.

**KPIs Displayed**:
- Number of Active Sessions
- Number of Expired Sessions
- Number of Abandoned Sessions
- Avg Session Duration

**Visualizations**:
- Bar Chart: Tests vs. Expired Session Count
- Table: Session ID | Test Name | Start Time | Expiry Time | Status

**Filters**:
- Date Range
- Test Name
- Session Status (Active / Expired)

---

## 5. Filters and Interactivity (Global)

- Date Range Picker  
- Student ID Dropdown  
- Test Name Dropdown  
- Completion Status Toggle  
- Question ID Selector  
- Session Status Toggle  
- Topic/Tag (if metadata available)  

All visualizations are interactive and filterable

---

## 6. Notes

- Dashboard will refresh daily or thrice a day  


---
