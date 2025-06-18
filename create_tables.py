from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

robot_user_name = os.getenv("robot_user_name")
robot_user_password = os.getenv("robot_user_password")
DB_URL = f"postgresql+psycopg2://{robot_user_name}:{robot_user_password}@localhost:5432/mcq_test_db"
engine = create_engine(DB_URL)

table_queries = [

    """
    CREATE TABLE IF NOT EXISTS students (
        student_id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS teachers (
        teacher_id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS tests (
        test_id SERIAL PRIMARY KEY,
        teacher_id INT REFERENCES teachers(teacher_id),
        title TEXT,
        start_time TIMESTAMP,
        deadline TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS questions (
        question_id SERIAL PRIMARY KEY,
        test_id INT REFERENCES tests(test_id),
        sequence_no INT,
        question_text TEXT,
        options JSONB,
        correct_option TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        session_id SERIAL PRIMARY KEY,
        student_id INT REFERENCES students(student_id),
        test_id INT REFERENCES tests(test_id),
        start_time TIMESTAMP,
        expiry_time TIMESTAMP,
        status TEXT  -- active, expired
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS answers (
        answer_id SERIAL PRIMARY KEY,
        student_id INT REFERENCES students(student_id),
        test_id INT REFERENCES tests(test_id),
        question_id INT REFERENCES questions(question_id),
        session_id INT REFERENCES sessions(session_id),
        submitted_option TEXT,
        timestamp TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS submissions (
        submission_id SERIAL PRIMARY KEY,
        student_id INT REFERENCES students(student_id),
        test_id INT REFERENCES tests(test_id),
        submit_time TIMESTAMP,
        completion_status TEXT  -- completed, timeout, skipped
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS etl_metadata (
        pipeline_name TEXT PRIMARY KEY,
        last_run_time TIMESTAMP
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS question_interaction_fact (
        student_id INT,
        test_id INT,
        question_id INT,
        first_answer_time TIMESTAMP,
        last_answer_time TIMESTAMP,
        answer_count INT,
        final_answer_option TEXT,
        is_correct BOOLEAN,
        revisited BOOLEAN,
        time_spent DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS test_summary_fact (
        student_id INT,
        test_id INT,
        test_start_time TIMESTAMP,
        test_submit_time TIMESTAMP,
        completion_status TEXT,
        total_sessions INT,
        total_questions INT,
        questions_answered INT,
        questions_revisited INT,
        avg_time_per_question DOUBLE PRECISION,
        total_time_spent DOUBLE PRECISION,
        score DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS test_funnel_fact (
        test_id INT PRIMARY KEY,
        questions_created INT,
        students_enrolled INT,
        students_started INT,
        students_completed INT,
        students_answered_all INT,
        students_dropped_midway INT
    );
    """
]

def create_all_tables():
    with engine.connect() as conn:
        for query in table_queries:
            conn.execute(text(query))

if __name__ == "__main__":
    create_all_tables()
