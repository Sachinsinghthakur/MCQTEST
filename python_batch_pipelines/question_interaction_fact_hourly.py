import pandas as pd
from datetime import datetime
from sqlalchemy import text
from ..utils import get_engine, send_alert
from ..etl_utils import get_time_range
import argparse

DATETIME_FORMAT = "%Y:%m:%d %H"
PIPELINE_NAME = "question_interaction_fact_hourly"

def parse_args():
    parser = argparse.ArgumentParser(description="Running question_interaction_fact_hourly")
    parser.add_argument("--env", help="db environment", required=True)
    parser.add_argument("--backfill_start_time", help="Start time override in 'YYYY:MM:DD HH' format", required=False)
    parser.add_argument("--backfill_end_time", help="End time override in 'YYYY:MM:DD HH' format", required=False)
    return parser.parse_args()

args = parse_args()


env = args.env
BACKFILL_START_TIME = (
    datetime.strptime(args.start, DATETIME_FORMAT) if args.start else None
)
BACKFILL_END_TIME = (
    datetime.strptime(args.end, DATETIME_FORMAT) if args.end else None
)

engine = get_engine(env)


def run_hourly_etl():
    start_time, end_time = get_time_range(
                                            engine=engine,
                                            pipeline_name=PIPELINE_NAME,
                                            start_override=BACKFILL_START_TIME,
                                            end_override=BACKFILL_END_TIME,
                                            datetime_format=DATETIME_FORMAT
                                        )

    print(f"[INFO] Running ETL from {start_time.strftime(DATETIME_FORMAT)} to {end_time.strftime(DATETIME_FORMAT)}")

    query = f"""
    SELECT a.*, q.correct_option, s.expiry_time, sub.submit_time, sub.completion_status
    FROM answers a
    JOIN questions q ON a.question_id = q.question_id
    LEFT JOIN sessions s ON a.session_id = s.session_id
    LEFT JOIN submissions sub ON a.student_id = sub.student_id AND a.test_id = sub.test_id
    WHERE a.timestamp >= '{start_time.strftime(DATETIME_FORMAT)}'::timestamp
      AND a.timestamp < '{end_time.strftime(DATETIME_FORMAT)}'::timestamp
    """
    answers_df = pd.read_sql(text(query), engine)

    if not answers_df.empty:
        answers_df['is_correct'] = answers_df['submitted_option'] == answers_df['correct_option']
        fact_df = (
            answers_df.groupby(['student_id', 'test_id', 'question_id'])
            .agg(
                first_answer_time=('timestamp', 'min'),
                last_answer_time=('timestamp', 'max'),
                answer_count=('answer_id', 'count'),
                final_answer_option=('submitted_option', 'last'),
                is_correct=('is_correct', 'last'),
                revisited=('answer_id', lambda x: len(x) > 1),
            )
            .reset_index()
        )
        fact_df['time_spent'] = (
            fact_df['last_answer_time'] - fact_df['first_answer_time']
        ).dt.total_seconds()

        fact_df.to_sql('question_interaction_fact', engine, if_exists='append', index=False)

    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO etl_metadata (pipeline_name, last_run_time, status)
            VALUES (:name, :run_time, :status)
            ON CONFLICT (pipeline_name) DO UPDATE
            SET last_run_time = :run_time, status = :status
        """), {
            'name': PIPELINE_NAME,
            'run_time': end_time.strftime(DATETIME_FORMAT),
            'status': 'success'
        })

if __name__ == "__main__":
    try:
        run_hourly_etl()
    except Exception as e:
        send_alert(f"[ERROR] ETL Pipeline `{PIPELINE_NAME}` failed: {str(e)}")
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_metadata (pipeline_name, last_run_time, status)
                VALUES (:name, :run_time, :status)
                ON CONFLICT (pipeline_name) DO UPDATE
                SET last_run_time = :run_time, status = :status
            """), {
                'name': PIPELINE_NAME,
                'run_time': datetime.utcnow().strftime(DATETIME_FORMAT),
                'status': 'failed'
            })
        raise
