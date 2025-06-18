import pandas as pd
from datetime import datetime
from sqlalchemy import text
from ..utils import get_engine, send_alert
from ..etl_utils import get_time_range
import argparse

DATETIME_FORMAT = "%Y:%m:%d %H"
PIPELINE_NAME = "test_summary_fact_hourly"

def parse_args():
    parser = argparse.ArgumentParser(description="Running test_summary_fact_hourly")
    parser.add_argument("--env", help="db environment", required=True)
    parser.add_argument("--backfill_start_time", help="Start time override", required=False)
    parser.add_argument("--backfill_end_time", help="End time override", required=False)
    return parser.parse_args()

args = parse_args()

env = args.env
BACKFILL_START_TIME = (
    datetime.strptime(args.backfill_start_time, DATETIME_FORMAT) if args.backfill_start_time else None
)
BACKFILL_END_TIME = (
    datetime.strptime(args.backfill_end_time, DATETIME_FORMAT) if args.backfill_end_time else None
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
    print(f"[INFO] ETL from {start_time} to {end_time}")

    query = f"""
    SELECT a.*, q.correct_option, sub.submit_time, sub.completion_status, s.start_time as session_start
    FROM answers a
    JOIN questions q ON a.question_id = q.question_id
    JOIN sessions s ON a.session_id = s.session_id
    LEFT JOIN submissions sub ON a.student_id = sub.student_id AND a.test_id = sub.test_id
    WHERE a.timestamp >= '{start_time}' AND a.timestamp < '{end_time}'
    """
    df = pd.read_sql(text(query), engine)

    if not df.empty:
        df['is_correct'] = df['submitted_option'] == df['correct_option']

        agg_df = (
            df.groupby(['student_id', 'test_id'])
            .agg(
                test_start_time=('session_start', 'min'),
                test_submit_time=('submit_time', 'max'),
                completion_status=('completion_status', 'last'),
                total_sessions=('session_id', pd.Series.nunique),
                total_questions=('question_id', pd.Series.nunique),
                questions_answered=('question_id', 'count'),
                questions_revisited=('question_id', lambda x: x.duplicated().sum()),
                total_time_spent=('timestamp', lambda x: (x.max() - x.min()).total_seconds()),
                score=('is_correct', 'mean'),
            )
            .reset_index()
        )

        agg_df['avg_time_per_question'] = agg_df['total_time_spent'] / agg_df['questions_answered']
        agg_df.to_sql("test_summary_fact", engine, if_exists="append", index=False)

    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO etl_metadata (pipeline_name, last_run_time, status)
            VALUES (:name, :run_time, :status)
            ON CONFLICT (pipeline_name) DO UPDATE SET last_run_time = :run_time, status = :status
        """), {
            "name": PIPELINE_NAME,
            "run_time": end_time.strftime(DATETIME_FORMAT),
            "status": "success"
        })

if __name__ == "__main__":
    try:
        run_hourly_etl()
    except Exception as e:
        send_alert(f"[ERROR] `{PIPELINE_NAME}` failed: {e}")
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_metadata (pipeline_name, last_run_time, status)
                VALUES (:name, :run_time, :status)
                ON CONFLICT (pipeline_name) DO UPDATE SET last_run_time = :run_time, status = :status
            """), {
                "name": PIPELINE_NAME,
                "run_time": datetime.utcnow().strftime(DATETIME_FORMAT),
                "status": "failed"
            })
        raise
