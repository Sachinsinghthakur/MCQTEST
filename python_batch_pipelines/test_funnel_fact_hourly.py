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
    print(f"[INFO] Funnel ETL from {start_time} to {end_time}")

    query = f"""
    SELECT t.test_id,
        (SELECT COUNT(*) FROM questions q WHERE q.test_id = t.test_id) AS questions_created,
        (SELECT COUNT(DISTINCT student_id) FROM sessions s WHERE s.test_id = t.test_id) AS students_started,
        (SELECT COUNT(DISTINCT student_id) FROM submissions sub WHERE sub.test_id = t.test_id AND sub.completion_status = 'completed') AS students_completed,
        (SELECT COUNT(DISTINCT a.student_id)
         FROM answers a
         WHERE a.test_id = t.test_id
         GROUP BY a.student_id
         HAVING COUNT(DISTINCT a.question_id) = (SELECT COUNT(*) FROM questions WHERE test_id = t.test_id)
        ) AS students_answered_all,
        (SELECT COUNT(DISTINCT student_id) FROM submissions sub WHERE sub.test_id = t.test_id AND sub.completion_status != 'completed') AS students_dropped_midway
    FROM tests t
    WHERE t.start_time >= '{start_time}' AND t.start_time < '{end_time}'
    """
    df = pd.read_sql(text(query), engine)
    df['students_enrolled'] = df['students_started'] + df['students_dropped_midway']
    df.to_sql("test_funnel_fact", engine, if_exists="append", index=False)

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
