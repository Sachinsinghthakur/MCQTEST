import argparse
from sqlalchemy import text
from datetime import datetime
from ..utils import get_engine
from pyspark.sql.functions import col
from ..etl_utils import initialize_spark_session_with_scaling

DATETIME_FORMAT = "%Y:%m:%d %H"
PIPELINE_NAME = "test_funnel_fact_hourly"

def parse_args():
    parser = argparse.ArgumentParser(description="Run test_funnel_fact_hourly ETL")
    parser.add_argument("--env", required=True, help="Database environment")
    parser.add_argument("--backfill_start_time", required=False, help="Backfill start time in 'YYYY:MM:DD HH' format")
    parser.add_argument("--backfill_end_time", required=False, help="Backfill end time in 'YYYY:MM:DD HH' format")
    return parser.parse_args()

args = parse_args()
env = args.env
start_time = datetime.strptime(args.backfill_start_time, DATETIME_FORMAT) if args.backfill_start_time else None
end_time = datetime.strptime(args.backfill_end_time, DATETIME_FORMAT) if args.backfill_end_time else None


def estimate_input_size(start_time, end_time, engine):
    query = text("""
        SELECT COUNT(*) as row_count
        FROM tests
        WHERE start_time >= :start_time AND start_time < :end_time
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'start_time': start_time,
            'end_time': end_time
        }).fetchone()
        return result.row_count


def run_etl():
    engine = get_engine(env)
    cluster_name = 'spark_test_funnel_fact_hourly_'+str(env)
    if not start_time or not end_time:
        raise ValueError("Both start_time and end_time must be provided for hourly ETL.")

    print(f"[INFO] Running ETL from {start_time} to {end_time}")
    record_count = estimate_input_size(start_time, end_time, engine)

    spark = initialize_spark_session_with_scaling(cluster_name,record_count)

    df = spark.read \
        .format("jdbc") \
        .option("url", engine.url.render_as_string(hide_password=False)) \
        .option("dbtable", f"""
            (
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
            ) AS funnel_data
        """) \
        .option("user", engine.url.username) \
        .option("password", engine.url.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    if df.rdd.isEmpty():
        print("[INFO] No records found in the selected time range.")
        return

    df = df.withColumn("students_enrolled", col("students_started") + col("students_dropped_midway"))

    df.write \
        .format("jdbc") \
        .option("url", engine.url.render_as_string(hide_password=False)) \
        .option("dbtable", "test_funnel_fact") \
        .option("user", engine.url.username) \
        .option("password", engine.url.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

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
        run_etl()
    except Exception as e:
        print(f"[ERROR] ETL failed: {str(e)}")
        engine = get_engine(env)
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
