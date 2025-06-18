import argparse
from sqlalchemy import text
from datetime import datetime
from ..utils import get_engine
from pyspark.sql.functions import col, min as min_, max as max_, count, last, expr
from ..etl_utils import initialize_spark_session_with_scaling

DATETIME_FORMAT = "%Y:%m:%d %H"

def parse_args():
    parser = argparse.ArgumentParser(description="Run question_interaction_fact_hourly ETL")
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
        FROM answers
        WHERE timestamp >= :start_time AND timestamp < :end_time
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'start_time': start_time,
            'end_time': end_time
        }).fetchone()
        return result.row_count



def run_etl():
    engine = get_engine(env)
    cluster_name = 'spark_question_interaction_fact_hourly_'+str(env)
    if not start_time or not end_time:
        raise ValueError("Both start_time and end_time must be provided for hourly ETL.")

    print(f"[INFO] Running ETL from {start_time} to {end_time}")
    record_count = estimate_input_size(start_time, end_time, engine)

    spark = initialize_spark_session_with_scaling(cluster_name,record_count)

    df = spark.read \
        .format("jdbc") \
        .option("url", engine.url.render_as_string(hide_password=False)) \
        .option("dbtable", f"""
            (SELECT a.*, q.correct_option, s.expiry_time, sub.submit_time, sub.completion_status
             FROM answers a
             JOIN questions q ON a.question_id = q.question_id
             LEFT JOIN sessions s ON a.session_id = s.session_id
             LEFT JOIN submissions sub ON a.student_id = sub.student_id AND a.test_id = sub.test_id
             WHERE a.timestamp >= '{start_time}' AND a.timestamp < '{end_time}'
            ) as answer_data
        """) \
        .option("user", engine.url.username) \
        .option("password", engine.url.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    if df.rdd.isEmpty():
        print("[INFO] No records found in the selected time range.")
        return


    df = df.withColumn("is_correct", col("submitted_option") == col("correct_option"))

    grouped_df = df.groupBy("student_id", "test_id", "question_id").agg(
        min_("timestamp").alias("first_answer_time"),
        max_("timestamp").alias("last_answer_time"),
        count("answer_id").alias("answer_count"),
        last("submitted_option").alias("final_answer_option"),
        last("is_correct").alias("is_correct"),
        expr("count(answer_id) > 1").alias("revisited")
    )

    grouped_df = grouped_df.withColumn(
        "time_spent",
        (col("last_answer_time").cast("long") - col("first_answer_time").cast("long"))
    )

    grouped_df.write \
        .format("jdbc") \
        .option("url", engine.url.render_as_string(hide_password=False)) \
        .option("dbtable", "question_interaction_fact") \
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
            'name': "question_interaction_fact_hourly",
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
                'name': "question_interaction_fact_hourly",
                'run_time': datetime.utcnow().strftime(DATETIME_FORMAT),
                'status': 'failed'
            })
        raise
