from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime
from pathlib import Path
import shutil


# =========================================================
# 1. UPLOAD MOVIES CSV FILES TO S3 RAW
# =========================================================

def upload_movie_csvs():

    input_folder = Path("/opt/airflow/dags/movies_input")
    processed_folder = Path("/opt/airflow/dags/movies_processed")

    processed_folder.mkdir(exist_ok=True)

    s3 = S3Hook(
        aws_conn_id="aws_connection"
    )

    csv_files = list(input_folder.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            "No CSV files found in movies_input"
        )

    for file in csv_files:

        print(f"Uploading {file.name} to S3...")

        s3.load_file(
            filename=str(file),
            key=f"raw/{file.name}",
            bucket_name="sagar-movies-data-project",
            replace=True,
        )

        shutil.move(
            str(file),
            str(processed_folder / file.name)
        )

        print(
            f"{file.name} uploaded and moved to movies_processed"
        )


# =========================================================
# 2. EXPORT DATABRICKS GOLD → S3 GOLD
# =========================================================

def export_gold_to_s3():

    hook = DatabricksSqlHook(
        databricks_conn_id="databricks_connection"
    )

    connection = hook.get_conn()
    cursor = connection.cursor()

    try:

        print("Reading Gold table from Databricks...")

        cursor.execute("""
            SELECT
                genre,
                total_movies,
                average_score,
                total_gross
            FROM gold_genre_analytics
            ORDER BY genre
        """)

        rows = cursor.fetchall()

        print(
            f"Rows received from Gold table: {len(rows)}"
        )

        # Create CSV content
        csv_content = (
            "genre,total_movies,average_score,total_gross\n"
        )

        for row in rows:

            csv_content += ",".join(
                "" if value is None else str(value)
                for value in row
            ) + "\n"

        # Upload Gold CSV to S3
        s3 = S3Hook(
            aws_conn_id="aws_connection"
        )

        s3.load_string(
            string_data=csv_content,
            key="gold/genre_analytics.csv",
            bucket_name="sagar-movies-data-project",
            replace=True,
        )

        print(
            "Gold data successfully uploaded to S3"
        )

    finally:

        cursor.close()
        connection.close()


# =========================================================
# 3. AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="movies_s3_pipeline",

    start_date=datetime(2026, 9, 19),

    schedule=None,

    catchup=False,

) as dag:

    # =====================================================
    # TASK 1
    # CSV → S3 RAW
    # =====================================================

    upload_movies = PythonOperator(

        task_id="upload_movies_to_s3",

        python_callable=upload_movie_csvs,
    )


    # =====================================================
    # TASK 2
    # S3 RAW → DATABRICKS
    # Bronze → Silver → Gold
    # =====================================================

    run_databricks_pipeline = DatabricksSubmitRunOperator(

        task_id="run_databricks_pipeline",

        databricks_conn_id="databricks_connection",

        pipeline_task={
            "pipeline_id": "0c8062f0-eef9-431b-b9d4-f7d77596d8bf"
        },
    )


    # =====================================================
    # TASK 3
    # DATABRICKS GOLD → S3 GOLD
    # =====================================================

    export_gold = PythonOperator(

        task_id="export_gold_to_s3",

        python_callable=export_gold_to_s3,
    )


    # =====================================================
    # TASK 4
    # S3 GOLD → SNOWFLAKE
    # =====================================================

    load_gold = SQLExecuteQueryOperator(

        task_id="load_gold_to_snowflake",

        conn_id="movies_snowflake_connection",

        sql="""

        TRUNCATE TABLE
        MOVIES_DB.GOLD.GENRE_ANALYTICS;


        COPY INTO MOVIES_DB.GOLD.GENRE_ANALYTICS

        FROM @MOVIES_DB.GOLD.MOVIES_GOLD_STAGE

        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
        )

        PATTERN = '.*genre_analytics\\.csv'

        FORCE = TRUE

        ON_ERROR = 'CONTINUE';

        """,

    )


# =========================================================
# 4. TASK ORDER
# =========================================================

upload_movies >> run_databricks_pipeline >> export_gold >> load_gold