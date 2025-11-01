from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="source_to_raw",
    start_date=datetime(2025, 10, 11),
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    # Define SparkSubmitOperator task
    spark_test_task = SparkSubmitOperator(
        task_id="customers_spark_job",
        application="/opt/airflow/dags/s_2_r/customers.py",  # Path to Spark job
        conn_id="spark_local",  # Spark connection ID
    )

    # Define task dependencies (only one task in this case)
    spark_test_task
