from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="spark_test_dag",
    start_date=datetime(2025, 10, 11),
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    # Define SparkSubmitOperator task
    spark_test_task = SparkSubmitOperator(
        task_id="run_spark_test",
        application="/opt/airflow/spark-scripts/spark_test.py",  # Path to Spark job
        conn_id="spark_local",  # Spark connection ID
        # spark_binary="/opt/spark_home/bin/spark-submit",  # Path to spark-submit
        # conf={
        #     "spark.master": "local[*]",  # Run in local mode
        #     "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        #     "spark.hadoop.fs.s3a.access.key": "minioadmin",
        #     "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        #     "spark.hadoop.fs.s3a.path.style.access": "true",
        #     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # },
        # jars="/opt/spark_home/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark_home/jars/hadoop-aws-3.3.4.jar,/opt/spark_home/jars/aws-java-sdk-bundle-1.12.262.jar",
        # env_vars={
        #     "SPARK_HOME": "/opt/spark_home",
        #     "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
        # },
    )

    # Define task dependencies (only one task in this case)
    spark_test_task
