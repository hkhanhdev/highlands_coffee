from pyspark.sql import SparkSession

# # Spark tự động load Iceberg JAR từ /opt/spark_home/jars
CATALOG_NAME = "`highlands-coffee-catalog`"
REST_SERVICE = "iceberg-rest"
MINIO_SERVICE = "minio"

spark = SparkSession.builder \
    .appName("S_2_R_customers_pipeline") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", f"http://{REST_SERVICE}:8181") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_SERVICE}:9000") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", f"http://{MINIO_SERVICE}:9000") \
    .getOrCreate()
# Tất cả Iceberg operations hoạt động bình thường
# spark.sql("CREATE TABLE `highlands-coffee-catalog`.test_db.test_table (id INT, name STRING) USING iceberg")
# spark.sql("INSERT INTO `highlands-coffee-catalog`.test_db.test_table VALUES (1, 'test')")
spark.sql(f"SELECT * FROM {CATALOG_NAME}.test_schema.test_table").show()

spark.stop()