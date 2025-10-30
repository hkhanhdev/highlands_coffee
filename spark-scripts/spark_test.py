from pyspark.sql import SparkSession


# Spark tự động load Iceberg JAR từ /opt/spark_home/jars
CATALOG_NAME = "highlands-coffee-catalog"
REST_SERVICE = "172.20.0.5"
MINIO_SERVICE = "172.20.0.4"
spark = SparkSession.builder \
.appName("IcebergDataPipeline") \
.config(f"spark.sql.catalog.{CATALOG_NAME}.uri", f"http://{REST_SERVICE}:8181") \
.config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_SERVICE}:9000") \
.config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", f"http://{MINIO_SERVICE}:9000") \
.getOrCreate()

# Tất cả Iceberg operations hoạt động bình thường
spark.sql("CREATE TABLE `highlands-coffee-catalog`.test_db.test_table (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO `highlands-coffee-catalog`.test_db.test_table VALUES (1, 'test')")
spark.sql("SELECT * FROM spark_catalog.db.test_table").show()

spark.stop()





conf_map = spark.sparkContext.getConf().getAll()
# Lọc và in ra các cấu hình quan trọng, sắp xếp theo tên
conf_map.sort()
for key, value in conf_map:
    # Chỉ in ra các cấu hình liên quan đến Spark SQL, Catalog, S3/Hadoop và Iceberg
    if key.startswith("spark.sql") or key.startswith("spark.hadoop") or key.startswith("spark.cores") or key.startswith("spark.app"):
         print(f"{key}: {value}")

table_name = "`highlands-coffee-catalog`.test_schema.test_table"
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        product_id INT,
        product_name STRING
    )
    USING iceberg
    LOCATION 's3a://highlands-coffee-catalog/test_schema/test_table' 
""")


# Tạo DataFrame chứa dữ liệu mẫu phù hợp với schema mới
data = [
    (1, "Phin Sữa Đá"),
    (2, "Trà Sen Vàng"),
    (3, "Bánh Mì VN")
]
columns = ["product_id", "product_name"]

df = spark.createDataFrame(data, columns)

# Chèn dữ liệu vào bảng Iceberg
try:
    df.writeTo(table_name).append()
    print(f"Đã chèn {df.count()} hàng dữ liệu mới vào bảng '{table_name}'.")
    
    # --- 4. KIỂM TRA DỮ LIỆU ---
    print("\n--- 4. Đọc và Kiểm tra dữ liệu từ bảng ---")
    result_df = spark.table(table_name)
    result_df.show(truncate=False)
    print(f"Tổng số hàng trong bảng: {result_df.count()}")
    
except Exception as e:
    print(f"LỖI khi chèn hoặc đọc dữ liệu: {e}")