from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType
from pyiceberg.table import Table
import pandas as pd

# --- 1️⃣ Tạo catalog REST ---
catalog = load_catalog(
    "rest",
    **{
        "uri": "http://iceberg-rest:8181",
        "s3.endpoint": "http://minio_dd:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
        "warehouse": "s3://mycatalog/",
    },
)
# print(catalog.list_tables("weather_data_duck"))

# --- 2️⃣ Tạo schema Iceberg ---
schema = Schema(
    NestedField(1, "name", StringType(), required=True),
    NestedField(2, "temperature", DoubleType()),
)
identifier = ("weather_data_duck", "data_duck")

if identifier in catalog.list_tables("weather_data_duck"):
    table = catalog.load_table(identifier)
    print("✅ Table already exists, loaded:", table.name())
else:
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        location="s3://mycatalog/weather_data_duck",
    )
    print("✅ Table created:", table.name())

# # --- 3️⃣ Tạo table mới trong MinIO qua REST Catalog ---
# catalog.create_table(
#     identifier=("weather_data_duck", "data_duck"),
#     schema=schema,
#     location="s3://mycatalog/weather_data_duck",
# )

# --- 4️⃣ Ghi dữ liệu với DuckDB ---
import duckdb

df = pd.DataFrame({
    "name": ["Hanoi", "HCM", "Danang"],
    "temperature": [29.5, 31.2, 28.0],
})

con = duckdb.connect()
con.execute("""
INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-east-1';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_url_style='path';
SET s3_use_ssl=false;
""")
con.register("df", df)

# Ghi dữ liệu vào Iceberg table thông qua PyIceberg metadata
con.execute("""
COPY df TO 's3://mycatalog/weather_data_duck/data.parquet'
    (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
""")


