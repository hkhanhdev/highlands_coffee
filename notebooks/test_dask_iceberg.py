from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, FloatType, StringType

# Connect to Dask
client = Client("tcp://dask-scheduler:8786")

# Connect to catalog
catalog = load_catalog("mycatalog")

# Define schema
schema = Schema(
    NestedField(1, "station_id", LongType(), required=True),
    NestedField(2, "temperature", FloatType()),
    NestedField(3, "city", StringType()),
)

# Create or load table
try:
    table = catalog.create_table(
        identifier=("default", "weather_data_dask"),
        schema=schema,
        location="s3://mycatalog/weather_data/weather_data_dask",
    )
    print("✅ Table created")
except Exception:
    table = catalog.load_table(("default", "weather_data_dask"))
    print("✅ Table loaded")

# Generate sample data
pdf = pd.DataFrame({
    "station_id": [1, 2, 3, 4],
    "temperature": [28.1, 27.9, 29.3, 30.2],
    "city": ["Hanoi", "Hue", "Danang", "Saigon"],
})
ddf = dd.from_pandas(pdf, npartitions=1)

# Write to MinIO using s3fs
s3_opts = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://minio:9000"},
}

output_path = "s3://mycatalog/weather_data/weather_data_dask/"
ddf.to_parquet(output_path, storage_options=s3_opts, engine="pyarrow")

print("✅ Data written to MinIO:", output_path)
