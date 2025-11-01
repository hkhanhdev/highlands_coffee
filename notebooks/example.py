from prefect import flow, task
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import os

# =====================================
# Kết nối tới Dask Scheduler
# =====================================
def get_dask_client():
    return Client("tcp://dask-scheduler:8786")


# =====================================
# Task 1: Tạo dataset mẫu
# =====================================
@task
def create_data():
    data = pd.DataFrame({
        "city": ["Hanoi", "Hanoi", "Saigon", "Saigon", "Danang"],
        "temperature": [32, 34, 31, 33, 30],
        "humidity": [70, 65, 80, 78, 75]
    })
    return data


# =====================================
# Task 2: Xử lý dữ liệu bằng Dask
# =====================================
@task
def process_with_dask(data):
    client = get_dask_client()
    print("Connected to Dask:", client)

    # Chuyển pandas -> dask dataframe
    ddf = dd.from_pandas(data, npartitions=2)

    # Tính trung bình theo city
    result = ddf.groupby("city").mean().compute()
    print("Processed result:\n", result)
    return result.reset_index()


# =====================================
# Task 3: Ghi kết quả lên MinIO (dưới dạng Parquet)
# =====================================
@task
def save_to_minio(result):
    import duckdb

    # Sử dụng DuckDB ghi thẳng ra MinIO
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_use_ssl=false;
    """)

    # Ghi file parquet
    con.register("result_df", result)
    con.execute("""
        COPY result_df TO 's3://mycatalog/results/avg_temp.parquet'
        (FORMAT PARQUET, OVERWRITE TRUE)
    """)
    print("✅ Saved to MinIO successfully.")


# =====================================
# Flow Prefect
# =====================================
@flow(name="dask-duckdb-pipeline")
def main_flow():
    data = create_data()
    result = process_with_dask(data)
    save_to_minio(result)


if __name__ == "__main__":
    main_flow()

