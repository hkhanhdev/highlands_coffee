# file: faking_pipeline_prefect.py
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.filesystems import LocalFileSystem
import asyncio
import os

# === IMPORT MODULES CỦA BẠN ===
from customer_faking import generate_customer_dataframe
from transactions_faking import generate_transaction_dataframe
from transaction_details_faking import get_transaction_details
from modulars import *

import polars as pl

# ====================================================
# DEFINE TASKS
# ====================================================

@task(retries=3, retry_delay_seconds=60)
def check_daily_limit(table_name: str) -> bool:
    runflag = verify_daily_rate_limit(table_name)
    if runflag:
        get_run_logger().warning(f"Dữ liệu {table_name} đã được tạo hôm nay — skip.")
        return False
    return True


@task(retries=3)
def generate_and_write_customers():
    start_id = 0
    try:
        last_id = get_data_from_parquet("customers", "id")
        start_id = int(last_id["id"].tail(1).item())
        get_run_logger().info(f"Last customer id: {start_id}")
    except Exception:
        get_run_logger().warning("Bắt đầu từ id = 0 (customers chưa tồn tại)")
    df = generate_customer_dataframe(start_id=start_id)
    write_dataframe_to_parquet(df, "customers")
    get_run_logger().info("✅ Customers generated & written!")


@task(retries=3)
def generate_and_write_transactions():
    start_id = 0
    try:
        last_id = get_data_from_parquet("customer_transactions", "id")
        start_id = int(last_id["id"].tail(1).item())
        get_run_logger().info(f"Last transaction id: {start_id}")
    except Exception:
        get_run_logger().warning("Bắt đầu từ id = 0 (transactions chưa tồn tại)")
    df = generate_transaction_dataframe(start_id=start_id)
    write_dataframe_to_parquet(df, "customer_transactions")
    get_run_logger().info("✅ Transactions generated & written!")


@task(retries=3)
def generate_and_write_transaction_details():
    start_id, order_id = 0, 1
    try:
        transactions_df = get_data_from_parquet("customer_transactions", "*")
        last_df = get_data_from_parquet("transaction_details", "id,order_id")
        start_id = int(last_df["id"].tail(1).item())
        order_id = int(last_df["order_id"].tail(1).item())
    except Exception:
        get_run_logger().warning("Bắt đầu từ id = 0 (transaction_details chưa tồn tại)")
        transactions_df = None

    details_df = get_transaction_details(transactions_df, last_detail_id=start_id, last_order_id=order_id)
    write_dataframe_to_parquet(details_df, "transaction_details")
    get_run_logger().info("✅ Transaction details generated & written!")


# ====================================================
# FLOW CHÍNH
# ====================================================

@flow(name="faking_pipeline")
def faking_pipeline():
    # print("🚀 Bắt đầu flow faking_pipeline...")
    # print("Faking Pipeline is running...")
    # print("Faking Pipeline completed.")
    if check_daily_limit("customers"):
        generate_and_write_customers()
    if check_daily_limit("customer_transactions"):
        generate_and_write_transactions()
    if check_daily_limit("transaction_details"):
        generate_and_write_transaction_details()


# ====================================================
# AUTO DEPLOYMENT LOGIC (Prefect 3.x)
# ====================================================

def deployment():
    """
    Prefect 3.x: deploy hoặc update flow lên Prefect server.
    Không còn dùng `Deployment`, dùng API `flow.deploy()`.
    """
    os.environ["PREFECT_API_URL"] = "http://prefect:4200/api"  # cho docker env
    # Deploy/update flow
    faking_pipeline.from_source(
        source=str(Path(__file__).parent),
        entrypoint="prefect_faking_pipeline.py:faking_pipeline",
    ).deploy(
        name="daily_faking_pipeline",
        work_pool_name="duckdask",  # hoặc "default" nếu bạn dùng pool mặc định
        schedule=CronSchedule(
            cron="0 9 * * *",
            timezone="Asia/Ho_Chi_Minh"
        ),
        description="Fake OLTP data daily for analytics testing",
        tags=["data_faking", "oltp"],
    )
    print("✅ Flow deployed/updated successfully to Prefect server!")


if __name__ == "__main__":
    deployment()
