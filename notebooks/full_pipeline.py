from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import random,os,time
from typing import Optional, Tuple, Dict, List
from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.filesystems import LocalFileSystem
from modules import *
# -------------------------
# Config
# -------------------------
ETL_LOG_PATH = Path("ETL_JOB_LOG.csv")  # change to DB in prod
SOURCE_BASE = Path("/opt/source_data/")  # base folder where source data lives

STAGES = ["source_2_raw","raw_2_enriched","enriched_2_curated"]
SMALL_TABLES = [
    "coupons",
    "memberships",
    "payment_methods",
    "products",
    "product_details",
    "store_geolocations",
]

LARGE_TABLES = [
    "customers",
    "customer_transactions",
    "transaction_details",
]

JOB_ORDER = SMALL_TABLES + LARGE_TABLES  # run small -> large

# -------------------------
# Helpers for CSV log
# -------------------------
def _ensure_log_exists():
    if not ETL_LOG_PATH.exists():
        df = pd.DataFrame(columns=["job_name","etl_date","start_time","end_time","status","message"])
        df.to_csv(ETL_LOG_PATH, index=False)

def append_log_record(record: Dict):
    """
    Append a single record (dict) into the CSV log atomically-ish.
    Uses pandas to write header only if file missing.
    """
    _ensure_log_exists()
    df = pd.DataFrame([record])
    # append without header if file exists
    df.to_csv(ETL_LOG_PATH, mode="a", header=not ETL_LOG_PATH.exists(), index=False)

def load_job_log_df() -> pd.DataFrame:
    _ensure_log_exists()
    return pd.read_csv(ETL_LOG_PATH,dtype=str).fillna("")

@task
def discover_jobs(process_date: str = None, stage: str = None,jobs_log_df:pd.DataFrame = None):
    logger = get_run_logger()
    df = jobs_log_df
    jobs_to_run = {}
    # logger.info(f"\n------STEP 1 : DISCOVERING JOBS ------")
    # Determine which dates need processing
    if process_date:
        # logger.info(f"Processing for specific date: {process_date}")
        process_date = "/opt/source_data/" + str(process_date)
        dates_to_process = [Path(process_date)]
    else:
        dates_to_process = getSourceDataDirs(SOURCE_BASE) 
    # logger.info(f"Dates to process: {dates_to_process[0].name[-8:] } - {dates_to_process[-1].name[-8:]}")

    # Assign all df to all_jobs_df (All job log records)
    all_jobs_df = df
    # Append source_date column extracted from message
    all_jobs_df["source_date"] = (
            df["message"]
            .astype(str)
            .str.extract(r"(\d{8})")  # lấy chuỗi YYYYMMDD đầu tiên
        )
    # Append stage column extracted from job_name
    all_jobs_df["stage"] = (
            df["job_name"]
            .astype(str)
            .str.extract(r"^(source_2_raw|raw_2_enriched|enriched_2_curated)")
        )
    # logger.info(all_jobs_df)
    jobs_by_stage = all_jobs_df[
            (all_jobs_df["stage"]  == stage)
        ]
    
    for path in dates_to_process:
        date = path.name[-8:]  # get YYYYMMDD from path
        # logger.info(f"-----------------DATE: {date}----------------")
        jobs_to_run[date] = []

        # Filtering down only a job for this date
        jobs_by_date = all_jobs_df[
            (all_jobs_df["source_date"]  == date)
        ]

        if jobs_by_date.empty or jobs_by_stage.empty: #This date or stage has no computed jobs - RUN ALL JOBS = Append all tables to jobs_to_run[path]
            jobs_to_run[date] = JOB_ORDER
        elif not jobs_by_date.empty or not jobs_by_stage.empty: #THis date or stage has some computed jobs but not all of that are succeeded or failed or pending
            for table in JOB_ORDER:
                job_name = f"{stage}_{table}"
                # Filter to get pending/failed jobs for this job_name 
                job_to_complete = all_jobs_df[
                    (all_jobs_df["job_name"] == job_name)
                    & (all_jobs_df["status"].str.lower() != "success")
                ]
                if job_to_complete.empty:
                    logger.warning(f"Job {job_name} already succeeded for date {date}, skipping.")
                else:
                    logger.info(f"Job {job_name} needs to re-run for date {date}.")
                    jobs_to_run[date].append(table)
        logger.info(f"Found {len(jobs_to_run[date])} pending/failed jobs on date {date}")
    # logger.info(f"------END STEP 1------\n")
    return jobs_to_run


@task
def create_pending_record(job_name: str, process_date: str) -> Dict:
    now = datetime.utcnow()
    return {
        "job_name": job_name,
        "etl_date": now.strftime("%Y%m%d"),
        "start_time": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": "",
        "status": "running",
        "message": process_date
    }

@task
def determine_source(table: str, last_success_date: Optional[str], process_date: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Determine source path and source_date string (YYYYMMDD).
    Returns (source_path_or_None, source_date_or_None)
    """
    logger = get_run_logger()

    if table in SMALL_TABLES:
        # fixed file for small tables
        candidate = SOURCE_BASE / f"oltp_data/{table}.csv"
        if candidate.exists():
            # logger.info(f"Small table source found: {candidate}")
            return str(candidate), process_date 
        else:
            # logger.error(f"Small table source not found: {candidate}")
            return None, None

    if table in LARGE_TABLES:
        # # next target date:
        # if last_success_date:
        #     try:
        #         dt = datetime.strptime(last_success_date, "%Y%m%d")
        #         target = dt + timedelta(days=1)
        #         source_date = target.strftime("%Y%m%d")
        #     except Exception:
        #         # fallback to process_date
        #         source_date = process_date
        # else:
        #     # if never ran before, run for process_date
        #     source_date = process_date
        source_date = process_date
        folder = SOURCE_BASE / source_date
        file = folder / f"{source_date}_{table}.parquet"
        if file.exists():
            # logger.info(f"Large table source found: {file}")
            return str(file), source_date
        else:
            # logger.error(f"Large table source not found at {file}")
            return None, source_date

    # unknown table
    return None, None

@task(retries=1, retry_delay_seconds=2)
def compute_stub(table: str, source_path: str) -> Tuple[bool, int, str]:
    """
    Fake compute function.
    Returns (success: bool, rows_processed: int, message: str)
    Success probability = 0.7
    """
    logger = get_run_logger()
    logger.info(f"Starting fake compute for {table} using {source_path}")
    time.sleep(1.5)  # simulate work

    success = random.random() < 0.9  # 90% success rate
    if success:
        # fake rows processed (for metrics)
        logger.info(f"Fake compute success for {table}")
        return True, "ok"
    else:
        logger.error(f"Fake compute FAILED for {table}")
        return False, "fake_compute_failed"

@task
def finalize_and_append(record: Dict, success: bool, source_date: Optional[str], extra_message: str = ""):
    """
    Finalize record fields and append to ETL_JOB_LOG.csv
    For successful large table run, message stores the source_date processed.
    For failure or no_data, message contains a human message.
    """
    logger = get_run_logger()
    now = datetime.utcnow()
    record["end_time"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    if success:
        record["status"] = "success"
        record["message"] = source_date or extra_message or "done"
        
        logger.info(f"Job {record['job_name']} succeeded.")
    else:
        # failed or no data
        if extra_message:
            record["message"] = f"{source_date}|{extra_message}"
        else:
            record["message"] = f"{source_date}|failed processing source_date"
        record["status"] = "failed"
        logger.info(f"Job {record['job_name']} failed.")

    # Append to CSV
    # Use pandas to append: write header if file not exists
    header = not ETL_LOG_PATH.exists()
    df = pd.DataFrame([record])
    df.to_csv(ETL_LOG_PATH, mode="a", header=header, index=False)

@task
def update_log_record(log_df:pd.DataFrame,record: pd.DataFrame, success: bool, source_date: Optional[str], extra_message: str = ""):
    """
    Update an existing log record (DataFrame with single row).
    For successful large table run, message stores the source_date processed.
    For failure or no_data, message contains a human message.
    """
    logger = get_run_logger()
    now = datetime.utcnow()
    record_idx = record.index[0]
    # Load full log
    full_log_df = log_df
    # Find the index to update, actually this is redundant as we have record_idx
    idx = full_log_df[
        (full_log_df["job_name"] == record.at[record_idx, "job_name"])
        & (full_log_df["etl_date"] == record.at[record_idx, "etl_date"])
        & (full_log_df["start_time"] == record.at[record_idx, "start_time"])
    ].index

    if success:
        record.at[record_idx, "status"] = "success"
        record.at[record_idx, "message"] = source_date or extra_message or "done"
        record.at[record_idx, "etl_date"] = now.strftime("%Y%m%d")
        record.at[record_idx, "start_time"] = now.strftime("%Y-%m-%dT%H:%M:%S")
        record.at[record_idx, "end_time"] = now.strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(f"Job {record.at[record_idx, 'job_name']} succeeded.")
    else:
        # failed or no data
        if extra_message:
            record.at[record_idx, "message"] = f"{source_date}|{extra_message}"
        else:
            record.at[record_idx, "message"] = f"{source_date}|failed processing source_date"
        record.at[record_idx, "status"] = "failed"
        logger.info(f"Job {record.at[record_idx, 'job_name']} failed.")

    if not idx.empty:
        full_log_df.loc[idx, :] = record.values
        full_log_df.to_csv(ETL_LOG_PATH, index=False)
    else:
        logger.error("Failed to find the log record to update.")


@task
def source_to_raw():
    """
    Example task: source to raw transformation.
    """
    start_time = time.time()
    logger = get_run_logger()
    logger.info("Starting source to raw transformation...")
    # Simulate work
    time.sleep(2)
    logger.info("Completed source to raw transformation.")
    end_time = time.time()
    logger.info(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
    return True

# Main Flow
@flow(name="daily_etl_flow")
def daily_etl_flow(process_date: Optional[str] = None):
    """
    Top-level flow to run daily ETL jobs.
    process_date: YYYYMMDD optional. If None -> today in UTC.
    """
    logger = get_run_logger()
    logger.info(f"---------------DAILY ETL FLOW START---------------\n")
    # default to UTC today
    if not process_date:
        process_date = datetime.utcnow().strftime("%Y%m%d")
    # else:
    #     # validate format

    all_logs_df = load_job_log_df()
    all_logs_df["source_date"] = (
            all_logs_df["message"]
            .astype(str)
            .str.extract(r"(\d{8})")  # lấy chuỗi YYYYMMDD đầu tiên
        )
    
    for stage in STAGES:
        logger.info(f"----------PROCESSING STAGE | {stage}----------")
        jobs_to_run = discover_jobs(process_date,stage,all_logs_df)
        for date, tables_need_to_run in jobs_to_run.items():
            # logger.info(f"-----DATE: {date} | Pending Jobs: {tables_need_to_run} -----")
            for table in tables_need_to_run:
                job_name = f"{stage}_{table}"
                logger.info(f"Preparing to run job {job_name} (table={table}) for date={date}")

                # determine source_path for this table/date
                source_path, source_date = determine_source.submit(table, None, date).result()
                # logger.info(f"Source path: {source_path}, table: {table}")

                # if table == "coupons" or table == "memberships":
                func = load_compute_function(stage, table)
                success, compute_msg = func(source_path,table,source_date)
                # success, compute_msg = compute_stub.submit(table, source_path).result()

                #Find last recorded log for this job_name and date that is not success
                last_log = all_logs_df[
                    (all_logs_df["job_name"] == job_name)
                    & (all_logs_df["status"].str.lower() != "success") 
                    & (all_logs_df["source_date"] == date)
                ]
                if last_log.empty:
                    # No previous pending/failed run -> create new pending record
                    record = create_pending_record.submit(job_name, date).result()
                    finalize_and_append.submit(record, success, source_date, compute_msg)
                else:
                    record = last_log
                    update_log_record.submit(all_logs_df,record, success, source_date, compute_msg)

    logger.info("------------------------------------------FINISHED------------------------------------")


def deployment():
    """
    Prefect 3.x: deploy hoặc update flow lên Prefect server.
    Không còn dùng `Deployment`, dùng API `flow.deploy()`.
    """
    os.environ["PREFECT_API_URL"] = "http://prefect:4200/api"  # cho docker env
    # Deploy/update flow
    daily_etl_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="full_pipeline.py:daily_etl_flow",
    ).deploy(
        name="daily_etl_flow",
        work_pool_name="duckdask",
        schedule=CronSchedule(
            cron="0 10 * * *",
            timezone="Asia/Ho_Chi_Minh"
        ),
        description="Daily ETL Pipeline",
        tags=["full_data_pipeline"],
    )
    print("✅ Flow deployed/updated successfully to Prefect server!")


if __name__ == "__main__":
    deployment()