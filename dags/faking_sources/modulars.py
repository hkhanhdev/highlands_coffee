from pathlib import Path
from faker import Faker
import time, os, pyodbc, polars as pl, pandas as pd
from dotenv import load_dotenv
# Khởi tạo Faker
fake = Faker('vi_VN')
# Load environment variables from .env file at project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))
FAKE_DATA_DIR = Path("/opt/source_data/oltp_data")
# FAKE_DATA_DIR.mkdir(exist_ok=True)
TRANSACTIONS_FILE = FAKE_DATA_DIR / "customer_transactions_data.csv" 
PRODUCTS_FILE = FAKE_DATA_DIR / "products.csv"
PRODUCT_DETAILS_FILE = FAKE_DATA_DIR / "product_details.csv"
CUSTOMERS_FILE = FAKE_DATA_DIR / "customers.csv" 
PAYMENT_METHODS_FILE = FAKE_DATA_DIR / "payment_methods.csv"
MEMBERSHIP_FILE = FAKE_DATA_DIR / "memberships.csv"
COUPONS_FILE = FAKE_DATA_DIR / "coupons.csv"
STORE_GEOLOCATIONS_FILE = FAKE_DATA_DIR / "store_geolocations.csv"
TRANSACTION_DETAILS_FILE = FAKE_DATA_DIR / "transaction_details.csv"
OUTPUT_PATH = "/opt/source_data"#"D:\\DE_Projects\\highlands_coffee\\source_data\\"
NUM_RECORDS_TO_GENERATE = 10

def get_max_csv_id(DESTINATION_FILE) -> int:
	"""
	Lấy id lớn nhất từ file destination_file
	"""
	print("--- TASK: Get Max Recorded ID ---")
	start_time = time.time()
	if not DESTINATION_FILE.exists():
		max_id = 0
	else:
		try:
			df_last = pl.scan_csv(DESTINATION_FILE).select('id').tail(1).collect()
			if df_last.height == 0:
				max_id = 0
			else:
				max_id = df_last['id'][0]
		except Exception as e:
			print(f"Warning: Could not read existing csv IDs due to error: {e}")
			max_id = 0
	end_time = time.time()
	print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
	print(f"Max recorded id: {max_id}")
	return max_id

def get_available_csv_data(DESTINATION_FILE:str,which_cols:str="*") -> pl.DataFrame:
    print("--- TASK: Get Available Data ---")
    start_time = time.time()
    if not DESTINATION_FILE.exists():
        raise FileNotFoundError(f"ERROR: Data file '{DESTINATION_FILE}' does not exist.")
    data = None
    if which_cols == "*":
        data = pl.read_csv(DESTINATION_FILE)
    else:
        data = pl.read_csv(DESTINATION_FILE)[which_cols]
    end_time = time.time()
    print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
    return data

def write_dataframe_to_csv(df: pl.DataFrame,DESTINATION_FILE:str,mode:str="a"):
    """
    Ghi DataFrame ra file CSV ở chế độ Append.
    """
    print("--- TASK: Writing data to CSV (Append Mode) ---")
    start_time = time.time()
    # Kiểm tra id của bản ghi đầu tiên
    # first_id = df[0, "id"] if df.height > 0 else None
    # is_first_run = (first_id == 1)
    df_pd = df.to_pandas()
    df_pd.to_csv(DESTINATION_FILE, mode=mode, header=True, index=False)
    print(f"Written {len(df)} records successfully into: {DESTINATION_FILE}")
    end_time = time.time()
    print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")

def verify_daily_rate_limit(table_name:str):
    # 1. Lấy ngày hôm nay ở định dạng YYYYMMDD
    current_time = time.localtime()
    # Format the time structure into a string "YYYYMMDD"
    today_str = time.strftime("%Y%m%d", current_time)
    # today_str = datetime.now().strftime("%Y%m%d")
    print(f"Hôm nay là: {today_str}")

    dirs_sorted = get_dirs_from_output_path()
    # 2. Kiểm tra xem có thư mục nào tồn tại không và kiểm tra luôn tên dir mới nhất có phải là ngày hôm nay không
    if not dirs_sorted or dirs_sorted[-1].name != today_str:
        print(f"❌ KHÔNG tìm thấy thư mục. Mặc định: CHƯA chạy.")
        return False

    # 3. Lấy thư mục ngày mới nhất
    latest_dir: Path = dirs_sorted[-1]
    latest_dir_name = latest_dir.name
    print(f"Thư mục mới nhất được tìm thấy là: {latest_dir_name}.Tiến hành kiểm tra tệp...")

    # Xây dựng tên tệp mong muốn: <tên_dir>_<tên_bảng>.parquet
    file_name = f"{latest_dir_name}_{table_name}.parquet"
    file_path: Path = latest_dir / file_name

    # 6. Kiểm tra sự tồn tại của tệp
    if file_path.exists() and file_path.is_file():
        print(f"✅ Tệp '{file_name}' đã TỒN TẠI trong thư mục hôm nay. (Đã chạy)")
        return True
    else:
        print(f"❌ Tệp '{file_name}' CHƯA tồn tại trong thư mục hôm nay. (Chưa chạy)")
        return False

def get_dirs_from_output_path(output_path:str=OUTPUT_PATH):
    """
    Lấy danh sách thư mục con trong thư mục output_path, sắp xếp giảm dần theo tên (ngày).
    """
    base_path = Path(output_path)
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"ERROR: Output path '{output_path}' does not exist or is not a directory.")
    dirs = [d for d in base_path.iterdir() if d.is_dir() and d.name.isdigit() and len(d.name) == 8]
    dirs_sorted = sorted(dirs, key=lambda x: x.name, reverse=False)
    return dirs_sorted

def get_single_parquet_data(file_path:str,which_cols:str="*") -> pl.DataFrame:
    # print("--- TASK: Get Available Data from Parquet File ---")
    # start_time = time.time()
    # file_path = Path(file_path)
    # if not file_path.exists() or not file_path.is_file():
    #     raise FileNotFoundError(f"ERROR: Parquet file '{file_path}' does not exist or is not a file.")
    data = None
    if which_cols == "*":
        data = pl.read_parquet(file_path)
    else:
        cols_to_read = [col.strip() for col in which_cols.split(',')]
        data = pl.read_parquet(file_path)[cols_to_read]
    # end_time = time.time()
    # print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
    return data

def get_data_from_parquet(table_name:str,which_cols:str="*",mode:str="lastest") -> pl.DataFrame:
    print("--- TASK: Get Available Data from Parquet ---")
    start_time = time.time()
    dirs_sorted = get_dirs_from_output_path()
    data = pl.DataFrame()
    total_rows = 0
    if mode == "lastest":
        if not dirs_sorted:
            raise FileNotFoundError(f"ERROR: No dated directories found in output path '{OUTPUT_PATH}'.")
        latest_dir = dirs_sorted[-1]
        file_path =  latest_dir / f"{latest_dir.name}_{table_name}.parquet"
        # Đọc file parquet ra
        data = get_single_parquet_data(file_path,which_cols)
        # rows_added = data.shape[0]
        # total_rows += rows_added

    elif mode == "incremental":
        for dirs in dirs_sorted:
            file_path = dirs / f"{dirs.name}_{table_name}.parquet"
            # Đọc từng file
            df_daily = get_single_parquet_data(file_path, which_cols)
            # Nối (append) DataFrame ngày vào DataFrame tích lũy
            # Dùng pl.concat với how="vertical" để nối theo chiều dọc
            data = pl.concat([data, df_daily], how="vertical")
            rows_added = df_daily.shape[0]
            total_rows += rows_added
        print(f"✅ Hoàn tất Incremental Read. Tổng số bản ghi: {total_rows}")
    return data

def write_dataframe_to_parquet(df: pl.DataFrame,table_name:str):
    """
    Ghi DataFrame ra file Parquet.
    """
    print("--- TASK: Writing data to Parquet ---")
    start_time = time.time()
    # Get the current time structure (struct_time)
    current_time = time.localtime()

    # Format the time structure into a string "YYYYMMDD"
    today = time.strftime("%Y%m%d", current_time)
    # output_path = "D:\\DE_Projects\\highlands_coffee\\source_data\\"

    target_dir = os.path.join(OUTPUT_PATH, today) 
    try:
        os.makedirs(target_dir, exist_ok=True)
        print(f"Thư mục đích đã được xác nhận/tạo: {target_dir}")
    except Exception as e:
        print(f"Lỗi khi tạo thư mục: {e}")
        # Thoát nếu không thể tạo thư mục
        exit()
    df.write_parquet(f"{target_dir}/{today}_{table_name}.parquet", compression="snappy")
    print(f"Written {len(df)} records successfully!")
    end_time = time.time()
    print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")

if __name__ == "__main__":
    # dirs = get_dirs_from_output_path()
    # print(dirs[0])
    pass