import polars as pl
# import pandas as pd
import random,os,datetime,time
from faking_sources.modulars import *
# from modulars import *

# --- Cấu hình Cố định ---
NUM_RECORDS_TO_GENERATE = 5000 #Ghi đè biến từ modulars.py(flexible)

# --- 2. Task: Tạo DataFrame Chính ---
def generate_customer_dataframe(start_id: int = 0) -> pl.DataFrame:
    """
    Tạo DataFrame với dữ liệu khách hàng.
    """
    print("--- TASK: Tạo Customer DataFrame ---")
    start_time = time.time()
    N = NUM_RECORDS_TO_GENERATE
    ids = list(range(start_id+1, start_id+1 + N))
    valid_mem_ids = get_available_csv_data(MEMBERSHIP_FILE, which_cols="id").to_list()
    start_date = datetime.date.today() - datetime.timedelta(days=5 * 365)
    end_date = datetime.date.today()
    
    # Generate Vietnamese first names
    def get_vietnamese_first_name():
        return fake.name().split()[-1]

    def get_vietnamese_middle_name():
        # Giả sử tên đầy đủ: Họ Middle Tên
        parts = fake.name().split()
        if len(parts) > 2:
            return " ".join(parts[1:-1])
        return ""

    prev_mem_id_flags = [random.random() < 0.2 for _ in range(N)]
    prev_mem_id_values = []
    current_mem_id_values = []
    registration_dates = []
    for i, flag in enumerate(prev_mem_id_flags):
        if flag:
            prev_mem_id_values.append(1)
            current_mem_id_values.append(1)
            # Đăng ký gần hiện tại cho hạng Common
            reg_date = fake.date_between(start_date=end_date - datetime.timedelta(days=365), end_date=end_date)
            registration_dates.append(reg_date)
        else:
            # Chọn current_mem_id ngẫu nhiên từ 2 đến max
            cur_mem = random.choice(valid_mem_ids[1:])
            current_mem_id_values.append(cur_mem)
            prev_mem_id_values.append(cur_mem - 1)
            years_ago = 1 + int(cur_mem / len(valid_mem_ids) * 4)
            reg_date = fake.date_between(start_date=end_date - datetime.timedelta(days=years_ago * 365), end_date=end_date - datetime.timedelta(days=(5-cur_mem)*60))
            registration_dates.append(reg_date)

    df_base = pl.DataFrame({
        "id": ids,
        "cus_name": [
            (fake.last_name() + (" " + get_vietnamese_middle_name() if get_vietnamese_middle_name() else "") + " " + get_vietnamese_first_name()).replace("  ", " ")
            for _ in range(N)
        ],
        "registration_date": registration_dates,
        "current_mem_id": current_mem_id_values,
    })

    # prev_mem_id và current_mem_id đã được sinh ở trên
    df_final = df_base.with_columns([
        pl.Series(prev_mem_id_values).alias("prev_mem_id"),
    ])

    # Thêm cột last_updated_at (sử dụng random_date_after helper)
    def random_date_after(reg_date):
        days_diff = (end_date - reg_date).days
        if days_diff <= 0: return reg_date
        return reg_date + datetime.timedelta(days=random.randint(0, days_diff))
    
    df_final = df_final.with_columns(
        pl.Series([random_date_after(d) if d is not None else None for d in df_final['registration_date'].to_list()]).alias("last_updated_at")
    ).select([
        "id",
         "cus_name", "current_mem_id", "prev_mem_id", "last_updated_at", "registration_date"
    ])
    df_final = df_final.with_columns(
		pl.col("current_mem_id").fill_nan(None).cast(pl.Int64, strict=False),
        pl.col("prev_mem_id").fill_nan(None).cast(pl.Int64, strict=False)
	)
    # Trả về DataFrame (Airflow sẽ tự động lưu vào XCom dưới dạng serializable object)
    end_time = time.time()
    print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
    return df_final

def main():
    #Thêm cơ chế kiểm tra xem file đã tồn tại chưa để lấy start_id, nếu rồi không chạy nữa - end cho ngày hôm nay
    runflag = verify_daily_rate_limit("customers")
    if runflag:
        print("⚠️ Dữ liệu đã được tạo cho ngày hôm nay. Kết thúc quá trình sinh dữ liệu.")
        return
    #Lấy id lastest từ lần chạy trước của bang customers
    start_id = 0
    try:
        last_id = get_data_from_parquet("customers", "id")
        last_id = last_id['id'].tail(1).item()
        start_id = int(last_id)
        print(f"Last customer id found: {start_id}")
    except pl.exceptions.ColumnNotFoundError:
        print("Col 'id' does not exist within table 'customers'. Will start from id = 0.")
    except FileNotFoundError:
        print("Table 'customers' does not exist. Will start from id = 0.")
    # print(start_id)
    # Tạo DataFrame khách hàng mới
    customer_df = generate_customer_dataframe(start_id=start_id)

    write_dataframe_to_parquet(customer_df,table_name="customers")
    print("✅ Customer data generation and write completed!")

if __name__ == "__main__":
    main()
