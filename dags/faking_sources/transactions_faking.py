import polars as pl
# import pandas as pd
import random,os,datetime,time
from faking_sources.modulars import *
# from modulars import *

NUM_RECORDS_TO_GENERATE = 100000 #Ghi đè biến từ modulars.py(flexible)

def generate_transaction_dataframe(start_id:int=0) -> pl.DataFrame:
	# Đọc bảng coupons
	coupons_df = get_available_csv_data(COUPONS_FILE)
	# Đọc bảng store_geolocations
	store_ids = get_available_csv_data(STORE_GEOLOCATIONS_FILE, which_cols="id").to_list()
	# Lấy danh sách id phương thức thanh toán
	payment_method_ids = get_available_csv_data(PAYMENT_METHODS_FILE,which_cols="id").to_list()
	# Lấy danh sách id khách hàng
	# customers = get_available_cloud_data("customers", which_cols="*")
	customers = get_data_from_parquet("customers",mode="incremental", which_cols="*")
	customer_ids = customers['id'].to_list()
	# Chuẩn bị dict lưu số lần sử dụng coupon cho mỗi khách hàng
	coupon_usage = {}
	print("--- TASK: Generate Transaction DataFrame ---")
	start_time = time.time()
	N = NUM_RECORDS_TO_GENERATE
	now = datetime.datetime.now().replace(microsecond=0)
	order_dates = [now.strftime('%Y-%m-%d %H:%M:%S') for _ in range(N)]
	cus_ids = [random.choice(customer_ids) for _ in range(N)]
	store_id_values = [random.choice(store_ids) for _ in range(N)]
	coupon_id_values = []
	for i in range(N):
		cus_id = cus_ids[i]
		# Lấy current_mem_id của khách hàng này
		try:
			customer_row = customers.filter(pl.col('id') == cus_id)
			if customer_row.height == 0:
				current_mem_id = None
			else:
				current_mem_id = customer_row['current_mem_id'][0]
		except Exception:
			current_mem_id = None
		# Lọc các coupon hợp lệ cho khách hàng này
		available_coupons = coupons_df.filter(
			(pl.col('effective_member_id') == current_mem_id) &
			(pl.col('expiry_date') >= now.strftime('%Y-%m-%d'))
		)
		# Chọn coupon chưa vượt quá usage_limit
		chosen_coupon_id = None
		for idx in range(available_coupons.height):
			coupon_id = available_coupons['id'][idx]
			# Cast coupon_id to int if not None
			if coupon_id is not None:
				coupon_id = int(coupon_id)
			usage_limit = available_coupons['usage_limit'][idx]
			key = (cus_id, coupon_id)
			used = coupon_usage.get(key, 0)
			# Nếu usage_limit là None hoặc NaN, coi như không giới hạn
			if usage_limit is None or (isinstance(usage_limit, float) and pd.isna(usage_limit)):
				chosen_coupon_id = coupon_id
				coupon_usage[key] = used + 1
				break
			elif used < usage_limit:
				chosen_coupon_id = coupon_id
				coupon_usage[key] = used + 1
				break
		coupon_id_values.append(chosen_coupon_id)
	statuses = ["Success" for _ in range(N)]
	payment_method_id_values = [random.choice(payment_method_ids) for _ in range(N)]
	df = pl.DataFrame({
		"id": list(range(start_id+1, start_id+1 + N)),
		"order_date": order_dates,
		"cus_id": cus_ids,
		"store_id": store_id_values,
		"payment_method_id": payment_method_id_values,
		"coupon_id": coupon_id_values,
		"status": statuses
	})
	# Replace NaN with None in the target column and cast to nullable Int64
	df = df.with_columns(
		pl.col("coupon_id").fill_nan(None).cast(pl.Int64, strict=False)
	)
	end_time = time.time()
	print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
	return df


def main():
	#Thêm cơ chế kiểm tra xem file đã tồn tại chưa để lấy start_id, nếu rồi không chạy nữa - end cho ngày hôm nay
	runflag = verify_daily_rate_limit("customer_transactions")
	if runflag:
		print("⚠️ Dữ liệu đã được tạo cho ngày hôm nay. Kết thúc quá trình sinh dữ liệu.")
		return
	#Lấy id lastest từ lần chạy trước của bang transactions
	start_id = 0
	try:
		last_id = get_data_from_parquet("customer_transactions", "id")
		last_id = last_id['id'].tail(1).item()
		start_id = int(last_id)
		print(f"Last recorded id in customer_transactions: {start_id}")
	except pl.exceptions.ColumnNotFoundError:
		print("Col 'id' does not exist within table 'customer_transactions'. Will start from id = 0.")
	except FileNotFoundError:
		print("Table 'customer_transactions' does not exist. Will start from id = 0.")
	# Sinh dữ liệu giao dịch
	transaction_df = generate_transaction_dataframe(start_id=start_id)
	# print(transaction_df)

	write_dataframe_to_parquet(transaction_df,"customer_transactions")
	print("✅ Transaction data generation and write completed!")

if __name__ == "__main__":
	main()