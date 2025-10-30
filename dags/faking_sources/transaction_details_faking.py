import polars as pl
# import pandas as pd
import random,os,datetime,time
from faking_sources.modulars import *
# from modulars import *
# NUM_RECORDS_TO_GENERATE = 10

def get_transaction_details(transactions_df:pl.DataFrame,last_order_id:int=1,last_detail_id:int=0) -> pl.DataFrame:
	print("--- TASK: Generate Transaction Details DataFrame ---")
	start_time = time.time()
	product_details_df = pl.read_csv(PRODUCT_DETAILS_FILE)
	coupons_df = pl.read_csv(COUPONS_FILE)
	details_records = []
	detail_id = 1 + last_detail_id
	for order in transactions_df.iter_rows(named=True):
		order_id = order['id']
		coupon_id = order.get('coupon_id', None)
		# Xác suất quantity cho đơn hàng
		rand = random.random()
		if rand < 0.7:
			total_quantity = random.randint(1, 2)
		elif rand < 0.95:
			total_quantity = random.randint(3, 6)
		else:
			total_quantity = random.randint(7, 20)
		# Chọn sản phẩm chi tiết cho đơn hàng
		chosen_details = random.choices(product_details_df['id'].to_list(), k=total_quantity)
		# Đếm số lượng từng sản phẩm chi tiết
		from collections import Counter
		detail_counter = Counter(chosen_details)
		for product_details_id, quantity in detail_counter.items():
			# Lấy thông tin sản phẩm con
			detail_row = product_details_df.filter(pl.col('id') == product_details_id)
			if detail_row.height == 0:
				continue
			product_id = detail_row['product_id'][0]
			price = detail_row['price'][0]
			subtotal = price * quantity
			# Tính discount_amount
			discount_amount = 0
			subtotal_after_discount = subtotal
			if coupon_id is not None:
				coupon_row = coupons_df.filter(pl.col('id') == coupon_id)
				if coupon_row.height > 0:
					discount_value = coupon_row['discount_value'][0]
					if discount_value <= 100:
						discount_amount = subtotal * discount_value / 100
					else:
						discount_amount = discount_value
					subtotal_after_discount = subtotal - discount_amount
			details_records.append({
				"id": detail_id,
				"order_id": order_id,
				"product_id": product_id,
				"product_details_id": product_details_id,
				"quantity": quantity,
				"subtotal": subtotal,
				"discount_amount": discount_amount,
				"subtotal_after_discount": subtotal_after_discount
			})
			detail_id += 1
	df = pl.DataFrame(details_records)
	end_time = time.time()
	print(f"⏱️ Task runtime: {end_time - start_time:.4f} seconds")
	return df

def main():
	#Thêm cơ chế kiểm tra xem file đã tồn tại chưa để lấy start_id, nếu rồi không chạy nữa - end cho ngày hôm nay
	runflag = verify_daily_rate_limit("transaction_details")
	if runflag:
		print("⚠️ Dữ liệu đã được tạo cho ngày hôm nay. Kết thúc quá trình sinh dữ liệu.")
		return
	
	customer_transactions_df = None
	start_id = 0
	order_id = 1
	try:
		#Lấy lastest customer transactions để tiếp tục sinh dữ liệu cho tập data lastest
		customer_transactions_df = get_data_from_parquet("customer_transactions", which_cols="*")
		#Lấy last order id từ bảng transaction_details để biết được đã sinh dữ liệu đến order nào rồi, last detail id để sinh tiếp
		last_transaction_details_df = get_data_from_parquet("transaction_details","id,order_id")
		# print(last_id)
		last_details_id = last_transaction_details_df['id'].tail(1).item()
		last_order_id_in_details = last_transaction_details_df['order_id'].tail(1).item()
		start_id = int(last_details_id)
		order_id = int(last_order_id_in_details)
		print(f"Last recorded id in transaction_details: {start_id} and order_id: {order_id}")
	except pl.exceptions.ColumnNotFoundError:
		print("Col 'id' does not exist within table 'transaction_details'. Will start from id = 0.")
	except FileNotFoundError:
		print("Table 'transaction_details' does not exist. Will start from id = 0.")
	# Sinh dữ liệu chi tiết giao dịch
	transaction_details_df = get_transaction_details(transactions_df=customer_transactions_df,last_detail_id=start_id, last_order_id=order_id)
	# print(transaction_details_df)
	write_dataframe_to_parquet(transaction_details_df,"transaction_details")
	print("✅ Transaction details data generation and write completed!")

if __name__ == "__main__":
	main()


