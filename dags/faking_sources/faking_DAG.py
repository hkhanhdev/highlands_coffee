from __future__ import annotations
import pendulum,random
from airflow.decorators import dag, task, task_group
from airflow.operators.python import ShortCircuitOperator
from airflow.exceptions import AirflowFailException # Dùng để dừng DAG
from airflow.models.baseoperator import chain

# Import các hàm xử lý dữ liệu từ script đã tạo
from faking_sources.customer_faking import generate_customer_dataframe
from faking_sources.transactions_faking import generate_transaction_dataframe
from faking_sources.transaction_details_faking import get_transaction_details
from faking_sources.modulars import * 
# Cấu hình Retry (thử lại 2 lần sau mỗi 5 phút nếu lỗi)
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=1),
}

@dag(
    dag_id="faking_data_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 9 * * *",  # Chạy hàng ngày vào lúc 9:00 AM
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["data_faking", "oltp"],
)
def faking_pipeline_dag():
    def check_daily_limit(table_name):
        runflag = verify_daily_rate_limit(table_name)
        if runflag:
            print("⚠️ Dữ liệu đã được tạo cho ngày hôm nay. Kết thúc quá trình sinh dữ liệu.")
            return False
        return True

    @task_group(group_id="customers_faking_group")
    def customers_faking_group():
        # 1. Task ShortCircuitOperator: Kiểm tra điều kiện
        verify_limit_sc = ShortCircuitOperator(
            task_id="verify_customers_ratelimit_sc",
            # Sử dụng hàm check_daily_limit đã định nghĩa
            python_callable=check_daily_limit,
            op_args=["customers"], # Truyền tham số cho hàm check_daily_limit
            retries=3,
            trigger_rule='none_failed'
        )
        #Generate and write customers dataframe to parquet
        @task(task_id="generate_and_write_customers_df_to_parquet",retries=3,trigger_rule='none_failed')
        def generate_and_write_customers_df_to_parquet():
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

        # Thiết lập thứ tự thực thi trong nhóm
        # Khởi tạo Task Instance cho TaskFlow
        generate_and_write_customers = generate_and_write_customers_df_to_parquet()

        # Thiết lập dependency: Task sinh dữ liệu chỉ chạy nếu ShortCircuitOperator trả về True
        verify_limit_sc >> generate_and_write_customers
        return verify_limit_sc, generate_and_write_customers

    @task_group(group_id="transactions_faking_group")
    def transactions_faking_group():
        # 1. Task ShortCircuitOperator: Kiểm tra điều kiện
        verify_limit_sc = ShortCircuitOperator(
            task_id="verify_transactions_ratelimit_sc",
            # Sử dụng hàm check_daily_limit đã định nghĩa
            python_callable=check_daily_limit,
            op_args=["customer_transactions"], # Truyền tham số cho hàm check_daily_limit
            retries=3,
            trigger_rule='none_failed'
        )
        #Generate and write transactions dataframe to parquet
        @task(task_id="generate_and_write_transactions_df_to_parquet",retries=3,trigger_rule='none_failed')
        def generate_and_write_transactions_df_to_parquet():
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
        @task(task_id="end_of_flow")
        def end_of_flow():
            print("Flow ended due to rate limit check.")
        # Thiết lập thứ tự thực thi trong nhóm
        generate_and_write_transactions = generate_and_write_transactions_df_to_parquet()
        verify_limit_sc >> generate_and_write_transactions
        return verify_limit_sc, generate_and_write_transactions
    
    @task_group(group_id="transaction_details_faking_group")
    def transaction_details_faking_group():
        # 1. Task ShortCircuitOperator: Kiểm tra điều kiện
        verify_limit_sc = ShortCircuitOperator(
            task_id="verify_transaction_details_ratelimit_sc",
            # Sử dụng hàm check_daily_limit đã định nghĩa
            python_callable=check_daily_limit,
            op_args=["transaction_details"], # Truyền tham số cho hàm check_daily_limit
            retries=3,
            trigger_rule='none_failed'
        )
        #Generate and write transaction_details dataframe to parquet
        @task(task_id="generate_and_write_transaction_details_df_to_parquet",retries=3)
        def generate_and_write_transaction_details_df_to_parquet():
             #Thêm cơ chế kiểm tra xem file đã tồn tại chưa để lấy start_id, nếu rồi không chạy nữa - end cho ngày hôm nay
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
        # Thiết lập thứ tự thực thi trong nhóm
        generate_and_write_transaction_details = generate_and_write_transaction_details_df_to_parquet()
        verify_limit_sc >> generate_and_write_transaction_details
        return verify_limit_sc, generate_and_write_transaction_details

    # 1. Khởi tạo các Task Group
    verify_cust, gen_cust = customers_faking_group()
    verify_trans, gen_trans = transactions_faking_group()
    verify_detail, gen_detail = transaction_details_faking_group()

    # # Thiết lập thứ tự thực thi giữa các nhóm
    # customers_faking_group() >> transactions_faking_group() >> transaction_details_faking_group()
        # 2. THIẾT LẬP DEPENDENCY VÀ TRIGGER_RULE
    
    # a) Nhóm Transactions phải chạy sau Customers
    # Ta đặt dependency lên task "verify" đầu tiên của nhóm Transactions
    # và sử dụng trigger_rule để buộc nó chạy ngay cả khi task trước bị Skipped.
    
    # Task cuối cùng của nhóm Customers (gen_cust) phải hoàn thành trước khi task đầu tiên của nhóm Transactions chạy.
    # gen_cust >> verify_trans
    
    # **CÁCH KHẮC PHỤC CHÍNH:** Thiết lập trigger_rule trên task verify của nhóm Transactions
    # Bắt buộc task verify_transactions_ratelimit_sc phải chạy chừng nào không có task tiền nhiệm nào thất bại.
    # Điều này cho phép nó chạy ngay cả khi gen_cust bị Skipped.

    gen_cust >> verify_trans >> gen_trans >> verify_detail
    
    # b) Nhóm Transaction Details phải chạy sau Transactions
    # Tương tự, ta đặt trigger_rule lên task verify đầu tiên của nhóm Details.
    

# Khởi tạo DAG
faking_pipeline_dag()