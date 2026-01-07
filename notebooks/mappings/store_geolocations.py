from dask.distributed import Client
import dask.dataframe as dd
from pyiceberg.catalog import load_catalog
import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, FloatType, StringType, IntegerType,DateType,DecimalType,DoubleType
from .modules import getDaskClient, closeDaskClient, loadIcebergCatalog, loadOrCreateIcebergTable, getSourceData,writeDaskDfToIceberg
from prefect import get_run_logger

logger = get_run_logger()

TABLE = "store_geolocations"
TABLE_TYPE = "sm"
TABLE_IDENTIFIER = ("raw_test", "store_geolocations") #logical table identifier in Iceberg catalog
TABLE_LOCATION = "s3://highlands-coffee-warehouse/raw_test/store_geolocations" #physical location in MinIO bucket
# Create output schema
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "branch_name", StringType(), required=False),
    NestedField(3, "full_address", StringType(), required=False),
    NestedField(4, "city", StringType(), required=False),
)

def s_2_r(source_path: str, source_table: str, source_date: str):
    logger.info(f"Running {TABLE} s_2_r")
    client = None
    # Get Dask client
    client_result = getDaskClient("tcp://dask-scheduler:8786")
    if isinstance(client_result, tuple): #Handle error if any
        return client_result
        # raise Exception(client_result[1])
    client = client_result #Return the client if no error
    logger.info("✅ Successfully connected to Dask client")

    # Load Iceberg catalog
    catalog_result = loadIcebergCatalog("mycatalog")
    if isinstance(catalog_result, tuple):
        return catalog_result
        # raise Exception(catalog_result[1])
    catalog = catalog_result
    logger.info("✅ Successfully loaded Iceberg catalog")

    # Reading input data using Dask
    df_result = getSourceData(day=source_date, table_name=TABLE, engine="dask", table_type=TABLE_TYPE)
    if isinstance(df_result, tuple): #Handle error if any
        return df_result
    df = df_result
    logger.info("✅ Successfully read source data")

    # Create or load Iceberg table
    table_result = loadOrCreateIcebergTable(
        catalog,
        table_identifier=TABLE_IDENTIFIER,
        schema=SCHEMA,
        location=TABLE_LOCATION,
    )
    if isinstance(table_result, tuple):
        return table_result
        # raise Exception(table_result[1])
    table = table_result
    logger.info("✅ Successfully loaded or created logical Iceberg table")

    # Write data to MinIO using Iceberg table append
    write_result = writeDaskDfToIceberg(df, table)
    if isinstance(write_result, tuple):
        return write_result
        # raise Exception(write_result[1])
    # arrow_table = write_result
    # logger.info(arrow_table)
    logger.info("✅ Successfully wrote data to Iceberg table")

    close_result = closeDaskClient(client)
    if isinstance(close_result, tuple):
        logger.warning(f"⚠️ Error while closing Dask client: {close_result[1]}")
    else:
        logger.info("✅ Successfully closed Dask client")

    return True, "ok"

def r_2_e(source_path: str,source_table:str,source_date:str):
    print(f"Running {TABLE} r_2_e")
    return False, f"No action for {TABLE} r_2_e"

def e_2_c(source_path: str,source_table:str,source_date:str):
    print(f"Running {TABLE} e_2_c")
    return False, f"No action for {TABLE} e_2_c"

def main():

    # catalog = load_catalog("mycatalog")
    # catalog.drop_table(("raw_test", "coupons"))
    # # table = catalog.load_table(("raw_test", "coupons"))
    # # print(table.schema())

    print("Coupons module executed")

if __name__ == "__main__":
    main()