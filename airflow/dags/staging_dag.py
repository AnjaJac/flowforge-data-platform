from airflow import DAG
import polars as pl
from pathlib import Path
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from src.staging.customers import clean_customers
from src.staging.schema_validation import validate_all_schemas
from src.staging.sellers import clean_sellers
from src.staging.payments import clean_payments
from src.staging.products import clean_products
from src.staging.reviews import clean_reviews
from src.staging.orders import clean_orders, clean_order_items

RAW_DIR = "/opt/airflow/data/raw"
VALIDATION_CONFIG_PATH = "/opt/airflow/config/validation.yaml"
STAGING_DIR = "/opt/airflow/data/staging"


def run_customer_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/customers.parquet")
    df = clean_customers(df)
    df.write_parquet(f"{STAGING_DIR}/stg_customers.parquet")

def run_seller_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/sellers.parquet")
    df = clean_sellers(df)
    df.write_parquet(f"{STAGING_DIR}/stg_sellers.parquet")

def run_payment_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/payments.parquet")
    df = clean_payments(df)
    df.write_parquet(f"{STAGING_DIR}/stg_payments.parquet")

def run_product_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/products.parquet")
    df = clean_products(df)
    df.write_parquet(f"{STAGING_DIR}/stg_products.parquet")
def run_review_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/reviews.parquet")
    df = clean_reviews(df)
    df.write_parquet(f"{STAGING_DIR}/stg_reviews.parquet")

def run_order_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/orders.parquet")
    df = clean_orders(df)
    df.write_parquet(f"{STAGING_DIR}/stg_orders.parquet")

def run_order_items_processing():
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(f"{RAW_DIR}/order_items.parquet")
    df = clean_order_items(df)
    df.write_parquet(f"{STAGING_DIR}/stg_order_items.parquet")

def run_schema_validation():
    validate_all_schemas(raw_dir=RAW_DIR, config_path=VALIDATION_CONFIG_PATH)


with DAG(
    dag_id="staging_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    schema_validation_task = PythonOperator(
        task_id="schema_validation_task",
        python_callable=run_schema_validation,
    )

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    with TaskGroup("customer_processing") as customer_processing:
        customer_task = PythonOperator(
            task_id="clean_customers_task",
            python_callable=run_customer_processing,
        )
    with TaskGroup("seller_processing") as seller_processing:
        seller_task = PythonOperator(
            task_id="clean_sellers_task",
            python_callable=run_seller_processing,
        )
    with TaskGroup("payment_processing") as payment_processing:
        payment_task = PythonOperator(
            task_id="clean_payments_task",
            python_callable=run_payment_processing,
        )
    with TaskGroup("product_processing") as product_processing:
        product_task = PythonOperator(
            task_id="clean_products_task",
            python_callable=run_product_processing,
        )
    with TaskGroup("review_processing") as review_processing:
        review_task = PythonOperator(
            task_id="clean_reviews_task",
            python_callable=run_review_processing,
        )
    with TaskGroup("order_processing") as order_processing:
        orders_task = PythonOperator(
            task_id="clean_orders_task",
            python_callable=run_order_processing,
        )
        order_items_task = PythonOperator(
            task_id="clean_order_items_task",
            python_callable=run_order_items_processing,
        )
    
    start >> schema_validation_task >> customer_processing >> seller_processing >> payment_processing >> product_processing >> review_processing >> order_processing >> publish_metadata