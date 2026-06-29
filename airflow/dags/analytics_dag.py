from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

from airflow.operators.python import PythonOperator
from pathlib import Path

import polars as pl

from src.analytics.sales_performance import generate_sales_performance
from src.analytics.customer_lifetime_value import generate_customer_lifetime_value
from src.analytics.seller_performance import generate_seller_performance
from src.analytics.product_performance import generate_product_performance
from src.analytics.customer_retention import generate_customer_retention

CORE_DIR = "/opt/airflow/data/core"
ANALYTICS_DIR = "/opt/airflow/data/analytics"

def run_sales_performance():
    Path(ANALYTICS_DIR).mkdir(parents=True, exist_ok=True)
    orders = pl.read_parquet(f"{CORE_DIR}/core_orders.parquet")
    payments = pl.read_parquet(f"{CORE_DIR}/core_payments.parquet")
    result = generate_sales_performance(orders, payments)
    result.write_parquet(f"{ANALYTICS_DIR}/sales_performance.parquet")
    return {"report": "sales_performance", "row_count": result.height}


def run_customer_lifetime_value():
    Path(ANALYTICS_DIR).mkdir(parents=True, exist_ok=True)
    orders = pl.read_parquet(f"{CORE_DIR}/core_orders.parquet")
    payments = pl.read_parquet(f"{CORE_DIR}/core_payments.parquet")
    result = generate_customer_lifetime_value(orders, payments)
    result.write_parquet(f"{ANALYTICS_DIR}/customer_lifetime_value.parquet")
    return {"report": "customer_lifetime_value", "row_count": result.height}


def run_seller_performance():
    Path(ANALYTICS_DIR).mkdir(parents=True, exist_ok=True)
    order_items = pl.read_parquet(f"{CORE_DIR}/core_order_items.parquet")
    reviews = pl.read_parquet(f"{CORE_DIR}/core_reviews.parquet")
    result = generate_seller_performance(order_items, reviews)
    result.write_parquet(f"{ANALYTICS_DIR}/seller_performance.parquet")
    return {"report": "seller_performance", "row_count": result.height}


def run_product_performance():
    Path(ANALYTICS_DIR).mkdir(parents=True, exist_ok=True)
    order_items = pl.read_parquet(f"{CORE_DIR}/core_order_items.parquet")
    reviews = pl.read_parquet(f"{CORE_DIR}/core_reviews.parquet")
    result = generate_product_performance(order_items, reviews)
    result.write_parquet(f"{ANALYTICS_DIR}/product_performance.parquet")
    return {"report": "product_performance", "row_count": result.height}


def run_customer_retention():
    Path(ANALYTICS_DIR).mkdir(parents=True, exist_ok=True)
    orders = pl.read_parquet(f"{CORE_DIR}/core_orders.parquet")
    result = generate_customer_retention(orders)
    result.write_parquet(f"{ANALYTICS_DIR}/customer_retention.parquet")
    return {"report": "customer_retention", "row_count": result.height}


with DAG(
    dag_id="analytics_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    wait_for_core = ExternalTaskSensor(
        task_id="wait_for_core",
        external_dag_id="core_dag",
        external_task_id="publish_metadata",
        allowed_states=["success"],
        poke_interval=30,
    )

    sales_performance_task = PythonOperator(
        task_id="sales_performance_task",
        python_callable=run_sales_performance,
    )
    customer_lifetime_value_task = PythonOperator(
        task_id="customer_lifetime_value_task",
        python_callable=run_customer_lifetime_value,
    )
    seller_performance_task = PythonOperator(
        task_id="seller_performance_task",
        python_callable=run_seller_performance,
    )
    product_performance_task = PythonOperator(
        task_id="product_performance_task",
        python_callable=run_product_performance,
    )
    customer_retention_task = PythonOperator(
        task_id="customer_retention_task",
        python_callable=run_customer_retention,
    )

    analytics_tasks = [
        sales_performance_task,
        customer_lifetime_value_task,
        seller_performance_task,
        product_performance_task,
        customer_retention_task,
    ]

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    wait_for_core >> analytics_tasks >> publish_metadata