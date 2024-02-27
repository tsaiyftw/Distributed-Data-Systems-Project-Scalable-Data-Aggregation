from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from airflow.providers.mongo.hooks.mongo import MongoHook

import datetime
import json

# DAG for periodic updates of MongoDB collections from public BigQuery datasets
# datasets:
# * 311_service_requests
# * sffd_service_calls
# * sfpd_incidents

# these tasks should be completed for all three datasets
# t1: query mongodb for last entered {DATASET_TIME}
# t2: retrieve rows from bigquery newer than t1 result and export
# t3: insert new documents into mongodb

with DAG(
    dag_id="update_mongoDB",
    description="Updates data on MongoDB",
    start_date=datetime.datetime(2024, 2, 27),
    schedule="* * * * *",
) as dag:

    def query_mongo(collection_name: str, field_name: str, **context) -> str:
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.SanFrancisco
        collection = db[collection_name]
        print(f"Connected to MongoDB - {client.server_info()}")
        recent_date_json = collection.find_one(
            filter={}, projection={field_name: 1, "_id": 0}, sort=[(field_name, -1)]
        )
        client.close()
        date = recent_date_json[field_name]

        # push date to XCOM
        task_instance = context["task_instance"]
        task_instance.xcom_push(key=f"latest_{collection_name}_date", value=date)
        return date

    get_latest_service_date = PythonOperator(
        task_id="get_latest_service_date",
        python_callable=query_mongo,
        op_kwargs={"collection_name": "service_requests", "field_name": "created_date"},
        dag=dag,
    )

    get_latest_sffd_date = PythonOperator(
        task_id="get_latest_sffd_date",
        python_callable=query_mongo,
        op_kwargs={"collection_name": "sffd_service_calls", "field_name": "call_date"},
        dag=dag,
    )
