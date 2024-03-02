from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    schedule="@once",
) as dag:

    def query_mongo(collection_name: str, field_name: str, **context) -> str:
        """
        Queries MongoDB collection_name for greatest value of field_name.
        This is intended to get the latest date present in the collection.
        """
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.SanFrancisco
        collection = db[collection_name]
        print(f"Connected to MongoDB - {client.server_info()}")
        recent_date_json = collection.find_one(
            filter={}, projection={field_name: 1, "_id": 0}, sort=[(field_name, -1)]
        )
        print(f"Retrieved date - {recent_date_json}")
        client.close()
        date = str(recent_date_json[field_name])
        return date

    def insert_mongo(collection_name: str, data: any, **context) -> None:
        """
        Takes data collected from BigQuery and inserts into MongoDB collection collection_name
        """

    get_latest_service_date = PythonOperator(
        task_id="get_latest_service_date",
        python_callable=query_mongo,
        op_kwargs={"collection_name": "service_requests", "field_name": "created_date"},
        do_xcom_push=True,
        dag=dag,
    )

    get_latest_sffd_date = PythonOperator(
        task_id="get_latest_sffd_date",
        python_callable=query_mongo,
        op_kwargs={"collection_name": "sffd_service_calls", "field_name": "call_date"},
        do_xcom_push=True,
        dag=dag,
    )

    pull_311_bigquery_data = BigQueryInsertJobOperator(
        task_id="pull_311_bigquery_data",
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE dds_dataset.sffd_temp
                SELECT * FROM bigquery-public-data.san_francisco_311.311_service_requests
                WHERE created_date > "{get_latest_service_date.output}"
                """,
                "useLegacySql": False,
            }
        },
        do_xcom_push=True,
        dag=dag,
        location="US",
    )

    pull_sffd_bigquery_data = BigQueryInsertJobOperator(
        task_id="pull_sffd_bigquery_data",
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE dds_dataset.sffd_temp
                SELECT * FROM bigquery-public-data.san_francisco_sffd_service_calls.sffd_service_calls
                WHERE call_date > DATE("{get_latest_sffd_date.output}")
                """,
                "useLegacySql": False,
            }
        },
        do_xcom_push=True,
        dag=dag,
        location="US",
    )

    [get_latest_service_date, get_latest_sffd_date] >> [pull_311_bigquery_data, pull_sffd_bigquery_data]
