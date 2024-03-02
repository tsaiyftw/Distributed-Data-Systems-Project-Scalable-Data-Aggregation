from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.mongo.hooks.mongo import MongoHook

import datetime
import json

from typing import Dict

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
    end_date=datetime.datetime(2024, 3, 9),
    schedule="0 9 * * *",
) as dag:

    def query_mongo(collection_name: str, field_name: str, **context) -> str:
        """
        Queries MongoDB collection_name for greatest value of field_name.
        This is intended to get the latest date present in the collection.

        :param collection_name: The name of the MongoDB collection to insert records into.
        :param field_name: The field to find the largest value of.
        """
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.SanFrancisco
        collection = db[collection_name]
        print(f"Retrieving data from {collection_name}")
        print(f"Connected to MongoDB - {client.server_info()}")
        recent_date_json = collection.find_one(
            filter={}, projection={field_name: 1, "_id": 0}, sort=[(field_name, -1)]
        )
        print(f"Retrieved date - {recent_date_json}")
        client.close()
        date = str(recent_date_json[field_name])
        return date

    def insert_many_records(collection_name: str, data: Dict[str, list], **context) -> None:
        """
        Connects to MongoDB and inserts many records into the specified collection.
        The data is expected to be in the format {columnName: [row1, row2, row3...]}.
        Each key-value pair in the data dictionary corresponds to a column and its values across rows.
        
        :param collection_name: The name of the MongoDB collection to insert records into.
        :param data: The data to insert, formatted as {columnName: [row1, row2, row3...]}.
        """
        # Establish connection to MongoDB
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.SanFrancisco 
        collection = db[collection_name]
        print(f"Preparing to insert data into {collection_name}")
        print(f"Connected to MongoDB - {client.server_info()}")

        documents = data
        
        # Insert the documents into the collection
        if documents:
            result = collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} records into {collection_name} collection.")
        else:
            print("No documents to insert.")
        
        # Close the MongoDB connection
        client.close()

        return (result.acknowledged, len(result.inserted_ids))

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

    create_temp_bq_service = BigQueryInsertJobOperator(
        task_id="create_temp_bq_service",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE dds_dataset.service_temp AS
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

    create_temp_bq_sffd = BigQueryInsertJobOperator(
        task_id="create_temp_bq_sffd",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE dds_dataset.sffd_temp AS
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

    pull_temp_bq_service = BigQueryGetDataOperator(
        task_id="pull_temp_bq_service",
        dataset_id="dds_dataset",
        table_id="service_temp",
        max_results=5000,
        as_dict=True,
        do_xcom_push=True,
        dag=dag,
        location="US",
    )

    pull_temp_bq_sffd = BigQueryGetDataOperator(
        task_id="pull_temp_bq_sffd",
        dataset_id="dds_dataset",
        table_id="sffd_temp",
        max_results=5000,
        as_dict=True,
        do_xcom_push=True,
        dag=dag,
        location="US",
    )

    push_service_data_mongo = PythonOperator(
        task_id = "push_service_data_mongo",
        python_callable=insert_many_records,
        op_kwargs = {"collection_name": "service_requests", "data": pull_temp_bq_service.output},
        do_xcom_push = True,
        dag = dag
    )

    push_sffd_data_mongo = PythonOperator(
        task_id = "push_sffd_data_mongo",
        python_callable=insert_many_records,
        op_kwargs = {"collection_name": "sffd_service_calls", "data": pull_temp_bq_sffd.output},
        do_xcom_push = True,
        dag = dag
    )

    do_1 = EmptyOperator(task_id = "dummy_op_1")

    # task execution order
    do_1 >> [get_latest_service_date, get_latest_sffd_date]
    get_latest_service_date >> create_temp_bq_service >> pull_temp_bq_service >> push_service_data_mongo
    get_latest_sffd_date >> create_temp_bq_sffd >> pull_temp_bq_sffd >> push_sffd_data_mongo
