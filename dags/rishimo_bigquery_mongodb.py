from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from airflow.providers.mongo.hooks.mongo import MongoHook

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
) as dag:
    
    def query_mongo(collection: str) -> str:
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        