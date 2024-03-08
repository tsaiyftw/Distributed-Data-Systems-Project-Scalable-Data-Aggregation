import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocSubmitJobOperator

# DAG Configuration
with DAG(
    dag_id="submit_spark_jobs",
    description="Submits various SF aggregation and modeling Spark jobs.",
    start_date=datetime.datetime(2024, 2, 27),
    end_date=datetime.datetime(2024, 3, 9),
    schedule="0 4 * * 5",
) as dag:
    # Dataproc job configuration for a PySpark job
    pyspark_job = {
        "reference": {"project_id": "msds697-project"},
        "placement": {"cluster_name": "cluster-f76f"},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc-stage-central/spark-jobs/sfpd-job.py",
        },
    }

    # Submit PySpark job to Dataproc
    submit_sfpd_pyspark = DataprocSubmitJobOperator(
        task_id="submit_sfpd_pyspark",
        project_id="msds697-project",
        region="us-central1",
        job=pyspark_job,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID matches the one configured in Airflow
    )

    # Dataproc job configuration for a PySpark job
    pyspark_job = {
        "reference": {"project_id": "msds697-project"},
        "placement": {"cluster_name": "cluster-f76f"},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc-stage-central/spark-jobs/sffd-model.py",
        },
    }

    # Submit PySpark job to Dataproc
    submit_sffd_pyspark_model = DataprocSubmitJobOperator(
        task_id="submit_sffd_pyspark_model",
        project_id="msds697-project",
        region="us-central1",
        job=pyspark_job,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID matches the one configured in Airflow
    )

    # Dataproc job configuration for a PySpark job
    pyspark_job = {
        "reference": {"project_id": "msds697-project"},
        "placement": {"cluster_name": "cluster-f76f"},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc-stage-central/spark-jobs/sffd-job2.py",
        },
    }

    # Submit PySpark job to Dataproc
    submit_sffd_pyspark_agg = DataprocSubmitJobOperator(
        task_id="submit_sffd_pyspark_agg",
        project_id="msds697-project",
        region="us-central1",
        job=pyspark_job,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID matches the one configured in Airflow
    )

    pyspark_job = {
        "reference": {"project_id": "msds697-project"},
        "placement": {"cluster_name": "cluster-f76f"},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc-stage-central/spark-jobs/service_request-job.py",
        },
    }

    # Submit PySpark job to Dataproc
    submit_service_request_agg = DataprocSubmitJobOperator(
        task_id="submit_service_request_agg",
        project_id="msds697-project",
        region="us-central1",
        job=pyspark_job,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID matches the one configured in Airflow
    )

    (
        submit_sfpd_pyspark
        >> submit_sffd_pyspark_model
        >> submit_sffd_pyspark_agg
        >> submit_service_request_agg
    )
