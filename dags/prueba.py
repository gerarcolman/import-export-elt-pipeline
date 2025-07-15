from airflow.decorators import dag, task
from pendulum import datetime, duration
import requests
from google.cloud import storage
import os
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode
from my_cosmos_config import DBT_PROFILE_CONFIG, DBT_PROJECT_CONFIG, DBT_EXECUTION_CONFIG, DBT_RENDER_CONFIG


BUCKET_NAME = os.environ.get("MY_GCP_BUCKET")
PROJECT_ID = os.environ.get("MY_GCP_PROJECT_ID")
SCHEMA = os.environ.get("MY_DATASET")


def extract_date_parts(logical_date: datetime):
    year = logical_date.strftime("%Y")
    month = logical_date.month
    months_spanish = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO',
                      'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    url_month = months_spanish[month - 1]

    return {"year": year, "url_month": url_month}

def get_paths(logical_date: datetime):
    year = extract_date_parts(logical_date)["year"]
    url_month = extract_date_parts(logical_date)["url_month"]
    url = f"https://datosabiertos.aduana.gov.py/all_data/{year}/{url_month}/{year}_{url_month}.csv"
    raw_localfile_path = f"/tmp/raw_import_export_{year}_{url_month}.csv"
    raw_gcp_bucket_path = f"raw/import-export_{year}_{url_month}.csv"

    return {"url": url, "raw_localfile_path": raw_localfile_path, "raw_gcp_bucket_path": raw_gcp_bucket_path}

#def blob_paths(logical_date: datetime):
#    year = extract_date_parts(logical_date)["year"]
#    url_month = extract_date_parts(logical_date)["url_month"]
#    raw_blob_path = f"raw/import-export_{year}_{url_month}.csv"
#    cleaned_blob_path = f"clean/import-export_{year}_{url_month}_cleaned.csv"

#    return {"raw_blob_path": raw_blob_path, "cleaned_blob_path": cleaned_blob_path}

def clean_column_names(columns):
    import re

    cleaned = []
    for col in columns:
        col = col.strip()
        col = col.lower()
        col = re.sub(r"[^\w\s]", "", col)
        col = re.sub(r"\s+", "_", col)
        cleaned.append(col)
    return cleaned

@dag(
    default_args= {
        'owner':'gerarcolman',
        'retries': 1,
        'retry_delay': duration(minutes=1)
    },
    start_date=datetime(2023, 1, 5),
    schedule="@monthly",
    catchup=False,
    tags=["import-export", "Paraguay_2025"],
)
def monthly_csv_pipeline():

    @task
    def download_to_local(logical_date):
        import pandas as pd

        url = get_paths(logical_date)["url"]
        local_file = get_paths(logical_date)["raw_localfile_path"]

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(local_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        df = pd.read_csv(local_file, encoding="utf-8", on_bad_lines="skip", engine="python", decimal=',', quotechar='"')
        df.columns = clean_column_names(df.columns)
        df = df.drop(columns=["oficializacion", "cancelacion"], errors="ignore")
        df.to_csv(local_file, index=False, encoding="utf-8")

        return local_file

    @task
    def upload_to_gcs(local_file, logical_date):
        raw_blob_name = get_paths(logical_date)["raw_gcp_bucket_path"]

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(raw_blob_name)
        
        with open(local_file, "rb") as f:
            blob.upload_from_file(f, content_type='text/csv')

    @task
    def delete_localfile():
        from airflow.operators.python import get_current_context

        context = get_current_context()
        ti = context["ti"]
        local_file = ti.xcom_pull(task_ids="download_to_local")

        os.remove(local_file)

    @task
    def source_object(logical_date):
        raw_blob_name = get_paths(logical_date)["raw_gcp_bucket_path"]
        return raw_blob_name
    
    # @task
    # def bq_table(logical_date):
    #     return f"{PROJECT_ID}.{SCHEMA}.{extract_date_parts(logical_date)["url_month"]}_{extract_date_parts(logical_date)["year"]}_raw"

    source = source_object()
    #table = bq_table()
        
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[source],
        destination_project_dataset_table=f"{PROJECT_ID}.{SCHEMA}.datos_abiertos_raw",
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        max_bad_records="100",
        quote_character=None,
        ignore_unknown_values="True",
        encoding="UTF-8",
        gcp_conn_id="gcp",
        autodetect="True",
        project_id=PROJECT_ID,
        write_disposition="WRITE_APPEND",
    )

    dbt_transformation = DbtTaskGroup(
        group_id='dbt_transformation',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        render_config=DBT_RENDER_CONFIG,
    )


    t1 = download_to_local()
    t2 = upload_to_gcs(t1)
    t4 = delete_localfile()

    t1 >> t2 >> gcs_to_bigquery >> t4 >> dbt_transformation
    
monthly_csv_pipeline()