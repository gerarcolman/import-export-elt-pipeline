from airflow.decorators import dag, task
from pendulum import datetime, duration
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

def get_url(logical_date: datetime):
    year = extract_date_parts(logical_date)["year"]
    url_month = extract_date_parts(logical_date)["url_month"]
    url = f"https://datosabiertos.aduana.gov.py/all_data/{year}/{url_month}/{year}_{url_month}_Nivel_Item.csv"
    
    return url

def blob_paths(logical_date: datetime):
    year = extract_date_parts(logical_date)["year"]
    url_month = extract_date_parts(logical_date)["url_month"]
    raw_blob_path = f"raw/import-export_{year}_{url_month}.csv"
    cleaned_blob_path = f"clean/import-export_{year}_{url_month}_cleaned.csv"

    return {"raw_blob_path": raw_blob_path, "cleaned_blob_path": cleaned_blob_path}

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
    def upload_blob_from_memory(logical_date):
        import requests
        from io import BytesIO
        """Uploads a file to the bucket."""

        # The ID of your GCS bucket
        url = get_url(logical_date)

        # The ID of your GCS object
        raw_blob_name = blob_paths(logical_date)["raw_blob_path"]

        response = requests.get(url, stream=True)
        response.raise_for_status()

        csv_bytes = BytesIO(response.content)

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(raw_blob_name)
        blob.upload_from_file(csv_bytes, rewind=True, content_type='text/csv')

    @task
    def cleaned_file(logical_date):
        import pandas as pd

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        raw_csv_path = f'gs://{BUCKET_NAME}/{blob_paths(logical_date)["raw_blob_path"]}'
        df = pd.read_csv(raw_csv_path, encoding='utf-8', engine='python', on_bad_lines='skip', quotechar='"', decimal=',',
                         usecols=["OPERACION", "DESTINACION", "REGIMEN", "AÑO", "MES", "ADUANA", "COTIZACION", 
                                  "MEDIO TRANSPORTE", "CANAL", "ITEM", "PAIS ORIGEN", "PAIS PROCEDENCIA/DESTINO", "USO",
                                  "UNIDAD MEDIDA ESTADISTICA", "CANTIDAD ESTADISTICA", "KILO NETO", "KILO BRUTO", "FOB DOLAR",
                                  "FLETE DOLAR", "SEGURO DOLAR", "IMPONIBLE DOLAR", "IMPONIBLE GS", "AJUSTE A INCLUIR",
                                  "AJUSTE A DEDUCIR", "POSICION ", "RUBRO", "DESC CAPITULO" ,"DESC PARTIDA", "DESC POSICION",
                                  "MERCADERIA", "MARCA ITEM", "ACUERDO"])

        df.columns = clean_column_names(df.columns)
        df = df.replace('¿', '', regex=True)
            
        cleaned_csv_path = "/tmp/cleaned_file.csv"
        df.to_csv(cleaned_csv_path, index=False, encoding='utf-8')
        bucket.blob(blob_paths(logical_date)["cleaned_blob_path"]).upload_from_filename(cleaned_csv_path)

    @task
    def source_object(logical_date):
        return f'{blob_paths(logical_date)["cleaned_blob_path"]}'
    
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


    t1 = upload_blob_from_memory()
    t2 = cleaned_file()
    t3 = gcs_to_bigquery

    t1 >> t2 >> t3 >> dbt_transformation

monthly_csv_pipeline()