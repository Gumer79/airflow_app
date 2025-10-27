import os
import json
import logging
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from typing import Union, List, Dict, Any
from google.oauth2 import service_account
from google.cloud import bigquery

from utilities.conections import gcp_connection
import time


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


# --- Placeholder Variables ---
# Replace these with your actual GCP and BigQuery details
GCP_CONN_ID = "google_cloud_default"
BIGQUERY_PROJECT = "shirman-group-app"  # e.g., 'my-gcp-project'
BIGQUERY_DATASET = "user_data"  # e.g., 'analytics'
BIGQUERY_TABLE = "users"
GCS_BUCKET = "app_s3"  # e.g., 'my-wildberries-data'


# --- WildberriesAPI Class ---
# Included directly in the DAG file for simplicity
class WildberriesAPI:
    """
    Python SDK для API Поставщиков Wildberries.
    """

    BASE_URL = "https://statistics-api.wildberries.ru"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Требуется API-ключ.")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Authorization": self.api_key})

    def _make_request(self, endpoint: str, params: dict) -> List[Dict[str, Any]]:
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"Произошла HTTP-ошибка: {http_err} - {response.text}")
            raise
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Произошла ошибка запроса: {req_err}")
            raise

    def get_sales(
        self, date_from: Union[datetime.date, datetime, str], flag: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Получает информацию о продажах за определенную дату.
        Использует flag=1 для получения всех данных за указанную дату.
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, datetime.date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = date_from

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/sales", params=params)


# --- Airflow DAG Definition ---
@dag(
    dag_id="wildberries_sales_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["wildberries", "gcs", "bigquery"],
    doc_md="""
    ### Wildberries Sales to GCS DAG

    This DAG fetches users from a BigQuery table, retrieves their sales data from the Wildberries API for the previous day,
    and stores the data as JSON files in Google Cloud Storage.
    """,
)
def wildberries_sales_dag():
    @task
    def get_users_from_bigquery() -> List[Dict[str, Any]]:
        """
        Fetches user_id and token from the BigQuery users table.
        """
        # logging.info(
        #     f"Fetching users from {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        # )
        # credentials = gcp_connection(conn_id=GCP_CONN_ID)

        # credentials = service_account.Credentials.from_service_account_info(credentials)

        # query = f"SELECT user_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"
        # print(query)
        # client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT)
        # query_job = client.query(query)
        # print("query_job")
        # print(query_job)
        # for row in query_job:
        #     print(row)

        logging.info(
            f"Fetching users from {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        )
        print("GCP_CONN_ID", GCP_CONN_ID)
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

        sql = f"SELECT user_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"

        connection = bq_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)

        # Fetch all rows and column descriptions
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        users = [dict(zip(col_names, row)) for row in rows]

        if not users:
            raise ValueError("No users found in the BigQuery table.")

        logging.info(f"Found {len(users)} users.")
        return users

    @task
    def fetch_and_save_sales(user: Dict[str, Any], execution_date_str: str):
        """
        Fetches sales for a single user and saves them to GCS.
        """
        user_id = user.get("user_id")
        token = user.get("token")

        if not user_id or not token:
            logging.warning(f"Skipping user due to missing user_id or token: {user}")
            return

        logging.info(f"Processing user: {user_id}")

        # We will fetch data for the previous day based on the DAG's execution date
        execution_date = datetime.fromisoformat(execution_date_str)
        target_date = execution_date.date() - timedelta(days=1)

        try:
            # Initialize API and get sales
            wb_api = WildberriesAPI(api_key=token)
            sales_data = wb_api.get_sales(date_from=target_date, flag=1)
            logging.info(
                f"Successfully fetched {len(sales_data)} sales records for user {user_id}."
            )

            if not sales_data:
                logging.info(
                    f"No sales data found for user {user_id} for date {target_date}. Nothing to upload."
                )
                return

            # Prepare for GCS upload
            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
            file_name = f"wildberries/sales/{target_date.isoformat()}/{user_id}.json"

            # Upload data as a JSON file
            gcs_hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=file_name,
                data=json.dumps(sales_data, indent=4, ensure_ascii=False),
                mime_type="application/json",
            )
            logging.info(
                f"Successfully uploaded sales data for user {user_id} to gs://{GCS_BUCKET}/{file_name}"
            )

        except Exception as e:
            logging.error(
                f"Failed to process user {user_id}. Error: {e}", exc_info=True
            )
            # Depending on requirements, you might want to fail the task
            # raise e

    # Task dependencies
    users_list = get_users_from_bigquery()

    # Dynamically map the processing task over the list of users
    fetch_and_save_sales.partial(execution_date_str="{{ ds }}").expand(user=users_list)


# Instantiate the DAG
wildberries_sales_dag()
