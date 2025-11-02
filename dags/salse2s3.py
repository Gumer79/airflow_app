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
BIGQUERY_TABLE = "companies"  # Таблица с токенами партнеров
BIGQUERY_SALES_DATASET = "wildberries_raw"  # Dataset for sales data
BIGQUERY_SALES_TABLE = "sales_raw"  # Table for sales data
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
        from datetime import date

        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/sales", params=params)


# --- Airflow DAG Definition ---
@dag(
    dag_id="wildberries_sales_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule="0 1 * * *",  # Ежедневно в 1:00 UTC (выгрузка раз в сутки)
    catchup=False,
    tags=["wildberries", "gcs", "bigquery"],
    doc_md="""
    ### Wildberries Sales to GCS and BigQuery DAG

    This DAG fetches companies from a BigQuery table, retrieves their sales data from the Wildberries API for the previous day,
    stores the data as JSON files in Google Cloud Storage, and then loads the data into BigQuery.
    """,
)
def wildberries_sales_dag():
    @task
    def get_companies_from_bigquery() -> List[Dict[str, Any]]:
        """
        Получает company_id и token из таблицы BigQuery companies.
        """
        logging.info(
            f"Получение компаний из {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        )
        print("GCP_CONN_ID", GCP_CONN_ID)
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

        sql = f"SELECT company_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` WHERE token IS NOT NULL"

        connection = bq_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)

        # Fetch all rows and column descriptions
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        companies = [dict(zip(col_names, row)) for row in rows]

        if not companies:
            raise ValueError("Не найдено компаний в таблице BigQuery.")

        logging.info(f"Найдено {len(companies)} компаний.")
        return companies

    @task
    def fetch_and_save_sales(company: Dict[str, Any], execution_date_str: str):
        """
        Получает данные о продажах для одной компании и сохраняет их в GCS.
        """
        company_id = company.get("company_id")
        token = company.get("token")

        if not company_id or not token:
            logging.warning(
                f"Пропуск компании из-за отсутствия company_id или token: {company}"
            )
            return

        logging.info(f"Обработка компании: {company_id}")

        # Получаем данные за предыдущий день на основе даты выполнения DAG
        execution_date = datetime.fromisoformat(execution_date_str)
        target_date = execution_date.date() - timedelta(days=1)

        try:
            # Инициализируем API и получаем данные о продажах
            wb_api = WildberriesAPI(api_key=token)
            sales_data = wb_api.get_sales(date_from=target_date, flag=1)
            logging.info(
                f"Успешно получено {len(sales_data)} записей о продажах для компании {company_id}."
            )

            if not sales_data:
                logging.info(
                    f"Не найдено данных о продажах для компании {company_id} за дату {target_date}. Ничего не загружено."
                )
                return

            # Подготовка для загрузки в GCS
            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
            file_name = f"wildberries/sales/{target_date.isoformat()}/{company_id}.json"

            # Загружаем данные как JSON файл
            gcs_hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=file_name,
                data=json.dumps(sales_data, indent=4, ensure_ascii=False),
                mime_type="application/json",
            )
            logging.info(
                f"Успешно загружены данные о продажах для компании {company_id} в gs://{GCS_BUCKET}/{file_name}"
            )

        except Exception as e:
            logging.error(
                f"Ошибка при обработке компании {company_id}. Error: {e}", exc_info=True
            )
            # В зависимости от требований, вы можете захотеть завершить задачу с ошибкой
            # raise e

    @task
    def load_gcs_to_bigquery(execution_date_str: str):
        """
        Загружает данные из GCS в BigQuery.
        Читает все JSON файлы из папки wildberries/sales/{date}/ и загружает их в BigQuery.
        """
        from google.cloud import bigquery as bq_client
        from google.cloud.exceptions import NotFound
        from google.oauth2 import service_account

        execution_date = datetime.fromisoformat(execution_date_str)
        target_date = (execution_date - timedelta(days=1)).date()
        date_prefix = target_date.isoformat()

        logging.info(f"Загрузка данных из GCS в BigQuery для даты: {date_prefix}")

        try:
            # Инициализация клиентов
            gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

            # Получаем список всех файлов для указанной даты
            prefix = f"wildberries/sales/{date_prefix}/"
            files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=prefix)

            if not files:
                logging.warning(f"Не найдено файлов в GCS для даты {date_prefix}")
                return

            logging.info(f"Найдено {len(files)} файлов для загрузки")

            # Получаем credentials из BigQueryHook
            bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
            credentials = bq_hook.get_credentials()
            
            # Инициализируем BigQuery client с полученными credentials
            client = bq_client.Client(credentials=credentials, project=BIGQUERY_PROJECT)

            # Создаем dataset если его нет
            dataset_id = BIGQUERY_SALES_DATASET
            dataset_ref = f"{BIGQUERY_PROJECT}.{dataset_id}"

            try:
                client.get_dataset(dataset_ref)
                logging.info(f"Dataset {dataset_id} уже существует")
            except NotFound:
                dataset = bq_client.Dataset(dataset_ref)
                dataset.location = "EU"
                client.create_dataset(dataset, exists_ok=True)
                logging.info(f"Dataset {dataset_id} создан")

            # Схема таблицы для данных о продажах
            schema = [
                bq_client.SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
                bq_client.SchemaField("lastChangeDate", "TIMESTAMP", mode="NULLABLE"),
                bq_client.SchemaField("warehouseName", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("warehouseType", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("countryName", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("oblastOkrugName", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("regionName", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("supplierArticle", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("nmId", "INTEGER", mode="NULLABLE"),
                bq_client.SchemaField("barcode", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("category", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("subject", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("brand", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("techSize", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("incomeID", "INTEGER", mode="NULLABLE"),
                bq_client.SchemaField("isSupply", "BOOLEAN", mode="NULLABLE"),
                bq_client.SchemaField("isRealization", "BOOLEAN", mode="NULLABLE"),
                bq_client.SchemaField("totalPrice", "NUMERIC", mode="NULLABLE"),
                bq_client.SchemaField("discountPercent", "INTEGER", mode="NULLABLE"),
                bq_client.SchemaField("spp", "INTEGER", mode="NULLABLE"),
                bq_client.SchemaField("paymentSaleAmount", "NUMERIC", mode="NULLABLE"),
                bq_client.SchemaField("forPay", "NUMERIC", mode="NULLABLE"),
                bq_client.SchemaField("finishedPrice", "NUMERIC", mode="NULLABLE"),
                bq_client.SchemaField("priceWithDisc", "NUMERIC", mode="NULLABLE"),
                bq_client.SchemaField("saleID", "STRING", mode="REQUIRED"),
                bq_client.SchemaField("sticker", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("gNumber", "STRING", mode="NULLABLE"),
                bq_client.SchemaField("srid", "STRING", mode="NULLABLE"),
                # Служебные поля
                bq_client.SchemaField(
                    "user_id", "STRING", mode="REQUIRED"
                ),  # Для обратной совместимости
                bq_client.SchemaField(
                    "company_id", "STRING", mode="REQUIRED"
                ),  # ID компании
                bq_client.SchemaField(
                    "data_ingestion_time", "TIMESTAMP", mode="REQUIRED"
                ),
            ]

            table_id = f"{BIGQUERY_PROJECT}.{dataset_id}.{BIGQUERY_SALES_TABLE}"
            table_ref = bq_client.Table(table_id, schema=schema)

            # Настройка партиционирования по дате
            table_ref.time_partitioning = bq_client.TimePartitioning(
                type_=bq_client.TimePartitioningType.DAY, field="date"
            )

            # Создаем таблицу если её нет
            try:
                client.get_table(table_id)
                logging.info(f"Таблица {table_id} уже существует")
            except NotFound:
                client.create_table(table_ref, exists_ok=True)
                logging.info(f"Таблица {table_id} создана")

            # Читаем и обрабатываем все файлы
            all_rows = []
            data_ingestion_time = datetime.utcnow()

            for file_path in files:
                try:
                    # Извлекаем company_id из имени файла (формат: wildberries/sales/{date}/{company_id}.json)
                    file_name = file_path.split("/")[-1]
                    company_id = file_name.replace(".json", "")

                    # Читаем файл из GCS
                    file_content = gcs_hook.download(
                        bucket_name=GCS_BUCKET, object_name=file_path
                    )

                    # Парсим JSON массив
                    if isinstance(file_content, bytes):
                        file_content = file_content.decode("utf-8")

                    sales_data = json.loads(file_content)

                    if not isinstance(sales_data, list):
                        logging.warning(f"Файл {file_path} не содержит массив данных")
                        continue

                    # Обрабатываем каждую запись о продаже
                    for sale in sales_data:
                        # Добавляем company_id и data_ingestion_time к каждой записи
                        sale_record = sale.copy()
                        sale_record["company_id"] = company_id
                        sale_record["user_id"] = (
                            company_id  # Для обратной совместимости сохраняем также в user_id
                        )
                        sale_record["data_ingestion_time"] = (
                            data_ingestion_time.isoformat() + "Z"
                        )
                        all_rows.append(sale_record)

                    logging.info(
                        f"Обработан файл {file_path}: {len(sales_data)} записей"
                    )

                except Exception as e:
                    logging.error(
                        f"Ошибка при обработке файла {file_path}: {e}", exc_info=True
                    )
                    continue

            if not all_rows:
                logging.warning("Нет данных для загрузки в BigQuery")
                return

            logging.info(
                f"Всего подготовлено {len(all_rows)} записей для загрузки в BigQuery"
            )

            # Загружаем данные в BigQuery
            table = client.get_table(table_id)
            errors = client.insert_rows_json(table, all_rows)

            if errors:
                logging.error(f"Ошибки при загрузке данных в BigQuery: {errors}")
                raise Exception(f"Ошибки при загрузке: {errors}")

            logging.info(
                f"Успешно загружено {len(all_rows)} записей в таблицу {table_id}"
            )

        except Exception as e:
            logging.error(
                f"Ошибка при загрузке данных из GCS в BigQuery: {e}", exc_info=True
            )
            raise

    # Task dependencies
    companies_list = get_companies_from_bigquery()

    # Dynamically map the processing task over the list of companies
    sales_tasks = fetch_and_save_sales.partial(execution_date_str="{{ ds }}").expand(
        company=companies_list
    )

    # Загрузка в BigQuery после завершения всех задач загрузки в GCS
    load_task = load_gcs_to_bigquery(execution_date_str="{{ ds }}")

    # Устанавливаем зависимость: загрузка в BigQuery должна выполняться после всех загрузок в GCS
    _ = sales_tasks >> load_task  # Зависимость установлена


# Instantiate the DAG
wildberries_sales_dag()
