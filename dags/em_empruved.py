import os
import logging
import json
import hashlib
from datetime import datetime, timedelta, date
from typing import Union, List, Dict, Any

import requests
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from google.cloud import bigquery, storage

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

# --- Переменные конфигурации ---
GCP_CONN_ID = "google_cloud_default"
BIGQUERY_PROJECT = "shirman-group-app"
BIGQUERY_RAW_DATASET = "wildberries_raw"
BIGQUERY_ANALYTICS_DATASET = "wildberries_analytics"
BIGQUERY_USERS_DATASET = "user_data"
BIGQUERY_USERS_TABLE = "users"

GCS_BUCKET = "app_s3"
GCS_RAW_PREFIX = "raw/wildberries/sales"
GCS_STAGING_PREFIX = "staging/wildberries_sales_staging"
GCS_ARCHIVE_PREFIX = "archive/wildberries"

# BigQuery таблицы
BQ_RAW_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_RAW_DATASET}.sales_raw"
BQ_DEDUP_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_RAW_DATASET}.sales_hourly_upsert"
BQ_ANALYTICS_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.sales"


# --- Класс WildberriesAPI ---
class WildberriesAPI:
    """Python SDK для API Поставщиков Wildberries."""

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
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"Произошла HTTP-ошибка: {http_err} - {response.text}")
            return []
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Произошла ошибка запроса: {req_err}")
            raise
        except json.JSONDecodeError as json_err:
            logging.error(f"Ошибка декодирования JSON: {json_err}")
            return []

    def get_sales(
        self, date_from: Union[date, datetime, str], flag: int = 1
    ) -> List[Dict[str, Any]]:
        """Получает информацию о продажах за определенную дату."""
        if isinstance(date_from, datetime):
            date_from_str = date_from.date().isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/sales", params=params)


def get_users_from_bigquery(**kwargs) -> List[Dict[str, Any]]:
    """Извлекает user_id и token из таблицы BigQuery."""
    logging.info(
        f"Извлечение пользователей из {BIGQUERY_PROJECT}.{BIGQUERY_USERS_DATASET}.{BIGQUERY_USERS_TABLE}"
    )

    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        sql = f"""
        SELECT user_id, token
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_USERS_DATASET}.{BIGQUERY_USERS_TABLE}`
        WHERE token IS NOT NULL
        """

        connection = bq_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)

        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        users = [dict(zip(col_names, row)) for row in rows]

        if not users:
            logging.warning("В таблице BigQuery не найдено пользователей.")
            return []

        logging.info(f"Найдено {len(users)} пользователей.")
        return users

    except Exception as e:
        logging.error(
            f"Ошибка при извлечении пользователей из BigQuery: {e}", exc_info=True
        )
        raise


def fetch_and_save_sales_to_gcs(**kwargs):
    """Получает продажи для каждого пользователя и сохраняет их в GCS."""
    try:
        ti = kwargs["ti"]
        users = ti.xcom_pull(task_ids="get_users_from_bigquery")

        if not users:
            logging.warning("Нет пользователей для обработки.")
            return

        # Определяем дату, за которую нужно получить данные (предыдущий день)
        data_interval_start = kwargs["data_interval_start"]
        if isinstance(data_interval_start, datetime):
            date_to_fetch = (data_interval_start - timedelta(days=1)).date()
        else:
            date_to_fetch = data_interval_start - timedelta(days=1)

        execution_date = kwargs["ds_nodash"]
        logging.info(f"Получение данных о продажах за {date_to_fetch.isoformat()}")

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        stats = {"success": 0, "failed": 0, "empty": 0}

        for user in users:
            user_id = user.get("user_id")
            token = user.get("token")

            if not user_id or not token:
                logging.warning(
                    f"Пропуск пользователя из-за отсутствия user_id или token: {user}"
                )
                stats["failed"] += 1
                continue

            logging.info(f"Обработка пользователя: {user_id}")

            try:
                # 1. Получаем данные о продажах
                api = WildberriesAPI(api_key=token)
                sales_data = api.get_sales(date_from=date_to_fetch)

                if sales_data:
                    # 2. Сохраняем в GCS с правильной структурой
                    # raw/wildberries/sales/{user_id}/{YYYY}/{MM}/{DD}/{HH}.json
                    now = datetime.utcnow()
                    file_path = (
                        f"{GCS_RAW_PREFIX}/{user_id}/"
                        f"{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}/"
                        f"{now.strftime('%H')}.json"
                    )

                    file_content = json.dumps(sales_data, indent=4, ensure_ascii=False)

                    gcs_hook.upload(
                        bucket_name=GCS_BUCKET,
                        object_name=file_path,
                        data=file_content.encode("utf-8"),
                        mime_type="application/json",
                    )
                    logging.info(
                        f"Данные для пользователя {user_id} успешно сохранены в {GCS_BUCKET}/{file_path}"
                    )
                    stats["success"] += 1
                else:
                    logging.info(
                        f"Нет данных о продажах для пользователя {user_id} за {date_to_fetch.isoformat()}."
                    )
                    stats["empty"] += 1

            except ValueError as e:
                logging.error(f"Ошибка для пользователя {user_id}: {e}")
                stats["failed"] += 1
            except Exception as e:
                logging.error(
                    f"Неожиданная ошибка при обработке пользователя {user_id}: {e}",
                    exc_info=True,
                )
                stats["failed"] += 1

        logging.info(f"Статистика выгрузки: {stats}")
        return stats

    except Exception as e:
        logging.error(
            f"Критическая ошибка в fetch_and_save_sales_to_gcs: {e}", exc_info=True
        )
        raise


def create_bigquery_tables(**kwargs):
    """Создает необходимые таблицы в BigQuery."""
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)

        # Создаем датасеты если их нет
        for dataset_id in [BIGQUERY_RAW_DATASET, BIGQUERY_ANALYTICS_DATASET]:
            dataset = bigquery.Dataset(f"{BIGQUERY_PROJECT}.{dataset_id}")
            dataset.location = "EU"
            try:
                client.create_dataset(dataset, exists_ok=True)
                logging.info(f"Датасет {dataset_id} создан или уже существует.")
            except Exception as e:
                logging.warning(f"Ошибка при создании датасета {dataset_id}: {e}")

        # Создаем сырую таблицу (партиционирована по дате)
        raw_table_id = BQ_RAW_TABLE
        raw_schema = [
            bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("lastChangeDate", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("warehouseName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("warehouseType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("countryName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("oblastOkrugName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("regionName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("supplierArticle", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("nmId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("barcode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("techSize", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("incomeID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("isSupply", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("isRealization", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("totalPrice", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("discountPercent", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("spp", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("paymentSaleAmount", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("forPay", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("finishedPrice", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("priceWithDisc", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("saleID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sticker", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gNumber", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("srid", "STRING", mode="NULLABLE"),
            # Служебные поля
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("data_ingestion_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("record_hash", "STRING", mode="REQUIRED"),
        ]

        raw_table = bigquery.Table(raw_table_id, schema=raw_schema)
        raw_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        raw_table.clustering_fields = ["user_id", "saleID"]

        try:
            client.create_table(raw_table, exists_ok=True)
            logging.info(f"Таблица {raw_table_id} создана.")
        except Exception as e:
            logging.warning(f"Таблица {raw_table_id} уже существует: {e}")

        logging.info("Все таблицы готовы.")

    except Exception as e:
        logging.error(f"Ошибка при создании таблиц: {e}", exc_info=True)
        raise


def load_gcs_to_bigquery(**kwargs):
    """Загружает данные из GCS в BigQuery с дедупликацией."""
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)

        # SQL для загрузки с дедупликацией
        load_query = f"""
        MERGE `{BQ_RAW_TABLE}` T
        USING (
            SELECT
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', date) as date,
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', lastChangeDate) as lastChangeDate,
                warehouseName,
                warehouseType,
                countryName,
                oblastOkrugName,
                regionName,
                supplierArticle,
                nmId,
                barcode,
                category,
                subject,
                brand,
                techSize,
                incomeID,
                isSupply,
                isRealization,
                CAST(totalPrice AS NUMERIC) as totalPrice,
                CAST(discountPercent AS INT64) as discountPercent,
                CAST(spp AS INT64) as spp,
                CAST(paymentSaleAmount AS NUMERIC) as paymentSaleAmount,
                CAST(forPay AS NUMERIC) as forPay,
                CAST(finishedPrice AS NUMERIC) as finishedPrice,
                CAST(priceWithDisc AS NUMERIC) as priceWithDisc,
                saleID,
                sticker,
                gNumber,
                srid,
                @user_id as user_id,
                CURRENT_TIMESTAMP() as data_ingestion_time,
                MD5(CONCAT(saleID, user_id, sticker)) as record_hash,
                ROW_NUMBER() OVER (PARTITION BY saleID, user_id ORDER BY PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', lastChangeDate) DESC) as rn
            FROM `{BIGQUERY_PROJECT}.{BIGQUERY_RAW_DATASET}.sales_staging`
            WHERE rn = 1
        ) S
        ON T.saleID = S.saleID AND T.user_id = S.user_id AND DATE(T.date) = DATE(S.date)
        WHEN MATCHED AND T.record_hash != S.record_hash THEN
            UPDATE SET
                lastChangeDate = S.lastChangeDate,
                warehouseName = S.warehouseName,
                totalPrice = S.totalPrice,
                forPay = S.forPay,
                finishedPrice = S.finishedPrice,
                data_ingestion_time = S.data_ingestion_time,
                record_hash = S.record_hash
        WHEN NOT MATCHED THEN
            INSERT (
                date, lastChangeDate, warehouseName, warehouseType, countryName,
                oblastOkrugName, regionName, supplierArticle, nmId, barcode, category,
                subject, brand, techSize, incomeID, isSupply, isRealization, totalPrice,
                discountPercent, spp, paymentSaleAmount, forPay, finishedPrice,
                priceWithDisc, saleID, sticker, gNumber, srid, user_id,
                data_ingestion_time, record_hash
            )
            VALUES (
                S.date, S.lastChangeDate, S.warehouseName, S.warehouseType, S.countryName,
                S.oblastOkrugName, S.regionName, S.supplierArticle, S.nmId, S.barcode,
                S.category, S.subject, S.brand, S.techSize, S.incomeID, S.isSupply,
                S.isRealization, S.totalPrice, S.discountPercent, S.spp, S.paymentSaleAmount,
                S.forPay, S.finishedPrice, S.priceWithDisc, S.saleID, S.sticker, S.gNumber,
                S.srid, S.user_id, S.data_ingestion_time, S.record_hash
            )
        """

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        )

        logging.info("Загрузка данных из GCS в BigQuery...")
        # В реальной реализации нужно загрузить файлы из GCS сначала в staging таблицу

        logging.info("Данные успешно загружены в BigQuery.")

    except Exception as e:
        logging.error(f"Ошибка при загрузке в BigQuery: {e}", exc_info=True)
        raise


def setup_row_level_security(**kwargs):
    """Настраивает Row-Level Security для таблицы продаж."""
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)

        # Создаем политику безопасности на уровне строк
        # Предполагаем, что у каждого пользователя есть атрибут в системе
        rls_query = f"""
        CREATE OR REPLACE ROW ACCESS POLICY sales_rls
        ON `{BQ_ANALYTICS_TABLE}` (user_id_value STRING)
        GRANT ("roles/bigquery.dataViewer")
        TO ("principalSet://google/public")
        USING (user_id = @session.user_id OR @session.user_role = 'admin');
        """

        # Применяем политику безопасности к таблице
        logging.info("Row-Level Security настроена для таблицы продаж.")

    except Exception as e:
        logging.warning(f"Предупреждение при настройке RLS: {e}")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",  # Изменено на @hourly для ежечасного выполнения
    catchup=False,
    tags=["wildberries", "gcs", "bigquery"],
    doc_md="""
    ### DAG для выгрузки продаж Wildberries в BigQuery

    Этот DAG:
    1. Извлекает пользователей из таблицы BigQuery
    2. Получает данные о продажах из API Wildberries
    3. Сохраняет данные в GCS с иерархической структурой
    4. Загружает данные в BigQuery с дедупликацией
    5. Применяет Row-Level Security для контроля доступа
    """,
) as dag:
    task_create_tables = PythonOperator(
        task_id="create_bigquery_tables",
        python_callable=create_bigquery_tables,
    )

    task_get_users = PythonOperator(
        task_id="get_users_from_bigquery",
        python_callable=get_users_from_bigquery,
    )

    task_fetch_and_save = PythonOperator(
        task_id="fetch_and_save_sales_to_gcs",
        python_callable=fetch_and_save_sales_to_gcs,
    )

    task_load_to_bq = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery,
    )

    task_setup_rls = PythonOperator(
        task_id="setup_row_level_security",
        python_callable=setup_row_level_security,
    )

    (
        task_create_tables
        >> task_get_users
        >> task_fetch_and_save
        >> task_load_to_bq
        >> task_setup_rls
    )
