#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
DAG для автоматической загрузки данных о продажах Wildberries в GCS и BigQuery.

Использование:
    - DAG запускается автоматически каждый день в 03:00 UTC
    - Можно запустить вручную через Airflow UI для немедленной загрузки

Функционал:
    1. Получает список всех компаний с токенами из BigQuery
    2. Для каждой компании:
       - Запрашивает данные о продажах через Wildberries API
       - Сохраняет данные в GCS (Google Cloud Storage) в формате JSON
       - Загружает данные в BigQuery с партиционированием по дате
    3. Применяет Row-Level Security (RLS) для разграничения доступа

Структура данных в GCS:
    - Путь: gs://app_s3/wildberries/sales/{date}/{company_id}.json
    - Формат: JSON массив с данными о продажах

Таблица BigQuery:
    - Проект: shirman-group-app
    - Dataset: wildberries_raw
    - Таблица: sales_raw (партиционирована по полю date)

Расписание: Каждый день в 03:00 UTC
"""

import os
import json
import logging
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from typing import List, Dict, Any
from google.oauth2 import service_account
from google.cloud import bigquery

from utilities.wildberries_api import WildberriesAPI
from utilities.config import (
    GCP_CONN_ID,
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    COMPANIES_TABLE,
    BIGQUERY_WILDBERRIES_DATASET,
    BIGQUERY_SALES_TABLE,
    GCS_BUCKET,
    BIGQUERY_LOCATION,
    BIGQUERY_BATCH_SIZE,
    SCHEDULE_SALES,
)

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


# --- Python functions for tasks ---
def get_companies_from_bigquery(**kwargs) -> List[Dict[str, Any]]:
    """
    Получает company_id и token из таблицы BigQuery companies.
    """
    logging.info("=" * 80)
    logging.info("🔍 ПОЛУЧЕНИЕ СПИСКА КОМПАНИЙ ИЗ BIGQUERY")
    logging.info("=" * 80)
    logging.info(
        f"📊 Таблица: {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}"
    )
    
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    sql = f"SELECT company_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}` WHERE token IS NOT NULL"

    logging.info(f"🔄 Выполнение SQL запроса...")
    connection = bq_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    # Fetch all rows and column descriptions
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]

    companies = [dict(zip(col_names, row)) for row in rows]

    if not companies:
        logging.error("❌ Не найдено компаний в таблице BigQuery")
        raise ValueError("Не найдено компаний в таблице BigQuery.")

    logging.info(f"✅ Найдено компаний: {len(companies)}")
    logging.info("=" * 80)

    # Сохраняем в XCom для использования в следующих задачах
    return companies


def fetch_and_save_sales(**kwargs):
    """
    Получает данные о продажах для всех компаний и сохраняет их в GCS.
    """
    ti = kwargs["ti"]
    companies = ti.xcom_pull(task_ids="get_companies_from_bigquery")

    if not companies:
        logging.warning("❌ Нет компаний для обработки")
        return

    # Получаем данные за предыдущий день
    data_interval_start = kwargs.get("data_interval_start")
    if isinstance(data_interval_start, datetime):
        target_date = (data_interval_start - timedelta(days=1)).date()
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()

    logging.info("=" * 80)
    logging.info(f"📅 НАЧАЛО ОБРАБОТКИ ПРОДАЖ ЗА ДАТУ: {target_date.isoformat()}")
    logging.info(f"📊 Всего компаний для обработки: {len(companies)}")
    logging.info("=" * 80)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    stats = {"success": 0, "failed": 0, "empty": 0}

    for idx, company in enumerate(companies, 1):
        company_id = company.get("company_id")
        token = company.get("token")

        # Разделитель между компаниями
        logging.info("")
        logging.info("─" * 80)
        logging.info(f"🏢 КОМПАНИЯ #{idx}/{len(companies)}: {company_id}")
        logging.info("─" * 80)

        if not company_id or not token:
            logging.warning(
                f"⚠️  Пропуск компании из-за отсутствия company_id или token: {company}"
            )
            stats["failed"] += 1
            logging.info(f"📊 Статус: ПРОПУЩЕНО (нет данных)")
            continue

        try:
            # Инициализируем API и получаем данные о продажах
            logging.info(f"🔄 Запрос данных из Wildberries API...")
            wb_api = WildberriesAPI(api_key=token)
            sales_data = wb_api.get_sales(date_from=target_date, flag=1)
            
            logging.info(
                f"✅ Получено записей: {len(sales_data)}"
            )

            if not sales_data:
                logging.info(f"ℹ️  Нет данных о продажах за {target_date}")
                stats["empty"] += 1
                logging.info(f"📊 Статус: ПУСТО (нет продаж)")
                continue

            # Подготовка для загрузки в GCS
            file_name = f"wildberries/sales/{target_date.isoformat()}/{company_id}.json"

            # Загружаем данные как JSON файл
            logging.info(f"💾 Загрузка в GCS: gs://{GCS_BUCKET}/{file_name}")
            gcs_hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=file_name,
                data=json.dumps(sales_data, indent=4, ensure_ascii=False),
                mime_type="application/json",
            )
            logging.info(f"✅ Данные успешно сохранены в GCS")
            stats["success"] += 1
            logging.info(f"📊 Статус: УСПЕШНО ({len(sales_data)} записей)")

        except Exception as e:
            logging.error(f"❌ ОШИБКА при обработке компании: {str(e)}")
            logging.error(f"   Детали ошибки:", exc_info=True)
            stats["failed"] += 1
            logging.info(f"📊 Статус: ОШИБКА")

    # Итоговая статистика
    logging.info("")
    logging.info("=" * 80)
    logging.info("📈 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ")
    logging.info("=" * 80)
    logging.info(f"✅ Успешно обработано:  {stats['success']} компаний")
    logging.info(f"📭 Без данных о продажах: {stats['empty']} компаний")
    logging.info(f"❌ Ошибок при обработке:  {stats['failed']} компаний")
    logging.info(f"📊 Всего компаний:        {len(companies)}")
    logging.info("=" * 80)
    
    return stats


def load_gcs_to_bigquery(**kwargs):
    """
    Загружает данные из GCS в BigQuery.
    Читает все JSON файлы из папки wildberries/sales/{date}/ и загружает их в BigQuery.
    """
    from google.cloud import bigquery as bq_client
    from google.cloud.exceptions import NotFound

    # Получаем данные за предыдущий день
    data_interval_start = kwargs.get("data_interval_start")
    if isinstance(data_interval_start, datetime):
        target_date = (data_interval_start - timedelta(days=1)).date()
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()
    date_prefix = target_date.isoformat()

    logging.info("")
    logging.info("=" * 80)
    logging.info(f"🗄️  ЗАГРУЗКА ДАННЫХ ИЗ GCS В BIGQUERY")
    logging.info(f"📅 Дата: {date_prefix}")
    logging.info("=" * 80)

    try:
        # Инициализация клиентов
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        # Получаем список всех файлов для указанной даты
        prefix = f"wildberries/sales/{date_prefix}/"
        logging.info(f"🔍 Поиск файлов в GCS: gs://{GCS_BUCKET}/{prefix}")
        files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=prefix)

        if not files:
            logging.warning(f"⚠️  Не найдено файлов в GCS для даты {date_prefix}")
            return

        logging.info(f"✅ Найдено файлов для загрузки: {len(files)}")

        # Получаем credentials из BigQueryHook
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()

        # Инициализируем BigQuery client с полученными credentials
        client = bq_client.Client(credentials=credentials, project=BIGQUERY_PROJECT)

        # Создаем dataset если его нет
        dataset_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}"

        try:
            client.get_dataset(dataset_ref)
            logging.info(f"✅ Dataset {BIGQUERY_WILDBERRIES_DATASET} уже существует")
        except NotFound:
            dataset = bq_client.Dataset(dataset_ref)
            dataset.location = BIGQUERY_LOCATION
            client.create_dataset(dataset, exists_ok=True)
            logging.info(f"🆕 Dataset {BIGQUERY_WILDBERRIES_DATASET} создан")

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
                "company_id", "STRING", mode="REQUIRED"
            ),  # ID компании
            bq_client.SchemaField("data_ingestion_time", "TIMESTAMP", mode="REQUIRED"),
        ]

        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}.{BIGQUERY_SALES_TABLE}"
        table_ref = bq_client.Table(table_id, schema=schema)

        # Настройка партиционирования по дате
        table_ref.time_partitioning = bq_client.TimePartitioning(
            type_=bq_client.TimePartitioningType.DAY, field="date"
        )

        # Создаем таблицу если её нет
        try:
            client.get_table(table_id)
            logging.info(f"✅ Таблица {BIGQUERY_SALES_TABLE} уже существует")
        except NotFound:
            client.create_table(table_ref, exists_ok=True)
            logging.info(f"🆕 Таблица {BIGQUERY_SALES_TABLE} создана")

        # Читаем и обрабатываем все файлы
        logging.info("")
        logging.info("─" * 80)
        logging.info(f"📂 ОБРАБОТКА ФАЙЛОВ ИЗ GCS")
        logging.info("─" * 80)
        
        all_rows = []
        data_ingestion_time = datetime.utcnow()
        processed_files = 0
        failed_files = 0

        for idx, file_path in enumerate(files, 1):
            try:
                # Извлекаем company_id из имени файла (формат: wildberries/sales/{date}/{company_id}.json)
                file_name = file_path.split("/")[-1]
                company_id = file_name.replace(".json", "")

                logging.info(f"  📄 [{idx}/{len(files)}] Обработка файла: {file_name}")

                # Читаем файл из GCS
                file_content = gcs_hook.download(
                    bucket_name=GCS_BUCKET, object_name=file_path
                )

                # Парсим JSON массив
                if isinstance(file_content, bytes):
                    file_content = file_content.decode("utf-8")

                sales_data = json.loads(file_content)

                if not isinstance(sales_data, list):
                    logging.warning(f"     ⚠️  Файл не содержит массив данных")
                    failed_files += 1
                    continue

                # Обрабатываем каждую запись о продаже
                for sale in sales_data:
                    # Добавляем company_id и data_ingestion_time к каждой записи
                    sale_record = sale.copy()
                    sale_record["company_id"] = company_id
                    sale_record["data_ingestion_time"] = (
                        data_ingestion_time.isoformat() + "Z"
                    )
                    all_rows.append(sale_record)

                logging.info(f"     ✅ Записей: {len(sales_data)}, Компания: {company_id}")
                processed_files += 1

            except Exception as e:
                logging.error(f"     ❌ Ошибка: {str(e)}")
                logging.error(f"        Детали:", exc_info=True)
                failed_files += 1
                continue

        if not all_rows:
            logging.warning("⚠️  Нет данных для загрузки в BigQuery")
            return

        logging.info("")
        logging.info("─" * 80)
        logging.info(f"📊 СТАТИСТИКА ОБРАБОТКИ ФАЙЛОВ")
        logging.info("─" * 80)
        logging.info(f"✅ Успешно обработано: {processed_files} файлов")
        logging.info(f"❌ Ошибок:              {failed_files} файлов")
        logging.info(f"📝 Всего записей:       {len(all_rows)} записей")
        logging.info("─" * 80)

        # Загружаем данные в BigQuery батчами (чтобы избежать ошибки 413)
        logging.info("")
        logging.info(f"💾 Загрузка {len(all_rows)} записей в BigQuery...")
        logging.info(f"   Таблица: {BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}.{BIGQUERY_SALES_TABLE}")
        
        table = client.get_table(table_id)
        
        # Разбиваем данные на батчи
        batch_size = BIGQUERY_BATCH_SIZE
        total_batches = (len(all_rows) + batch_size - 1) // batch_size
        total_errors = []
        total_inserted = 0
        
        if total_batches > 1:
            logging.info(f"   Всего батчей: {total_batches} (по {batch_size} записей)")
        
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            if total_batches > 1:
                logging.info(f"   📦 Батч {batch_num}/{total_batches}: загрузка {len(batch)} записей...")
            
            errors = client.insert_rows_json(table, batch)
            
            if errors:
                logging.error(f"      ❌ Ошибки в батче {batch_num}:")
                for error in errors[:3]:  # Показываем первые 3 ошибки
                    logging.error(f"         {error}")
                total_errors.extend(errors)
            else:
                total_inserted += len(batch)
                if total_batches > 1:
                    logging.info(f"      ✅ Батч {batch_num} загружен успешно")
        
        if total_errors:
            logging.error(f"❌ Всего ошибок при загрузке: {len(total_errors)}")
            logging.error(f"   Успешно загружено: {total_inserted} записей")
            raise Exception(f"Ошибки при загрузке {len(total_errors)} записей")

        logging.info("")
        logging.info("=" * 80)
        logging.info(f"✅ ЗАГРУЗКА В BIGQUERY ЗАВЕРШЕНА УСПЕШНО")
        logging.info(f"📊 Загружено записей: {total_inserted}")
        logging.info(f"🗄️  Таблица: {BIGQUERY_SALES_TABLE}")
        logging.info("=" * 80)

    except Exception as e:
        logging.error("")
        logging.error("=" * 80)
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАГРУЗКЕ В BIGQUERY")
        logging.error("=" * 80)
        logging.error(f"Ошибка: {str(e)}")
        logging.error(f"Детали:", exc_info=True)
        logging.error("=" * 80)
        raise


# --- Airflow DAG Definition ---
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=SCHEDULE_SALES,  # Ежедневно в 03:00 UTC
    catchup=False,
    tags=["wildberries", "sales", "gcs", "bigquery"],
    doc_md="""
    ### Wildberries Sales to GCS and BigQuery DAG (Classic)

    This DAG fetches companies from a BigQuery table, retrieves their sales data from the Wildberries API for the previous day,
    stores the data as JSON files in Google Cloud Storage, and then loads the data into BigQuery.
    """,
) as dag:
    # Task 1: Получение компаний из BigQuery
    task_get_companies = PythonOperator(
        task_id="get_companies_from_bigquery",
        python_callable=get_companies_from_bigquery,
    )

    # Task 2: Получение и сохранение данных о продажах в GCS
    task_fetch_and_save = PythonOperator(
        task_id="fetch_and_save_sales",
        python_callable=fetch_and_save_sales,
    )

    # Task 3: Загрузка данных из GCS в BigQuery
    task_load_to_bq = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery,
    )

    # Устанавливаем зависимости
    task_get_companies >> task_fetch_and_save >> task_load_to_bq
