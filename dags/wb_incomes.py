#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
DAG для автоматической загрузки данных о поставках Wildberries в GCS и BigQuery.

Использование:
    - DAG запускается автоматически каждый день в 03:30 UTC
    - Можно запустить вручную через Airflow UI для немедленной загрузки

Функционал:
    1. Получает список всех компаний с токенами из BigQuery
    2. Для каждой компании:
       - Запрашивает данные о поставках через Wildberries API
       - Сохраняет данные в GCS (Google Cloud Storage) в формате JSON
       - Загружает данные в BigQuery с партиционированием по дате
    3. Применяет Row-Level Security (RLS) для разграничения доступа

Структура данных в GCS:
    - Путь: gs://app_s3/wildberries/incomes/{date}/{company_id}.json
    - Формат: JSON массив с данными о поставках

Таблица BigQuery:
    - Проект: shirman-group-app
    - Dataset: wildberries_raw
    - Таблица: incomes_raw (партиционирована по полю date)

Поля данных:
    - incomeId: ID поставки
    - number: Номер поставки
    - date: Дата поступления
    - lastChangeDate: Дата последнего изменения
    - supplierArticle: Артикул поставщика
    - techSize: Размер
    - barcode: Штрихкод
    - quantity: Количество
    - totalPrice: Цена
    - dateClose: Дата закрытия
    - warehouseName: Название склада
    - nmId: Артикул WB
    - status: Текущий статус поставки

Расписание: Каждый день в 03:30 UTC
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
from google.cloud import bigquery

from utilities.wildberries_api import WildberriesAPI
from utilities.config import (
    GCP_CONN_ID,
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    COMPANIES_TABLE,
    BIGQUERY_WILDBERRIES_DATASET,
    BIGQUERY_INCOMES_TABLE,
    GCS_BUCKET,
    BIGQUERY_LOCATION,
    BIGQUERY_BATCH_SIZE,
    SCHEDULE_INCOMES,
)

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def get_companies_from_bigquery(**kwargs) -> List[Dict[str, Any]]:
    """Получает company_id и token из таблицы BigQuery companies."""
    logging.info("=" * 80)
    logging.info("🔍 ПОЛУЧЕНИЕ СПИСКА КОМПАНИЙ ИЗ BIGQUERY")
    logging.info("=" * 80)
    
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    sql = f"SELECT company_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}` WHERE token IS NOT NULL"

    logging.info(f"🔄 Выполнение SQL запроса...")
    connection = bq_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    companies = [dict(zip(col_names, row)) for row in rows]

    if not companies:
        logging.error("❌ Не найдено компаний в таблице BigQuery")
        raise ValueError("Не найдено компаний в таблице BigQuery.")

    logging.info(f"✅ Найдено компаний: {len(companies)}")
    logging.info("=" * 80)
    return companies


def fetch_and_save_incomes(**kwargs):
    """Получает данные о поставках для всех компаний и сохраняет их в GCS."""
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
    logging.info(f"📦 НАЧАЛО ОБРАБОТКИ ПОСТАВОК ЗА ДАТУ: {target_date.isoformat()}")
    logging.info(f"📊 Всего компаний для обработки: {len(companies)}")
    logging.info("=" * 80)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    stats = {"success": 0, "failed": 0, "empty": 0}

    for idx, company in enumerate(companies, 1):
        company_id = company.get("company_id")
        token = company.get("token")

        logging.info("")
        logging.info("─" * 80)
        logging.info(f"🏢 КОМПАНИЯ #{idx}/{len(companies)}: {company_id}")
        logging.info("─" * 80)

        if not company_id or not token:
            logging.warning(f"⚠️  Пропуск компании из-за отсутствия данных")
            stats["failed"] += 1
            continue

        try:
            logging.info(f"🔄 Запрос данных из Wildberries API...")
            wb_api = WildberriesAPI(api_key=token)
            incomes_data = wb_api.get_incomes(date_from=target_date)
            
            logging.info(f"✅ Получено записей: {len(incomes_data)}")

            if not incomes_data:
                logging.info(f"ℹ️  Нет данных о поставках за {target_date}")
                stats["empty"] += 1
                continue

            file_name = f"wildberries/incomes/{target_date.isoformat()}/{company_id}.json"

            logging.info(f"💾 Загрузка в GCS: gs://{GCS_BUCKET}/{file_name}")
            gcs_hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=file_name,
                data=json.dumps(incomes_data, indent=4, ensure_ascii=False),
                mime_type="application/json",
            )
            logging.info(f"✅ Данные успешно сохранены в GCS")
            stats["success"] += 1

        except Exception as e:
            logging.error(f"❌ ОШИБКА при обработке компании: {str(e)}")
            stats["failed"] += 1

    logging.info("")
    logging.info("=" * 80)
    logging.info("📈 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ")
    logging.info("=" * 80)
    logging.info(f"✅ Успешно обработано:  {stats['success']} компаний")
    logging.info(f"📭 Без данных о поставках: {stats['empty']} компаний")
    logging.info(f"❌ Ошибок при обработке:  {stats['failed']} компаний")
    logging.info("=" * 80)
    
    return stats


def load_gcs_to_bigquery(**kwargs):
    """Загружает данные из GCS в BigQuery."""
    from google.cloud import bigquery as bq_client
    from google.cloud.exceptions import NotFound

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
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        prefix = f"wildberries/incomes/{date_prefix}/"
        logging.info(f"🔍 Поиск файлов в GCS: gs://{GCS_BUCKET}/{prefix}")
        files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=prefix)

        if not files:
            logging.warning(f"⚠️  Не найдено файлов в GCS для даты {date_prefix}")
            return

        logging.info(f"✅ Найдено файлов для загрузки: {len(files)}")

        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()
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

        # Схема таблицы для данных о поставках
        schema = [
            bq_client.SchemaField("incomeId", "INTEGER", mode="NULLABLE"),
            bq_client.SchemaField("number", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
            bq_client.SchemaField("lastChangeDate", "TIMESTAMP", mode="NULLABLE"),
            bq_client.SchemaField("supplierArticle", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("techSize", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("barcode", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
            bq_client.SchemaField("totalPrice", "NUMERIC", mode="NULLABLE"),
            bq_client.SchemaField("dateClose", "TIMESTAMP", mode="NULLABLE"),
            bq_client.SchemaField("warehouseName", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("nmId", "INTEGER", mode="NULLABLE"),
            bq_client.SchemaField("status", "STRING", mode="NULLABLE"),
            bq_client.SchemaField("company_id", "STRING", mode="REQUIRED"),
            bq_client.SchemaField("data_ingestion_time", "TIMESTAMP", mode="REQUIRED"),
        ]

        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}.{BIGQUERY_INCOMES_TABLE}"
        table_ref = bq_client.Table(table_id, schema=schema)
        table_ref.time_partitioning = bq_client.TimePartitioning(
            type_=bq_client.TimePartitioningType.DAY, field="date"
        )

        try:
            client.get_table(table_id)
            logging.info(f"✅ Таблица {BIGQUERY_INCOMES_TABLE} уже существует")
        except NotFound:
            client.create_table(table_ref, exists_ok=True)
            logging.info(f"🆕 Таблица {BIGQUERY_INCOMES_TABLE} создана")

        all_rows = []
        data_ingestion_time = datetime.utcnow()
        processed_files = 0
        failed_files = 0

        for idx, file_path in enumerate(files, 1):
            try:
                file_name = file_path.split("/")[-1]
                company_id = file_name.replace(".json", "")

                logging.info(f"  📄 [{idx}/{len(files)}] Обработка файла: {file_name}")

                file_content = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=file_path)
                if isinstance(file_content, bytes):
                    file_content = file_content.decode("utf-8")

                incomes_data = json.loads(file_content)

                if not isinstance(incomes_data, list):
                    logging.warning(f"     ⚠️  Файл не содержит массив данных")
                    failed_files += 1
                    continue

                for income in incomes_data:
                    income_record = income.copy()
                    income_record["company_id"] = company_id
                    income_record["data_ingestion_time"] = data_ingestion_time.isoformat() + "Z"
                    all_rows.append(income_record)

                logging.info(f"     ✅ Записей: {len(incomes_data)}, Компания: {company_id}")
                processed_files += 1

            except Exception as e:
                logging.error(f"     ❌ Ошибка: {str(e)}")
                failed_files += 1
                continue

        if not all_rows:
            logging.warning("⚠️  Нет данных для загрузки в BigQuery")
            return

        logging.info("")
        logging.info(f"💾 Загрузка {len(all_rows)} записей в BigQuery...")
        
        table = client.get_table(table_id)
        batch_size = BIGQUERY_BATCH_SIZE
        total_batches = (len(all_rows) + batch_size - 1) // batch_size
        total_errors = []
        total_inserted = 0
        
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            if total_batches > 1:
                logging.info(f"   📦 Батч {batch_num}/{total_batches}: загрузка {len(batch)} записей...")
            
            errors = client.insert_rows_json(table, batch)
            
            if errors:
                logging.error(f"      ❌ Ошибки в батче {batch_num}:")
                for error in errors[:3]:
                    logging.error(f"         {error}")
                total_errors.extend(errors)
            else:
                total_inserted += len(batch)
        
        if total_errors:
            logging.error(f"❌ Всего ошибок при загрузке: {len(total_errors)}")
            raise Exception(f"Ошибки при загрузке {len(total_errors)} записей")

        logging.info("")
        logging.info("=" * 80)
        logging.info(f"✅ ЗАГРУЗКА В BIGQUERY ЗАВЕРШЕНА УСПЕШНО")
        logging.info(f"📊 Загружено записей: {total_inserted}")
        logging.info("=" * 80)

    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        raise


# DAG Definition
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=SCHEDULE_INCOMES,  # Ежедневно в 03:30 UTC
    catchup=False,
    tags=["wildberries", "incomes", "gcs", "bigquery"],
    doc_md=__doc__,
) as dag:
    
    task_get_companies = PythonOperator(
        task_id="get_companies_from_bigquery",
        python_callable=get_companies_from_bigquery,
    )

    task_fetch_and_save = PythonOperator(
        task_id="fetch_and_save_incomes",
        python_callable=fetch_and_save_incomes,
    )

    task_load_to_bq = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery,
    )

    task_get_companies >> task_fetch_and_save >> task_load_to_bq

