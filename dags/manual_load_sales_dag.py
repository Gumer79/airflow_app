#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
DAG для ручной загрузки данных о продажах из GCS в BigQuery.

Использование:
    1. Откройте Airflow UI
    2. Найдите DAG 'manual_load_sales_to_bq'
    3. Нажмите "Trigger DAG w/ config"
    4. Укажите параметры в JSON:
       {
         "company_id": "bf4c3c85-462d-4567-b1ac-05b87acc478b",
         "start_date": "2025-08-08",
         "end_date": "2025-11-06"
       }
"""
import os 
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery as bq_client
from google.cloud.exceptions import NotFound

from utilities.config import (
    GCP_CONN_ID,
    BIGQUERY_PROJECT,
    BIGQUERY_WILDBERRIES_DATASET,
    BIGQUERY_SALES_TABLE,
    GCS_BUCKET,
    BIGQUERY_LOCATION,
    BIGQUERY_BATCH_SIZE,
)


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")



def load_sales_to_bigquery(**context):
    """
    Загружает данные о продажах из GCS в BigQuery.
    """
    # Получаем параметры из конфигурации DAG run
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    company_id = conf.get('company_id')
    start_date_str = conf.get('start_date')
    end_date_str = conf.get('end_date')
    
    if not all([company_id, start_date_str, end_date_str]):
        raise ValueError(
            "Необходимо указать параметры: company_id, start_date, end_date\n"
            "Пример конфигурации:\n"
            '{\n'
            '  "company_id": "bf4c3c85-462d-4567-b1ac-05b87acc478b",\n'
            '  "start_date": "2025-08-08",\n'
            '  "end_date": "2025-11-06"\n'
            '}'
        )
    
    try:
        start_date = datetime.strptime(str(start_date_str), '%Y-%m-%d').date()
        end_date = datetime.strptime(str(end_date_str), '%Y-%m-%d').date()
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Ошибка формата даты: {e}. Используйте формат YYYY-MM-DD")
    
    if start_date > end_date:
        raise ValueError("Начальная дата должна быть меньше или равна конечной дате")
    
    logging.info("")
    logging.info("=" * 80)
    logging.info("📊 ЗАГРУЗКА ДАННЫХ ИЗ GCS В BIGQUERY")
    logging.info("=" * 80)
    logging.info(f"🏢 Компания ID: {company_id}")
    logging.info(f"📅 Период: {start_date} → {end_date}")
    logging.info("=" * 80)

    # Инициализация hooks
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    
    credentials = bq_hook.get_credentials()
    client = bq_client.Client(credentials=credentials, project=BIGQUERY_PROJECT)

    # Проверяем/создаем dataset
    dataset_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}"
    
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"✅ Dataset {BIGQUERY_WILDBERRIES_DATASET} существует")
    except NotFound:
        dataset = bq_client.Dataset(dataset_ref)
        dataset.location = BIGQUERY_LOCATION
        client.create_dataset(dataset, exists_ok=True)
        logging.info(f"🆕 Dataset {BIGQUERY_WILDBERRIES_DATASET} создан")

    # Схема таблицы
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
        bq_client.SchemaField("company_id", "STRING", mode="REQUIRED"),
        bq_client.SchemaField("data_ingestion_time", "TIMESTAMP", mode="REQUIRED"),
    ]

    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}.{BIGQUERY_SALES_TABLE}"
    table_ref = bq_client.Table(table_id, schema=schema)
    table_ref.time_partitioning = bq_client.TimePartitioning(
        type_=bq_client.TimePartitioningType.DAY, field="date"
    )

    # Проверяем/создаем таблицу
    try:
        client.get_table(table_id)
        logging.info(f"✅ Таблица {BIGQUERY_SALES_TABLE} существует")
    except NotFound:
        client.create_table(table_ref, exists_ok=True)
        logging.info(f"🆕 Таблица {BIGQUERY_SALES_TABLE} создана")

    # Читаем файлы из GCS
    logging.info("")
    logging.info("📂 Чтение файлов из GCS...")
    
    all_rows = []
    data_ingestion_time = datetime.utcnow()
    files_processed = 0
    files_not_found = 0

    current_date = start_date
    while current_date <= end_date:
        file_path = f"wildberries/sales/{current_date.isoformat()}/{company_id}.json"

        try:
            if gcs_hook.exists(bucket_name=GCS_BUCKET, object_name=file_path):
                file_content = gcs_hook.download(
                    bucket_name=GCS_BUCKET, object_name=file_path
                )

                if isinstance(file_content, bytes):
                    file_content = file_content.decode("utf-8")

                sales_data = json.loads(file_content)

                if isinstance(sales_data, list):
                    for sale in sales_data:
                        sale_record = sale.copy()
                        sale_record["company_id"] = company_id
                        sale_record["data_ingestion_time"] = (
                            data_ingestion_time.isoformat() + "Z"
                        )
                        all_rows.append(sale_record)

                    files_processed += 1
                    if files_processed % 10 == 0:
                        logging.info(f"   Обработано {files_processed} файлов, записей: {len(all_rows)}")
            else:
                files_not_found += 1

        except Exception as e:
            logging.warning(f"   ⚠️  Ошибка обработки {file_path}: {e}")

        current_date += timedelta(days=1)

    logging.info("")
    logging.info(f"✅ Обработано файлов: {files_processed}")
    logging.info(f"⏭️  Файлов не найдено: {files_not_found}")
    logging.info(f"📝 Всего записей: {len(all_rows)}")

    if not all_rows:
        logging.warning("⚠️  Нет данных для загрузки в BigQuery")
        return {
            'files_processed': files_processed,
            'files_not_found': files_not_found,
            'records_loaded': 0,
            'status': 'no_data'
        }

    # Загружаем в BigQuery батчами
    logging.info("")
    logging.info(f"💾 Загрузка {len(all_rows)} записей в BigQuery...")
    
    table = client.get_table(table_id)
    
    batch_size = BIGQUERY_BATCH_SIZE
    total_batches = (len(all_rows) + batch_size - 1) // batch_size
    total_errors = []
    total_inserted = 0
    
    logging.info(f"   Всего батчей: {total_batches} (по {batch_size} записей)")
    
    for i in range(0, len(all_rows), batch_size):
        batch = all_rows[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        logging.info(f"   📦 Батч {batch_num}/{total_batches}: загрузка {len(batch)} записей...")
        
        errors = client.insert_rows_json(table, batch)
        
        if errors:
            logging.error(f"      ❌ Ошибки в батче {batch_num}:")
            for error in errors[:3]:
                logging.error(f"         {error}")
            total_errors.extend(errors)
        else:
            total_inserted += len(batch)
            logging.info(f"      ✅ Батч {batch_num} загружен успешно")
    
    if total_errors:
        error_msg = f"Ошибки при загрузке {len(total_errors)} записей из {len(all_rows)}"
        logging.error(f"❌ {error_msg}")
        logging.error(f"   Успешно загружено: {total_inserted} записей")
        raise Exception(error_msg)

    logging.info("")
    logging.info("=" * 80)
    logging.info(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО")
    logging.info(f"📊 Загружено записей: {total_inserted}")
    logging.info(f"🗄️  Таблица: {BIGQUERY_SALES_TABLE}")
    logging.info("=" * 80)
    
    return {
        'files_processed': files_processed,
        'files_not_found': files_not_found,
        'records_loaded': total_inserted,
        'status': 'success'
    }


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Ручная загрузка данных о продажах из GCS в BigQuery',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['manual', 'bigquery', 'sales', 'gcs'],
) as dag:
    
    load_task = PythonOperator(
        task_id='load_sales_to_bigquery',
        python_callable=load_sales_to_bigquery,
    )

