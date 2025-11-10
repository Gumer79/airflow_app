#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
DAG для добавления нового владельца (компании и пользователя) с отдельными задачами.

Использование:
    1. Откройте Airflow UI
    2. Найдите DAG 'add_new_owner_separate_tasks'
    3. Нажмите "Trigger DAG w/ config"
    4. Укажите параметры в JSON:
       {
         "company_name": "ООО Название компании",
         "owner": "Иванов Иван Иванович",
         "token": "wildberries_api_token_here",
         "email": "user@example.com",
         "tel_number": "+79001234567"
       }

Функционал:
    - Валидация входных данных
    - Создание компании и пользователя в BigQuery
    - Добавление IAM-разрешений для пользователя
    - Создание таблицы продаж с Row-Level Security (RLS)
    - Загрузка исторических данных продаж за последние 90 дней

Задачи выполняются последовательно с проверкой каждого шага.
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from utilities.config import BIGQUERY_DATASET, BIGQUERY_PROJECT, GCP_CONN_ID
from utilities.wildberries_api import WildberriesAPI

# Импорты для IAM
from google.cloud import resourcemanager_v3, bigquery
from google.iam.v1 import policy_pb2
import requests

COMPANIES_TABLE = "companies"
USERS_TABLE = "users"

# Константы для IAM
IAM_PROJECT_ID = "shirman-group-app"
IAM_ROLE_TO_ADD = "roles/bigquery.user"

# Константы для Sales
BIGQUERY_SALES_DATASET = "wildberries_raw"
BIGQUERY_SALES_TABLE = "sales_raw"
GCS_BUCKET = "app_s3"

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def _validate_input(**kwargs):
    """
    Валидация входных данных и их сохранение в XCom.
    """
    conf = kwargs["dag_run"].conf
    logging.info(f"Получены данные: {conf}")

    required_keys = [
        "company_name",
        "owner",
        "token",
        "email",
        "tel_number",
    ]

    if not all(key in conf for key in required_keys):
        raise AirflowException(f"Отсутствуют необходимые ключи в conf: {required_keys}")

    logging.info("✓ Все необходимые параметры присутствуют")
    
    # Возвращаем данные для последующих задач
    return {
        "company_name": conf["company_name"],
        "owner": conf["owner"],
        "token": conf["token"],
        "email": conf["email"],
        "tel_number": conf["tel_number"],
    }


def _create_company(**kwargs):
    """
    Задача 1: Создание компании в BigQuery.
    Атомарно создает компанию (если не существует) и возвращает её ID.
    """
    ti = kwargs["ti"]
    input_data = ti.xcom_pull(task_ids="validate_input")
    
    company_name = input_data["company_name"]
    owner = input_data["owner"]
    token = input_data["token"]

    logging.info(f"Начало создания компании: {company_name}")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # Атомарное создание компании с помощью MERGE
    merge_company_sql = f"""
        MERGE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}` T
        USING (SELECT @company_name AS company_name) S
        ON T.company_name = S.company_name
        WHEN NOT MATCHED THEN
          INSERT (company_id, company_name, owner, token)
          VALUES(GENERATE_UUID(), @company_name, @owner, @token);
    """

    job_configuration = {
        "query": {
            "query": merge_company_sql,
            "useLegacySql": False,
            "queryParameters": [
                {
                    "name": "company_name",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": company_name},
                },
                {
                    "name": "owner",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": owner},
                },
                {
                    "name": "token",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": token},
                },
            ],
        }
    }

    logging.info(f"Выполнение MERGE для компании: {company_name}...")
    job = bq_hook.insert_job(configuration=job_configuration)
    job.result()  # Ждем завершения
    logging.info("✓ Запрос MERGE для компании успешно выполнен.")

    # Получаем ID созданной или существующей компании
    get_company_id_sql = f"""
        SELECT company_id FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}`
        WHERE company_name = @company_name
        LIMIT 1;
    """
    
    logging.info("Получение company_id...")
    job_configuration = {
        "query": {
            "query": get_company_id_sql,
            "useLegacySql": False,
            "queryParameters": [
                {
                    "name": "company_name",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": company_name},
                }
            ],
        }
    }

    job = bq_hook.insert_job(configuration=job_configuration)
    query_results = job.result()
    
    try:
        first_row = next(query_results)
        company_id = first_row[0]
    except StopIteration:
        raise AirflowException(
            f"Не удалось найти company_id для компании '{company_name}'."
        )

    logging.info(f"✓ Company ID для '{company_name}': '{company_id}'")
    
    # Возвращаем company_id для следующей задачи
    return {
        "company_id": company_id,
        "company_name": company_name,
    }


def _create_user(**kwargs):
    """
    Задача 2: Создание пользователя в BigQuery.
    Атомарно создает пользователя для компании.
    """
    ti = kwargs["ti"]
    input_data = ti.xcom_pull(task_ids="validate_input")
    company_data = ti.xcom_pull(task_ids="create_company")
    
    email = input_data["email"]
    owner = input_data["owner"]
    tel_number = input_data["tel_number"]
    company_id = company_data["company_id"]

    logging.info(f"Начало создания пользователя: {email} для компании ID: {company_id}")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # Атомарное создание пользователя
    merge_user_sql = f"""
        MERGE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{USERS_TABLE}` T
        USING (SELECT @email AS email, @company_id AS company_id) S
        ON T.email = S.email AND T.company_id = S.company_id
        WHEN NOT MATCHED THEN
          INSERT (`user`, user_id, email, tel_number, company_id)
          VALUES (@user_name, GENERATE_UUID(), @email, @tel_number, @company_id);
    """

    job_configuration = {
        "query": {
            "query": merge_user_sql,
            "useLegacySql": False,
            "queryParameters": [
                {
                    "name": "user_name",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": owner},
                },
                {
                    "name": "email",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": email},
                },
                {
                    "name": "tel_number",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": tel_number},
                },
                {
                    "name": "company_id",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": company_id},
                },
            ],
        }
    }

    logging.info("Выполнение MERGE для пользователя...")
    job = bq_hook.insert_job(configuration=job_configuration)
    job.result()  # Ждем завершения
    logging.info(f"✓ Операция для пользователя с email '{email}' успешно завершена.")
    
    return {
        "email": email,
        "user_name": owner,
    }


def _add_user_to_iam_policy(**kwargs):
    """
    Задача 3: Добавление пользователя в IAM Policy проекта и датасетов.
    """
    ti = kwargs["ti"]
    user_data = ti.xcom_pull(task_ids="create_user")
    
    email = user_data["email"]

    logging.info(f"Начало добавления пользователя {email} в IAM-политику...")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # --- 1. Добавление в IAM политику проекта ---
    try:
        credentials = bq_hook.get_credentials()
        iam_client = resourcemanager_v3.ProjectsClient(credentials=credentials)

        project_name = f"projects/{IAM_PROJECT_ID}"
        member_to_add = f"user:{email}"

        # Получаем текущую политику
        policy = iam_client.get_iam_policy(request={"resource": project_name})

        # Делаем операцию идемпотентной
        binding_found = False
        role_updated = False

        for binding in policy.bindings:
            if binding.role == IAM_ROLE_TO_ADD:
                binding_found = True
                if member_to_add not in binding.members:
                    binding.members.append(member_to_add)
                    role_updated = True
                    logging.info(
                        f"✓ Пользователь {email} добавлен в роль {IAM_ROLE_TO_ADD}."
                    )
                else:
                    logging.info(
                        f"✓ Пользователь {email} уже имеет роль {IAM_ROLE_TO_ADD}."
                    )
                break

        # Если роль не найдена, создаем новую привязку
        if not binding_found:
            new_binding = policy_pb2.Binding(
                role=IAM_ROLE_TO_ADD, members=[member_to_add]
            )
            policy.bindings.append(new_binding)
            role_updated = True
            logging.info(f"✓ Создана новая привязка роли {IAM_ROLE_TO_ADD} для {email}.")

        # Устанавливаем обновленную политику
        if role_updated:
            iam_client.set_iam_policy(
                request={"resource": project_name, "policy": policy}
            )
            logging.info(f"✓ IAM-политика проекта успешно обновлена для {email}.")
        else:
            logging.info("✓ Обновление IAM-политики проекта не требуется.")

    except Exception as e:
        logging.error(f"✗ Не удалось обновить IAM-политику проекта: {e}")
        raise AirflowException(f"Ошибка обновления IAM-политики проекта: {e}")

    # --- 2. Добавление прав на датасеты ---
    datasets = ["wildberries_raw", "user_data"]
    
    for dataset_name in datasets:
        try:
            bq_client = bigquery.Client(
                project=BIGQUERY_PROJECT, credentials=bq_hook.get_credentials()
            )
            dataset_ref = bigquery.DatasetReference(BIGQUERY_PROJECT, dataset_name)
            dataset = bq_client.get_dataset(dataset_ref)

            entry = bigquery.AccessEntry(
                role="READER",
                entity_type="userByEmail",
                entity_id=email,
            )

            current_entries = list(dataset.access_entries)
            if entry not in current_entries:
                current_entries.append(entry)
                dataset.access_entries = current_entries
                bq_client.update_dataset(dataset, ["access_entries"])
                logging.info(
                    f"✓ Пользователь {email} добавлен как READER на датасет {dataset_name}."
                )
            else:
                logging.info(
                    f"✓ Пользователь {email} уже имеет доступ READER к датасету {dataset_name}."
                )
                
        except Exception as e:
            logging.error(f"✗ Не удалось обновить права на датасет {dataset_name}: {e}")
            # Не прерываем выполнение, если одна из датасетов не обновилась

    logging.info(f"✓ Все IAM операции для пользователя {email} завершены!")


def _check_and_load_sales_data(**kwargs):
    """
    Задача 4: Проверка наличия данных о продажах и загрузка за последние 3 месяца.
    Проверяет, есть ли данные о продажах для компании в BigQuery.
    Если нет, загружает данные за последние 3 месяца.
    """
    ti = kwargs["ti"]
    input_data = ti.xcom_pull(task_ids="validate_input")
    company_data = ti.xcom_pull(task_ids="create_company")
    
    company_id = company_data["company_id"]
    company_name = company_data["company_name"]
    token = input_data["token"]

    logging.info(f"Проверка наличия данных о продажах для компании: {company_name} (ID: {company_id})")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # Проверяем наличие данных о продажах для этой компании
    check_sales_sql = f"""
        SELECT COUNT(*) as sales_count
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}.{BIGQUERY_SALES_TABLE}`
        WHERE company_id = @company_id
        LIMIT 1;
    """

    job_configuration = {
        "query": {
            "query": check_sales_sql,
            "useLegacySql": False,
            "queryParameters": [
                {
                    "name": "company_id",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": company_id},
                }
            ],
        }
    }

    try:
        job = bq_hook.insert_job(configuration=job_configuration)
        query_results = job.result()
        first_row = next(query_results)
        sales_count = first_row[0]

        if sales_count > 0:
            logging.info(
                f"✓ Данные о продажах для компании {company_name} уже существуют ({sales_count} записей). Загрузка не требуется."
            )
            return {"status": "skipped", "reason": "sales_data_exists", "count": sales_count}

    except Exception as e:
        logging.warning(
            f"Не удалось проверить наличие данных о продажах (возможно, таблица не существует): {e}"
        )
        # Продолжаем загрузку данных

    # Загрузка данных за последние 3 месяца
    logging.info("")
    logging.info("=" * 80)
    logging.info("📦 ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ О ПРОДАЖАХ")
    logging.info("=" * 80)

    # Получаем даты за последние 3 месяца
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)  # Примерно 3 месяца
    # start_date = end_date - timedelta(days=10)  # Примерно 10 дней
    total_days = (end_date - start_date).days + 1
    
    # Рассчитываем приблизительное время выполнения
    estimated_minutes = total_days  # 1 запрос в минуту
    estimated_hours = estimated_minutes / 60

    logging.info(f"📅 Период: {start_date} → {end_date} ({total_days} дней)")
    logging.info(f"🏢 Компания: {company_name} (ID: {company_id})")
    logging.info(f"⏱️  Примерное время: ~{estimated_minutes} мин. (~{estimated_hours:.1f} ч.)")
    logging.info(f"ℹ️  Лимит API: 1 запрос в минуту на аккаунт")
    logging.info("=" * 80)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    wb_api = WildberriesAPI(api_key=token)

    total_records = 0
    successful_days = 0
    failed_days = 0
    empty_days = 0

    # Загружаем данные по дням с обработкой rate limiting
    current_date = start_date
    day_counter = 0
    
    while current_date <= end_date:
        day_counter += 1
        max_retries = 5
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                logging.info(f"📅 [{day_counter}/{total_days}] Обработка: {current_date.isoformat()}")
                
                if retry_count > 0:
                    logging.info(f"   🔄 Повторная попытка #{retry_count}")

                sales_data = wb_api.get_sales(date_from=current_date, flag=1)

                if sales_data:
                    # Сохраняем в GCS
                    file_name = f"wildberries/sales/{current_date.isoformat()}/{company_id}.json"

                    gcs_hook.upload(
                        bucket_name=GCS_BUCKET,
                        object_name=file_name,
                        data=json.dumps(sales_data, indent=4, ensure_ascii=False),
                        mime_type="application/json",
                    )

                    total_records += len(sales_data)
                    successful_days += 1
                    logging.info(f"   ✅ Успешно: {len(sales_data)} записей сохранено в GCS")
                else:
                    empty_days += 1
                    logging.info(f"   📭 Нет данных о продажах")

                success = True
                
                # Добавляем задержку между запросами (60 секунд)
                # Wildberries API: Лимит 1 запрос в минуту на один аккаунт продавца
                if current_date < end_date:  # Не ждем после последнего запроса
                    logging.info(f"   ⏳ Ожидание 60 сек. (лимит API: 1 запрос/минуту)...")
                    time.sleep(60)

            except requests.exceptions.HTTPError as http_err:
                # Проверяем, является ли это ошибкой rate limiting (429)
                if http_err.response.status_code == 429:
                    retry_count += 1
                    if retry_count < max_retries:
                        # Ожидание 60 секунд + дополнительное время при повторных попытках
                        wait_time = 60 + (retry_count * 30)  # 60, 90, 120, 150, 180 секунд
                        logging.warning(
                            f"   ⚠️  Rate limit exceeded! Ожидание {wait_time} сек. перед повторной попыткой..."
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(f"   ❌ Превышен лимит повторных попыток для {current_date}")
                        failed_days += 1
                        break
                else:
                    # Другая HTTP ошибка - логируем и переходим к следующему дню
                    logging.error(f"   ❌ HTTP ошибка {http_err.response.status_code}: {str(http_err)}")
                    failed_days += 1
                    break
                    
            except Exception as e:
                logging.error(f"   ❌ Ошибка: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    # Для других ошибок используем экспоненциальную задержку
                    wait_time = min(2 ** retry_count, 60)  # Максимум 60 секунд
                    logging.warning(f"   ⏳ Ожидание {wait_time} сек. перед повторной попыткой...")
                    time.sleep(wait_time)
                else:
                    failed_days += 1
                    break

        # Переходим к следующему дню
        current_date += timedelta(days=1)

    # Итоговая статистика загрузки
    logging.info("")
    logging.info("=" * 80)
    logging.info("📊 ИТОГОВАЯ СТАТИСТИКА ЗАГРУЗКИ")
    logging.info("=" * 80)
    logging.info(f"✅ Успешно загружено:    {successful_days} дней ({total_records} записей)")
    logging.info(f"📭 Дней без данных:       {empty_days} дней")
    logging.info(f"❌ Ошибок:                {failed_days} дней")
    logging.info(f"📊 Всего обработано:      {day_counter} дней")
    logging.info("=" * 80)

    # Загружаем данные из GCS в BigQuery
    if total_records > 0:
        logging.info("")
        logging.info("🗄️  Начинаем загрузку данных из GCS в BigQuery...")

        try:
            _load_sales_to_bigquery(
                company_id=company_id,
                start_date=start_date,
                end_date=end_date,
                bq_hook=bq_hook,
                gcs_hook=gcs_hook,
            )
            logging.info("")
            logging.info("=" * 80)
            logging.info("✅ ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ ЗАВЕРШЕНА УСПЕШНО!")
            logging.info("=" * 80)

        except Exception as e:
            logging.error("")
            logging.error("=" * 80)
            logging.error("❌ ОШИБКА ПРИ ЗАГРУЗКЕ В BIGQUERY")
            logging.error("=" * 80)
            logging.error(f"Детали: {e}")
            raise

    else:
        logging.warning("")
        logging.warning("=" * 80)
        logging.warning("⚠️  НЕТ ДАННЫХ ДЛЯ ЗАГРУЗКИ В BIGQUERY")
        logging.warning("=" * 80)

    return {
        "status": "completed",
        "total_records": total_records,
        "successful_days": successful_days,
        "failed_days": failed_days,
        "empty_days": empty_days,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def _load_sales_to_bigquery(company_id, start_date, end_date, bq_hook, gcs_hook):
    """
    Вспомогательная функция для загрузки данных из GCS в BigQuery.
    """
    from google.cloud import bigquery as bq_client
    from google.cloud.exceptions import NotFound

    logging.info("")
    logging.info("─" * 80)
    logging.info(f"📊 Подготовка к загрузке в BigQuery")
    logging.info(f"🏢 Компания: {company_id}")
    logging.info(f"📅 Период: {start_date} → {end_date}")
    logging.info("─" * 80)

    # Получаем credentials
    credentials = bq_hook.get_credentials()
    client = bq_client.Client(credentials=credentials, project=BIGQUERY_PROJECT)

    # Создаем dataset если его нет
    dataset_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}"

    try:
        client.get_dataset(dataset_ref)
        logging.info(f"✅ Dataset {BIGQUERY_SALES_DATASET} уже существует")
    except NotFound:
        dataset = bq_client.Dataset(dataset_ref)
        dataset.location = "europe-central2"
        client.create_dataset(dataset, exists_ok=True)
        logging.info(f"🆕 Dataset {BIGQUERY_SALES_DATASET} создан")

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
        bq_client.SchemaField("company_id", "STRING", mode="REQUIRED"),
        bq_client.SchemaField("data_ingestion_time", "TIMESTAMP", mode="REQUIRED"),
    ]

    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}.{BIGQUERY_SALES_TABLE}"
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

    # Читаем и обрабатываем файлы за указанный период
    logging.info("")
    logging.info(f"📂 Чтение файлов из GCS...")
    
    all_rows = []
    data_ingestion_time = datetime.utcnow()
    files_processed = 0
    files_skipped = 0

    current_date = start_date
    while current_date <= end_date:
        file_path = f"wildberries/sales/{current_date.isoformat()}/{company_id}.json"

        try:
            # Проверяем существование файла и читаем его
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
                    if files_processed % 10 == 0:  # Логируем каждые 10 файлов
                        logging.info(f"   Обработано {files_processed} файлов, записей: {len(all_rows)}")
            else:
                files_skipped += 1

        except Exception as e:
            logging.warning(f"   ⚠️  Ошибка обработки {file_path}: {e}")
            files_skipped += 1

        current_date += timedelta(days=1)

    logging.info("")
    logging.info(f"✅ Обработано файлов: {files_processed}")
    logging.info(f"⏭️  Пропущено файлов: {files_skipped}")
    logging.info(f"📝 Всего записей: {len(all_rows)}")

    if not all_rows:
        logging.warning("⚠️  Нет данных для загрузки в BigQuery")
        return

    # Загружаем данные в BigQuery батчами (чтобы избежать ошибки 413)
    logging.info("")
    logging.info(f"💾 Загрузка {len(all_rows)} записей в BigQuery...")
    
    table = client.get_table(table_id)
    
    # Разбиваем данные на батчи по 5000 записей
    batch_size = 5000
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
            for error in errors[:3]:  # Показываем первые 3 ошибки
                logging.error(f"         {error}")
            total_errors.extend(errors)
        else:
            total_inserted += len(batch)
            logging.info(f"      ✅ Батч {batch_num} загружен успешно")
    
    if total_errors:
        logging.error(f"❌ Всего ошибок при загрузке: {len(total_errors)}")
        logging.error(f"   Успешно загружено: {total_inserted} записей")
        raise Exception(f"Ошибки при загрузке {len(total_errors)} записей")

    logging.info(f"✅ Успешно загружено {total_inserted} записей в {BIGQUERY_SALES_TABLE}")


# Определение DAG
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "api", "users", "iam", "modular", "sales"],
    doc_md="""
    ### DAG для создания Компании и Пользователя (Модульный подход)
    
    Этот DAG разбит на отдельные задачи для лучшей модульности и отладки:
    
    1. **validate_input**: Валидация входных данных
    2. **create_company**: Атомарное создание компании в BigQuery
    3. **create_user**: Атомарное создание пользователя в BigQuery
    4. **add_user_to_iam_policy**: Добавление пользователя в IAM политику проекта и датасетов
    5. **check_and_load_sales_data**: Проверка наличия данных о продажах и загрузка за последние 3 месяца (если данных нет)
    
    Каждая задача выполняется независимо и использует XCom для передачи данных между задачами.
    
    **Требуемые параметры конфигурации:**
    - company_name: Название компании
    - owner: Имя владельца
    - token: Токен компании (API ключ Wildberries)
    - email: Email пользователя
    - tel_number: Телефон пользователя
    
    **Что делает DAG:**
    - Создает новую компанию в БД (если её нет)
    - Создает нового пользователя для компании
    - Назначает права доступа в BigQuery
    - Автоматически загружает исторические данные о продажах за последние 3 месяца (если данных нет в БД)
    
    **⚠️ ВАЖНО - Время выполнения:**
    - Wildberries API имеет лимит: **1 запрос в минуту** на аккаунт продавца
    - Загрузка данных за 90 дней займет примерно **1.5 часа**
    - DAG автоматически соблюдает лимиты API с задержками между запросами
    - Используется retry логика при ошибках rate limiting
    """,
) as dag:
    
    # Задача 0: Валидация входных данных
    validate_input_task = PythonOperator(
        task_id="validate_input",
        python_callable=_validate_input,
    )
    
    # Задача 1: Создание компании
    create_company_task = PythonOperator(
        task_id="create_company",
        python_callable=_create_company,
    )
    
    # Задача 2: Создание пользователя
    create_user_task = PythonOperator(
        task_id="create_user",
        python_callable=_create_user,
    )
    
    # Задача 3: Добавление пользователя в IAM Policy
    add_to_iam_task = PythonOperator(
        task_id="add_user_to_iam_policy",
        python_callable=_add_user_to_iam_policy,
    )
    
    # Задача 4: Проверка и загрузка данных о продажах за последние 3 месяца
    check_and_load_sales_task = PythonOperator(
        task_id="check_and_load_sales_data",
        python_callable=_check_and_load_sales_data,
    )
    
    # Определение зависимостей между задачами
    validate_input_task >> create_company_task >> create_user_task >> add_to_iam_task >> check_and_load_sales_task

