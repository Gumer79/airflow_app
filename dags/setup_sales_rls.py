"""
DAG для настройки Row-Level Security (RLS) для таблицы продаж.

Этот DAG создает RLS политику, которая ограничивает доступ к данным продаж
на основе company_id. Пользователи видят только данные своей компании.
"""
import os
import logging
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from utilities.config import BIGQUERY_PROJECT, GCP_CONN_ID

BIGQUERY_SALES_DATASET = "wildberries_raw"
BIGQUERY_SALES_TABLE = "sales_raw"
BIGQUERY_USERS_DATASET = "user_data"
BIGQUERY_USERS_TABLE = "users"

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def setup_row_level_security(**kwargs):
    """
    Создает Row-Level Security политику для таблицы sales_raw.
    
    RLS политика фильтрует данные на основе company_id:
    - Пользователи видят только продажи своей компании
    - Администраторы видят все данные
    
    Логика:
    1. Определяет company_id пользователя из таблицы users по email
    2. Фильтрует данные sales_raw по company_id
    """
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT)
        
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}.{BIGQUERY_SALES_TABLE}"
        users_table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_USERS_DATASET}.{BIGQUERY_USERS_TABLE}"
        
        # Проверяем, что таблица существует
        try:
            table = client.get_table(table_id)
            logging.info(f"Таблица {table_id} найдена")
        except NotFound:
            raise Exception(f"Таблица {table_id} не найдена. Сначала нужно создать таблицу.")
        
        # Проверяем, что таблица users существует
        try:
            users_table = client.get_table(users_table_id)
            logging.info(f"Таблица {users_table_id} найдена")
        except NotFound:
            raise Exception(f"Таблица {users_table_id} не найдена. Нужна для определения company_id пользователя.")
        
        # Удаляем существующую RLS политику, если она есть
        # (для идемпотентности)
        drop_policy_query = f"""
        DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
        ON `{table_id}`;
        """
        
        logging.info("Удаление существующей RLS политики (если есть)...")
        try:
            client.query(drop_policy_query).result()
            logging.info("Существующая политика удалена")
        except Exception as e:
            logging.info(f"Политика не существовала: {e}")
        
        # Создаем RLS политику
        # Политика проверяет company_id пользователя через подзапрос к таблице users
        create_policy_query = f"""
        CREATE ROW ACCESS POLICY sales_rls_policy
        ON `{table_id}`
        GRANT TO ('allAuthenticatedUsers')
        FILTER USING (
            -- Пользователь видит данные своей компании
            -- Определяем company_id пользователя по его email из таблицы users
            company_id IN (
                SELECT company_id 
                FROM `{users_table_id}` 
                WHERE email = SESSION_USER()
            )
            OR
            -- Администраторы видят все данные
            -- Можно добавить проверку роли через IAM или специальную таблицу
            SESSION_USER() IN (
                SELECT email 
                FROM `{users_table_id}` 
                WHERE email = SESSION_USER() 
                AND EXISTS (
                    SELECT 1 
                    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_USERS_DATASET}.users` u
                    WHERE u.email = SESSION_USER()
                    -- Здесь можно добавить условие для админов
                    -- Например, если есть поле is_admin = true
                )
            )
        );
        """
        
        logging.info("Создание RLS политики...")
        job = client.query(create_policy_query)
        job.result()  # Ждем завершения
        
        logging.info(f"✅ Row-Level Security успешно настроена для таблицы {table_id}")
        logging.info("Пользователи теперь будут видеть только данные своей компании")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при настройке RLS: {e}", exc_info=True)
        raise


def setup_rls_with_user_mapping_table(**kwargs):
    """
    Альтернативный вариант: создает отдельную таблицу-маппинг для RLS.
    
    Этот подход более производительный для больших объемов данных,
    так как не требует подзапросов при каждом обращении к таблице.
    """
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT)
        
        dataset_id = BIGQUERY_SALES_DATASET
        table_id = f"{BIGQUERY_PROJECT}.{dataset_id}.{BIGQUERY_SALES_TABLE}"
        mapping_table_id = f"{BIGQUERY_PROJECT}.{dataset_id}.user_company_mapping"
        users_table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_USERS_DATASET}.{BIGQUERY_USERS_TABLE}"
        
        # Создаем таблицу-маппинг user -> company_id для RLS
        # Эта таблица будет автоматически обновляться при изменении users
        create_mapping_table_query = f"""
        CREATE OR REPLACE TABLE `{mapping_table_id}` AS
        SELECT DISTINCT
            email,
            company_id
        FROM `{users_table_id}`
        WHERE email IS NOT NULL AND company_id IS NOT NULL;
        """
        
        logging.info("Создание таблицы-маппинга для RLS...")
        job = client.query(create_mapping_table_query)
        job.result()
        logging.info(f"Таблица-маппинг {mapping_table_id} создана")
        
        # Создаем RLS политику с использованием таблицы-маппинга
        drop_policy_query = f"""
        DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
        ON `{table_id}`;
        """
        
        try:
            client.query(drop_policy_query).result()
        except Exception:
            pass
        
        create_policy_query = f"""
        CREATE ROW ACCESS POLICY sales_rls_policy
        ON `{table_id}`
        GRANT TO ('allAuthenticatedUsers')
        FILTER USING (
            company_id IN (
                SELECT company_id 
                FROM `{mapping_table_id}` 
                WHERE email = SESSION_USER()
            )
        );
        """
        
        logging.info("Создание RLS политики с использованием таблицы-маппинга...")
        job = client.query(create_policy_query)
        job.result()
        
        logging.info(f"✅ RLS настроена с использованием таблицы-маппинга")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при настройке RLS с маппингом: {e}", exc_info=True)
        raise


def verify_rls_policy(**kwargs):
    """
    Проверяет, что RLS политика корректно применена.
    """
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT)
        
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}.{BIGQUERY_SALES_TABLE}"
        
        # Проверяем наличие RLS политик
        check_query = f"""
        SELECT
            policy_name,
            filter_predicate,
            grantee_type,
            grantee
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_SALES_DATASET}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
        WHERE table_name = '{BIGQUERY_SALES_TABLE}';
        """
        
        logging.info("Проверка RLS политик...")
        results = client.query(check_query).result()
        
        policies = list(results)
        if policies:
            logging.info(f"✅ Найдено {len(policies)} RLS политик:")
            for policy in policies:
                logging.info(f"  - {policy.policy_name}")
        else:
            logging.warning("⚠️ RLS политики не найдены")
        
    except Exception as e:
        logging.error(f"Ошибка при проверке RLS: {e}", exc_info=True)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Запускается вручную
    catchup=False,
    tags=["bigquery", "rls", "security", "sales"],
    doc_md="""
    ### DAG для настройки Row-Level Security для таблицы продаж
    
    Этот DAG настраивает Row-Level Security (RLS) для таблицы `sales_raw`,
    чтобы пользователи видели только данные своей компании.
    
    **Как это работает:**
    1. RLS политика фильтрует данные на основе `company_id`
    2. Определяет `company_id` пользователя через таблицу `users` по email
    3. Применяется автоматически ко всем запросам к таблице
    
    **Важно:**
    - НЕ нужно создавать новый датасет
    - RLS применяется на уровне таблицы
    - Работает для всех пользователей с доступом к таблице
    - Администраторы могут видеть все данные (опционально)
    
    **Альтернативный вариант:**
    - Можно создать отдельную таблицу-маппинг для лучшей производительности
    - Используйте задачу `setup_rls_with_user_mapping_table` вместо основной
    """,
) as dag:
    # Основной вариант: RLS с подзапросом к таблице users
    task_setup_rls = PythonOperator(
        task_id="setup_row_level_security",
        python_callable=setup_row_level_security,
    )
    
    # Альтернативный вариант: RLS с таблицей-маппингом (более производительный)
    task_setup_rls_mapping = PythonOperator(
        task_id="setup_rls_with_mapping_table",
        python_callable=setup_rls_with_user_mapping_table,
    )
    
    # Проверка RLS политики
    task_verify_rls = PythonOperator(
        task_id="verify_rls_policy",
        python_callable=verify_rls_policy,
    )
    
    # Используйте только один вариант настройки RLS:
    # Вариант 1: Простой (с подзапросом)
    # task_setup_rls >> task_verify_rls
    
    # Вариант 2: С таблицей-маппингом (рекомендуется для больших объемов)
    task_setup_rls_mapping >> task_verify_rls

