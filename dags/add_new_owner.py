import os
import logging
from datetime import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from utilities.config import BIGQUERY_DATASET, BIGQUERY_PROJECT, GCP_CONN_ID

COMPANIES_TABLE = "companies"
USERS_TABLE = "users"

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def _create_company_and_user_atomic(**kwargs):
    """
    Атомарно создает компанию (если не существует) и пользователя (если не связан с компанией),
    используя один Python-оператор и параметризованные запросы с MERGE.
    """
    conf = kwargs["dag_run"].conf
    logging.info(f"Получены данные: {conf}")

    # 1. Проверка входных данных
    required_keys = [
        "company_name",
        "owner",
        "token",
        "user_name",
        "email",
        "tel_number",
    ]
    print('conf', conf)

    if not all(key in conf for key in required_keys):
        raise AirflowException(f"Отсутствуют необходимые ключи в conf: {required_keys}")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # 2. Атомарное создание компании с помощью MERGE и получение её ID
    # MERGE...WHEN NOT MATCHED...INSERT создаст запись, только если её нет.
    # Мы используем UUID для надёжной генерации ID.
    merge_company_sql = f"""
                MERGE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}` T
                USING (SELECT @company_name AS company_name) S
                ON T.company_name = S.company_name
                WHEN NOT MATCHED THEN
                  INSERT (company_id, company_name, owner, token)
                  VALUES(GENERATE_UUID(), @company_name, @owner, @token);
            """
    company_name = conf.get("company_name", "ИП Новая Компания")
    owner = conf.get("owner", "Новый Владелец")
    token = conf.get("token", "your_long_token_string")
    # Конфигурация задания остается ТОЧНО ТАКОЙ ЖЕ
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

    # --- ГЛАВНОЕ ИЗМЕНЕНИЕ ---
    # Используем bq_hook.insert_job() вместо bq_hook.run()
    job = bq_hook.insert_job(configuration=job_configuration)

    # (Опционально) Можно дождаться завершения задания
    job.result()

    logging.info("Запрос MERGE успешно выполнен.")

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
        company_id = first_row[0]  # Извлекаем первое поле (company_id)
    except StopIteration:
        raise AirflowException(
            f"Не удалось найти company_id для компании '{company_name}'."
        )
    print("company_id", company_id)
    logging.info(f"Company ID для '{conf['company_name']}':'{company_id}'.")

    # 3. Атомарное создание пользователя для этой компании
    # Проверяем связку email + company_id
    merge_user_sql = f"""
        MERGE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{USERS_TABLE}` T
        USING (SELECT @email AS email, @company_id AS company_id) S
        ON T.email = S.email AND T.company_id = S.company_id
        WHEN NOT MATCHED THEN
          INSERT (`user`, user_id, email, tel_number, company_id)
          VALUES (@user_name, GENERATE_UUID(), @email, @tel_number, @company_id);
    """
    print("merge_user_sql", merge_user_sql)

    user_name = conf.get("user_name", "ИП Новая Компания")
    email = conf.get("email", "ИП Новая Компания")
    tel_number = conf.get("tel_number", "ИП Новая Компания")
    company_id = conf.get("company_id", "ИП Новая Компания")

    job_configuration = {
        "query": {
            "query": merge_user_sql,
            "useLegacySql": False,
            "queryParameters": [
                {
                    "name": "user_name",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": user_name},
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
    logging.info(f"Операция для пользователя с email '{email}' завершена.")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "api", "users", "refactored"],
    doc_md="""
    ### DAG для создания Компании и Пользователя (Атомарный, безопасный подход)

    Этот DAG использует `MERGE` для атомарного создания записей и `GENERATE_UUID()` для
    безопасной генерации ID. Все SQL-запросы параметризованы для защиты от инъекций.
    """,
) as dag:
    create_company_and_user_task = PythonOperator(
        task_id="create_company_and_user_atomic_task",
        python_callable=_create_company_and_user_atomi)