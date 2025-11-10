#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
DAG для добавления нового владельца (компании и пользователя) с использованием политик IAM.

Использование:
    1. Откройте Airflow UI
    2. Найдите DAG 'add_new_owner_policy'
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
    - Атомарное создание компании и пользователя в BigQuery
    - Автоматическое добавление IAM-разрешений (roles/bigquery.user)
    - Создание записи в таблице users с привязкой к компании
    - Обработка дубликатов и связей пользователей

Отличие от add_new_owner_separate_tasks:
    - Все операции выполняются в одной задаче (атомарно)
    - Использует политики IAM для управления доступом
    - Более быстрое выполнение
"""

import os
import logging
from datetime import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from utilities.config import BIGQUERY_DATASET, BIGQUERY_PROJECT, GCP_CONN_ID

# Импорты для IAM
from google.cloud import resourcemanager_v3, bigquery

from google.iam.v1 import policy_pb2

COMPANIES_TABLE = "companies"
USERS_TABLE = "users"

# Константы для IAM
IAM_PROJECT_ID = "shirman-group-app"
# IAM_ROLE_TO_ADD = "roles/bigquery.dataViewer"
IAM_ROLE_TO_ADD = "roles/bigquery.user"

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def _create_company_and_user_atomic(**kwargs):
    """
    Атомарно создает компанию (если не существует) и пользователя (если не связан с компанией),
    а затем добавляет пользователя в IAM-политику проекта.
    """
    conf = kwargs["dag_run"].conf
    logging.info(f"Получены данные: {conf}")

    # 1. Проверка и извлечение входных данных
    required_keys = [
        "company_name",
        "owner",
        "token",
        # "user_name",
        "email",
        "tel_number",
    ]
    print("conf", conf)

    if not all(key in conf for key in required_keys):
        raise AirflowException(f"Отсутствуют необходимые ключи в conf: {required_keys}")

    # Извлекаем все переменные один раз
    company_name = conf["company_name"]
    owner = conf["owner"]
    token = conf["token"]
    # user_name = conf["user_name"]
    email = conf["email"]
    tel_number = conf["tel_number"]

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    # 2. Атомарное создание компании с помощью MERGE и получение её ID
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
    logging.info("Запрос MERGE для компании успешно выполнен.")

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
    logging.info(f"Company ID для '{company_name}':'{company_id}'.")

    # 3. Атомарное создание пользователя для этой компании
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
                    "parameterValue": {
                        "value": company_id
                    },  # Теперь используется корректный ID
                },
            ],
        }
    }

    logging.info("Выполнение MERGE для пользователя...")
    job = bq_hook.insert_job(configuration=job_configuration)
    job.result()  # Ждем завершения
    logging.info(f"Операция для пользователя с email '{email}' завершена.")

    # 4. *** НОВАЯ ЛОГИКА: Добавление пользователя в IAM Policy ***
    logging.info(
        f"Добавление пользователя {email} в IAM-политику проекта {IAM_PROJECT_ID}..."
    )

    try:
        # Получаем credentials из существующего hook'а
        credentials = bq_hook.get_credentials()
        iam_client = resourcemanager_v3.ProjectsClient(credentials=credentials)

        project_name = f"projects/{IAM_PROJECT_ID}"
        member_to_add = f"user:{email}"

        # Получаем текущую политику
        policy = iam_client.get_iam_policy(request={"resource": project_name})

        # Делаем операцию идемпотентной (проверяем, есть ли уже такая роль и участник)
        binding_found = False
        role_updated = False

        for binding in policy.bindings:
            if binding.role == IAM_ROLE_TO_ADD:
                binding_found = True
                if member_to_add not in binding.members:
                    binding.members.append(member_to_add)
                    role_updated = True
                    logging.info(
                        f"Пользователь {email} добавлен в существующую роль {IAM_ROLE_TO_ADD}."
                    )
                else:
                    logging.info(
                        f"Пользователь {email} уже имеет роль {IAM_ROLE_TO_ADD}."
                    )
                break

        # Если роль (binding) не найдена, создаем новую привязку
        if not binding_found:
            new_binding = policy_pb2.Binding(
                role=IAM_ROLE_TO_ADD, members=[member_to_add]
            )
            policy.bindings.append(new_binding)
            role_updated = True
            logging.info(f"Создана новая привязка роли {IAM_ROLE_TO_ADD} для {email}.")

        # Устанавливаем обновленную политику, только если были изменения
        if role_updated:
            updated_policy = iam_client.set_iam_policy(
                request={"resource": project_name, "policy": policy}
            )
            logging.info(f"IAM-политика успешно обновлена для пользователя {email}.")
        else:
            logging.info("Обновление IAM-политики не требуется.")

    except Exception as e:
        # Логируем ошибку, но не прерываем DAG
        # (Критичность: пользователь создан в БД, но не в IAM)
        logging.error(f"Не удалось обновить IAM-политику: {e}")
        # Если это критично, можно "пробросить" исключение:
        # raise AirflowException(f"Не удалось обновить IAM-политику: {e}")

    datasets = ["wildberries_raw", "user_data"]
    for dataset in datasets:
        try:
            bq_client = bigquery.Client(
                project=BIGQUERY_PROJECT, credentials=bq_hook.get_credentials()
            )
            dataset_ref = bigquery.DatasetReference(BIGQUERY_PROJECT, dataset)
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
                    f"Пользователь {email} добавлен в dataViewer на {dataset}."
                )
            else:
                logging.info(
                    f"Пользователь {email} уже имеет доступ dataViewer к {dataset}."
                )
        except Exception as e:
            logging.error(f"Не удалось обновить IAM-политику датасета {dataset}: {e}")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "api", "users", "iam", "refactored"],
    doc_md="""
    ### DAG для создания Компании и Пользователя (Атомарный, безопасный подход)

    Этот DAG использует `MERGE` для атомарного создания записей и `GENERATE_UUID()` для
    безопасной генерации ID. Все SQL-запросы параметризованы для защиты от инъекций.

    **Новая функция**: После создания пользователя в BigQuery, DAG также
    добавляет этого пользователя в IAM-политику проекта с ролью `roles/bigquery.dataViewer`.
    """,
) as dag:
    create_company_and_user_task = PythonOperator(
        task_id="create_company_and_user_atomic_task",
        python_callable=_create_company_and_user_atomic,
    )
