import os
import logging
import requests
import json
from typing import Union, List, Dict, Any
from datetime import datetime, timedelta, date

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


# --- Класс WildberriesAPI ---
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
        """
        Получает информацию о продажах за определенную дату.
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.date().isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/sales", params=params)


def get_users_from_bigquery(**kwargs) -> List[Dict[str, Any]]:
    """
    Извлекает user_id и token из таблицы BigQuery.
    """
    logging.info(
        f"Извлечение пользователей из {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    )

    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        sql = f"SELECT user_id, token FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"

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
    """
    Получает продажи для каждого пользователя и сохраняет их в GCS.
    """
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
        print("try users", users)

        for user in users:
            user_id = user.get("user_id")
            token = user.get("token")
            print("try token", token)
            print("try user_id", user_id)
            if not user_id or not token:
                logging.warning(
                    f"Пропуск пользователя из-за отсутствия user_id или token: {user}"
                )
                continue

            logging.info(f"Обработка пользователя: {user_id}")

            try:
                # 1. Получаем данные о продажах
                api = WildberriesAPI(api_key=token)
                sales_data = api.get_sales(date_from=date_to_fetch)

                if sales_data:
                    # 2. Сохраняем в GCS
                    file_path = f"sales/{user_id}/{execution_date}.json"
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
                else:
                    logging.info(
                        f"Нет данных о продажах для пользователя {user_id} за {date_to_fetch.isoformat()}."
                    )

            except ValueError as e:
                logging.error(f"Ошибка для пользователя {user_id}: {e}")
            except Exception as e:
                logging.error(
                    f"Неожиданная ошибка при обработке пользователя {user_id}: {e}",
                    exc_info=True,
                )

    except Exception as e:
        logging.error(
            f"Критическая ошибка в fetch_and_save_sales_to_gcs: {e}", exc_info=True
        )
        raise


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # Изменено на @daily для регулярного выполнения
    catchup=False,
    tags=["wildberries", "gcs", "bigquery"],
    doc_md="""
    ### DAG для выгрузки продаж Wildberries в GCS

    Этот DAG извлекает пользователей из таблицы BigQuery, получает данные
    о их продажах из API Wildberries за предыдущий день и сохраняет
    результат в виде JSON-файлов в Google Cloud Storage.
    """,
) as dag:
    task_get_users = PythonOperator(
        task_id="get_users_from_bigquery",
        python_callable=get_users_from_bigquery,
    )

    task_fetch_and_save = PythonOperator(
        task_id="fetch_and_save_sales_to_gcs",
        python_callable=fetch_and_save_sales_to_gcs,
    )

    task_get_users >> task_fetch_and_save
