import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- Константы вашего проекта ---
GCP_CONN_ID = "google_cloud_default"
BIGQUERY_PROJECT = "shirman-group-app"
BIGQUERY_DATASET = "user_data"
COMPANIES_TABLE = "companies"
# ---------------------------------


@dag(
    dag_id="atomic_company_creation_fixed",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "fix"],
)
def company_creation_dag():
    """
    Этот DAG атомарно создает компанию в BigQuery, используя метод insert_job.
    """

    @task
    def create_company_in_bq(**kwargs):
        conf = kwargs.get("dag_run").conf if "dag_run" in kwargs else {}
        company_name = conf.get("company_name", "ИП НоваяКомпания")
        owner = conf.get("owner", "Новый Владелец")
        token = conf.get("token", "your_long_token_string")

        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

        merge_company_sql = f"""
            MERGE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{COMPANIES_TABLE}` T
            USING (SELECT @company_name AS company_name) S
            ON T.company_name = S.company_name
            WHEN NOT MATCHED THEN
              INSERT (company_id, company_name, owner, token)
              VALUES(GENERATE_UUID(), @company_name, @owner, @token);
        """

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

    create_company_in_bq()


company_creation_dag()
