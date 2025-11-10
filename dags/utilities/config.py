#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
Централизованная конфигурация для всех DAGs проекта Wildberries.

Этот файл содержит все константы и параметры, используемые в DAGs.
"""

# =============================================================================
# GOOGLE CLOUD PLATFORM (GCP) НАСТРОЙКИ
# =============================================================================

# Connection ID для подключения к GCP в Airflow
GCP_CONN_ID = "google_cloud_default"

# ID проекта Google Cloud Platform
BIGQUERY_PROJECT = "shirman-group-app"

# Название Google Cloud Storage bucket
GCS_BUCKET = "app_s3"

# Регион для создания datasets в BigQuery
BIGQUERY_LOCATION = "europe-central2"


# =============================================================================
# BIGQUERY DATASETS И ТАБЛИЦЫ
# =============================================================================

# --- Dataset для пользовательских данных (компании и пользователи) ---
BIGQUERY_DATASET = "user_data"
COMPANIES_TABLE = "companies"
USERS_TABLE = "users"

# --- Dataset для данных Wildberries ---
BIGQUERY_WILDBERRIES_DATASET = "wildberries_raw"

# Таблицы для данных Wildberries
BIGQUERY_SALES_TABLE = "sales_raw"      # Продажи и возвраты
BIGQUERY_ORDERS_TABLE = "orders_raw"    # Заказы
BIGQUERY_INCOMES_TABLE = "incomes_raw"  # Поставки
BIGQUERY_STOCKS_TABLE = "stocks_raw"    # Остатки на складах


# =============================================================================
# GOOGLE CLOUD STORAGE (GCS) ПУТИ
# =============================================================================

# Префиксы путей в GCS для различных типов данных
GCS_PREFIX_SALES = "wildberries/sales"      # gs://app_s3/wildberries/sales/{date}/{company_id}.json
GCS_PREFIX_ORDERS = "wildberries/orders"    # gs://app_s3/wildberries/orders/{date}/{company_id}.json
GCS_PREFIX_INCOMES = "wildberries/incomes"  # gs://app_s3/wildberries/incomes/{date}/{company_id}.json
GCS_PREFIX_STOCKS = "wildberries/stocks"    # gs://app_s3/wildberries/stocks/{date}/{company_id}.json


# =============================================================================
# IAM И БЕЗОПАСНОСТЬ
# =============================================================================

# ID проекта для IAM-политик
IAM_PROJECT_ID = "shirman-group-app"

# Роль BigQuery, которая назначается пользователям
IAM_ROLE_TO_ADD = "roles/bigquery.user"


# =============================================================================
# НАСТРОЙКИ ЗАГРУЗКИ ДАННЫХ
# =============================================================================

# Размер батча для загрузки данных в BigQuery (количество записей)
BIGQUERY_BATCH_SIZE = 5000

# Количество дней исторических данных для загрузки при создании нового владельца
HISTORICAL_DAYS = 90


# =============================================================================
# РАСПИСАНИЕ DAG'ов (CRON)
# =============================================================================

# Расписание для автоматических загрузок данных Wildberries
SCHEDULE_ORDERS = "30 2 * * *"   # 02:30 UTC - Заказы
SCHEDULE_SALES = "0 3 * * *"     # 03:00 UTC - Продажи
SCHEDULE_INCOMES = "30 3 * * *"  # 03:30 UTC - Поставки
SCHEDULE_STOCKS = "0 4 * * *"    # 04:00 UTC - Остатки


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_gcs_path(data_type: str, date_str: str, company_id: str) -> str:
    """
    Формирует путь к файлу в GCS.
    
    Args:
        data_type: Тип данных (sales, orders, incomes, stocks)
        date_str: Дата в формате ISO (YYYY-MM-DD)
        company_id: ID компании
        
    Returns:
        Полный путь к файлу в GCS
        
    Example:
        >>> get_gcs_path("sales", "2025-11-06", "abc-123")
        'wildberries/sales/2025-11-06/abc-123.json'
    """
    prefix_map = {
        "sales": GCS_PREFIX_SALES,
        "orders": GCS_PREFIX_ORDERS,
        "incomes": GCS_PREFIX_INCOMES,
        "stocks": GCS_PREFIX_STOCKS,
    }
    prefix = prefix_map.get(data_type.lower())
    if not prefix:
        raise ValueError(f"Unknown data type: {data_type}")
    
    return f"{prefix}/{date_str}/{company_id}.json"


def get_bigquery_table_id(data_type: str, full: bool = True) -> str:
    """
    Формирует полный ID таблицы BigQuery.
    
    Args:
        data_type: Тип данных (sales, orders, incomes, stocks)
        full: Если True, возвращает полный ID с проектом и dataset
        
    Returns:
        ID таблицы BigQuery
        
    Example:
        >>> get_bigquery_table_id("sales", full=True)
        'shirman-group-app.wildberries_raw.sales_raw'
        >>> get_bigquery_table_id("sales", full=False)
        'sales_raw'
    """
    table_map = {
        "sales": BIGQUERY_SALES_TABLE,
        "orders": BIGQUERY_ORDERS_TABLE,
        "incomes": BIGQUERY_INCOMES_TABLE,
        "stocks": BIGQUERY_STOCKS_TABLE,
    }
    table = table_map.get(data_type.lower())
    if not table:
        raise ValueError(f"Unknown data type: {data_type}")
    
    if full:
        return f"{BIGQUERY_PROJECT}.{BIGQUERY_WILDBERRIES_DATASET}.{table}"
    return table
