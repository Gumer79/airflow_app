# -*- coding: UTF-8 -*-
"""
Wildberries API SDK для работы с API Поставщиков Wildberries.

Поддерживаемые методы:
- get_sales()   - Получение информации о продажах
- get_incomes() - Получение информации о поставках
- get_stocks()  - Получение информации об остатках на складах
- get_orders()  - Получение информации о заказах

Все методы имеют лимит: 1 запрос в минуту на аккаунт продавца.
"""

import logging
import requests
from datetime import datetime, date
from typing import Union, List, Dict, Any


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
        """
        Выполняет HTTP запрос к API Wildberries.
        
        Args:
            endpoint: Путь к API endpoint
            params: Параметры запроса
            
        Returns:
            Ответ API в виде списка словарей
            
        Raises:
            requests.exceptions.HTTPError: При HTTP ошибках
            requests.exceptions.RequestException: При других ошибках запроса
        """
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"Произошла HTTP-ошибка: {http_err} - {response.text}")
            raise
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Произошла ошибка запроса: {req_err}")
            raise

    def get_sales(
        self, date_from: Union[date, datetime, str], flag: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Получает информацию о продажах и возвратах.
        
        Данные обновляются раз в 30 минут.
        Информация о заказе хранится 90 дней с момента оформления.
        
        1 строка = 1 заказ = 1 сборочное задание = 1 единица товара.
        Для определения заказа рекомендуется использовать поле srid.
        
        Ограничение с flag=0: 80000 строк на ответ. Для получения всех продаж и возвратов
        может потребоваться несколько запросов с использованием lastChangeDate.
        
        Args:
            date_from: Дата и время последнего изменения по продаже/возврату (RFC3339)
                      Примеры: "2019-06-20", "2019-06-20T23:59:59", "2019-06-20T00:00:00.12345"
            flag: 0 - данные с lastChangeDate >= dateFrom
                  1 (по умолчанию) - все продажи за указанную дату (время в дате не имеет значения)
            
        Returns:
            Список продаж и возвратов
            
        Example:
            >>> api = WildberriesAPI(api_key="your_token")
            >>> # Получить продажи с определенной даты (инкрементально)
            >>> sales = api.get_sales(date_from="2025-01-01T00:00:00", flag=0)
            >>> # Получить все продажи за конкретную дату
            >>> sales = api.get_sales(date_from="2025-01-01", flag=1)
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/sales", params=params)

    def get_incomes(
        self, date_from: Union[date, datetime, str]
    ) -> List[Dict[str, Any]]:
        """
        Получает информацию о поставках товаров для хранения на складах WB.
        
        Данные обновляются раз в 30 минут.
        Ограничение: 100000 строк на ответ. Для получения всех поставок может потребоваться
        несколько запросов с использованием lastChangeDate из последней строки предыдущего ответа.
        
        Args:
            date_from: Дата и время последнего изменения по поставке (RFC3339)
                      Примеры: "2019-06-20", "2019-06-20T23:59:59", "2019-06-20T00:00:00.12345"
            
        Returns:
            Список поставок с полями: incomeId, number, date, lastChangeDate, supplierArticle,
                                     techSize, barcode, quantity, totalPrice, dateClose,
                                     warehouseName, nmId, status
            
        Example:
            >>> api = WildberriesAPI(api_key="your_token")
            >>> incomes = api.get_incomes(date_from="2025-01-01")
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str}
        return self._make_request("/api/v1/supplier/incomes", params=params)

    def get_stocks(
        self, date_from: Union[date, datetime, str]
    ) -> List[Dict[str, Any]]:
        """
        Получает информацию об остатках товаров на складах WB.
        
        Данные обновляются раз в 30 минут.
        Ограничение: 60000 строк на ответ. Для получения всех остатков может потребоваться
        несколько запросов с использованием lastChangeDate из последней строки предыдущего ответа.
        
        Для получения полного остатка следует указывать максимально раннее значение даты.
        
        Args:
            date_from: Дата и время последнего изменения по товару (RFC3339)
                      Для полного остатка: "2019-06-20"
                      Примеры: "2019-06-20", "2019-06-20T23:59:59", "2019-06-20T00:00:00.12345"
            
        Returns:
            Список остатков с полями: lastChangeDate, warehouseName, supplierArticle, nmId,
                                     barcode, quantity, inWayToClient, inWayFromClient,
                                     quantityFull, category, subject, brand, techSize,
                                     Price, Discount, isSupply, isRealization, SCCode
            
        Example:
            >>> api = WildberriesAPI(api_key="your_token")
            >>> stocks = api.get_stocks(date_from="2019-06-20")
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str}
        return self._make_request("/api/v1/supplier/stocks", params=params)

    def get_orders(
        self, date_from: Union[date, datetime, str], flag: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Получает информацию обо всех заказах.
        
        Данные обновляются раз в 30 минут.
        Информация о заказе хранится 90 дней с момента оформления.
        
        1 строка = 1 заказ = 1 сборочное задание = 1 единица товара.
        Для определения заказа рекомендуется использовать поле srid.
        
        Ограничение с flag=0: 80000 строк на ответ. Для получения всех заказов может
        потребоваться несколько запросов с использованием lastChangeDate.
        
        Args:
            date_from: Дата и время последнего изменения по заказу (RFC3339)
                      Примеры: "2019-06-20", "2019-06-20T23:59:59", "2019-06-20T00:00:00.12345"
            flag: 0 (по умолчанию) - данные с lastChangeDate >= dateFrom
                  1 - все заказы за указанную дату (время в дате не имеет значения)
            
        Returns:
            Список заказов
            
        Example:
            >>> api = WildberriesAPI(api_key="your_token")
            >>> # Получить заказы с определенной даты (инкрементально)
            >>> orders = api.get_orders(date_from="2025-01-01T00:00:00", flag=0)
            >>> # Получить все заказы за конкретную дату
            >>> orders = api.get_orders(date_from="2025-01-01", flag=1)
        """
        if isinstance(date_from, datetime):
            date_from_str = date_from.isoformat()
        elif isinstance(date_from, date):
            date_from_str = date_from.isoformat()
        else:
            date_from_str = str(date_from)

        params = {"dateFrom": date_from_str, "flag": flag}
        return self._make_request("/api/v1/supplier/orders", params=params)

