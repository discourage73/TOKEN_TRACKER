# Создайте файл http_client.py
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class HttpClient:
    """
    Клиент для выполнения HTTP-запросов с пулом соединений и механизмом повторных попыток.
    """
    
    def __init__(self, retries=3, backoff_factor=0.3, pool_connections=10, pool_maxsize=10):
        """
        Инициализирует HTTP-клиент.
        
        Args:
            retries: Количество повторных попыток
            backoff_factor: Фактор экспоненциального увеличения времени ожидания
            pool_connections: Количество соединений в пуле
            pool_maxsize: Максимальный размер пула
        """
        self.session = requests.Session()
        
        # Настраиваем стратегию повторных попыток
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        # Создаем адаптер с настроенной стратегией
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize
        )
        
        # Регистрируем адаптер для HTTP и HTTPS
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.info(f"HTTP-клиент инициализирован с {retries} повторными попытками")
    
    def get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> requests.Response:
        """
        Выполняет GET-запрос с повторными попытками.
        
        Args:
            url: URL для запроса
            params: Параметры запроса
            timeout: Таймаут в секундах
            
        Returns:
            Ответ от сервера
        """
        try:
            logger.debug(f"GET-запрос: {url}, params: {params}")
            response = self.session.get(url, params=params, timeout=timeout)
            logger.debug(f"Статус ответа: {response.status_code}")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при выполнении GET-запроса к {url}: {str(e)}")
            raise
    
    def post(self, url: str, data: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None, timeout: int = 10) -> requests.Response:
        """
        Выполняет POST-запрос с повторными попытками.
        
        Args:
            url: URL для запроса
            data: Данные формы
            json: JSON-данные
            timeout: Таймаут в секундах
            
        Returns:
            Ответ от сервера
        """
        try:
            logger.debug(f"POST-запрос: {url}, data: {data}, json: {json}")
            response = self.session.post(url, data=data, json=json, timeout=timeout)
            logger.debug(f"Статус ответа: {response.status_code}")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при выполнении POST-запроса к {url}: {str(e)}")
            raise
    
    def close(self):
        """Закрывает сессию и освобождает ресурсы."""
        self.session.close()
        logger.info("HTTP-клиент закрыт")

# Создаем глобальный экземпляр для использования во всем приложении
http_client = HttpClient()

# Обертка для получения JSON-данных из ответа
def get_json_response(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Выполняет GET-запрос и возвращает данные в формате JSON.
    
    Args:
        url: URL для запроса
        params: Параметры запроса
        
    Returns:
        JSON-данные или None в случае ошибки
    """
    try:
        response = http_client.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        logger.warning(f"API error: {response.status_code}, URL: {url}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении JSON-данных из {url}: {str(e)}")
        return None