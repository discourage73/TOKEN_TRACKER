import logging
import time
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from functools import wraps

# Импортируем db_storage для всех операций с базой данных
from db_storage import (
    store_token_data as db_store_token_data,
    get_token_data as db_get_token_data,
    update_token_field as db_update_token_field,
    get_all_tokens as db_get_all_tokens,
    delete_token as db_delete_token,
    update_token_ath as db_update_token_ath
)

# Настройка логгирования
logger = logging.getLogger(__name__)

# Интервал для автоматической проверки токенов (в секундах)
AUTO_CHECK_INTERVAL = 60  # 1 минута

# Время последней автоматической проверки
last_auto_check_time = 0

# Период активности токенов (в секундах) - 7 дней
TOKEN_ACTIVE_PERIOD = 7 * 24 * 60 * 60  # 3 дня = 259200 секунд

# Словарь для хранения ID сообщений со списками токенов для каждого чата
list_message_ids = {}

# Функция для измерения времени выполнения (декоратор)
def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        if execution_time > 0.1:  # Логируем только если время выполнения > 100 мс
            logger.info(f"Функция {func.__name__} выполнена за {execution_time:.4f} секунд")
        return result
    return wrapper

def _is_token_active(token_data: Dict[str, Any]) -> bool:
    """
    Проверяет активен ли токен (младше 7 дней).
    
    Args:
        token_data: Данные токена
        
    Returns:
        True если токен активен (младше 7 дней), False если устарел
    """
    current_time = time.time()
    added_time = token_data.get('added_time', 0)
    
    if not added_time:
        return False
        
    return (current_time - added_time) <= TOKEN_ACTIVE_PERIOD

@measure_time
def store_token_data(query: str, data: Dict[str, Any]) -> None:
    """Сохраняет данные о токене в SQLite базу данных."""
    # Добавляем время добавления токена, если его нет
    if 'added_time' not in data:
        data['added_time'] = time.time()
    
    # Сохраняем данные напрямую в SQLite базу данных
    db_store_token_data(query, data)
    # Убираем дублирующий лог - уже логируется в db_storage.py

@measure_time
def get_token_data(query: str) -> Optional[Dict[str, Any]]:
    """Получает данные о токене из SQLite базы данных."""
    data = db_get_token_data(query)
    
    # Убираем дублирующие логи - уже логируется в db_storage.py
    return data

@measure_time
def update_token_field(query: str, field: str, value: Any) -> bool:
    """Обновляет значение поля в данных о токене в SQLite базе данных."""
    success = db_update_token_field(query, field, value)
    
    # Убираем дублирующие логи - уже логируется в db_storage.py
    return success

@measure_time
def remove_token_data(query: str) -> bool:
    """Удаляет данные о токене из SQLite базы данных."""
    success = db_delete_token(query)
    
    # Убираем дублирующие логи - уже логируется в db_storage.py
    return success

@measure_time
def get_all_tokens() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает словарь с АКТИВНЫМИ отслеживаемыми токенами (младше 7 дней) из SQLite базы данных.
    """
    # Получаем все токены из базы данных (теперь без параметра include_hidden)
    all_tokens = db_get_all_tokens()
    
    # Фильтруем только активные токены (младше 7 дней)
    active_tokens = {}
    for query, data in all_tokens.items():
        if _is_token_active(data):
            active_tokens[query] = data
    
    logger.info(f"Получено {len(active_tokens)} активных токенов из {len(all_tokens)} всего в базе")
    return active_tokens

@measure_time
def get_active_tokens() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает словарь с активными токенами (младше 7 дней).
    """
    # Теперь get_all_tokens уже возвращает только активные токены
    return get_all_tokens()

@measure_time
def get_all_tokens_for_analytics() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает ВСЕ токены из базы данных (включая старые) для аналитики.
    """
    return db_get_all_tokens()

@measure_time
def update_token_ath(query: str, current_mcap: float) -> bool:
    """Обновляет ATH (All-Time High) маркет капа токена, если текущее значение выше."""
    return db_update_token_ath(query, current_mcap)

def check_auto_update_needed() -> bool:
    """Проверяет, нужно ли выполнить автоматическую проверку токенов."""
    global last_auto_check_time
    current_time = time.time()
    
    if current_time - last_auto_check_time >= AUTO_CHECK_INTERVAL:
        last_auto_check_time = current_time
        return True
    
    return False

def update_last_auto_check_time() -> None:
    """Обновляет время последней автоматической проверки."""
    global last_auto_check_time
    last_auto_check_time = time.time()

@measure_time
def delete_token(query: str) -> bool:
    """Полностью удаляет токен из SQLite базы данных."""
    success = db_delete_token(query)
    
    # Убираем дублирующие логи - уже логируется в db_storage.py
    return success

@measure_time
def delete_all_tokens() -> int:
    """
    Полностью удаляет все токены из SQLite базы данных.
    Возвращает количество удаленных токенов.
    """
    # Импортируем функцию массового удаления
    try:
        from db_storage import delete_all_tokens as db_delete_all_tokens
        deleted_count = db_delete_all_tokens()
        logger.info(f"Все токены ({deleted_count} шт.) удалены из SQLite базы данных")
        return deleted_count
    except ImportError:
        # Fallback: удаляем по одному если функция не найдена
        all_tokens = db_get_all_tokens()
        token_count = len(all_tokens)
        
        if token_count == 0:
            return 0
        
        deleted_count = 0
        for query in all_tokens.keys():
            if db_delete_token(query):
                deleted_count += 1
        
        logger.info(f"Все токены ({deleted_count} из {token_count} шт.) удалены из SQLite базы данных (fallback метод)")
        return deleted_count

def store_list_message_id(chat_id: int, message_id: int) -> None:
    """Сохраняет ID сообщения со списком токенов для указанного чата."""
    list_message_ids[chat_id] = message_id
    logger.info(f"ID сообщения со списком токенов для чата {chat_id} сохранен: {message_id}")

def get_list_message_id(chat_id: int) -> Optional[int]:
    """Получает ID последнего сообщения со списком токенов для указанного чата."""
    message_id = list_message_ids.get(chat_id)
    return message_id