# api_cache.py - УЛУЧШЕННАЯ ВЕРСИЯ с умным кешированием

import time
import logging
import requests
from typing import Dict, Any, Optional, Callable, List
from functools import wraps
import random

logger = logging.getLogger(__name__)

# ГЛОБАЛЬНЫЙ КЕШ для всех API запросов (и старых, и новых функций)
_global_api_cache: Dict[str, Dict[str, Any]] = {}
_cache_timestamps: Dict[str, float] = {}
_cache_timeout = 120  # 2 минуты - достаточно для избежания дублирования

def timed_lru_cache(seconds: int = 60, maxsize: int = 128) -> Callable:
    """
    Декоратор для кэширования результатов функции с ограниченным временем жизни.
    """
    def decorator(func: Callable) -> Callable:
        cache = {}
        timestamps: Dict[str, float] = {}
        key_order = []
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            key = str(args) + str(kwargs)
            current_time = time.time()
            
            # СНАЧАЛА проверяем ГЛОБАЛЬНЫЙ кеш
            if key in _global_api_cache:
                cache_age = current_time - _cache_timestamps.get(key, 0)
                if cache_age < _cache_timeout:
                    logger.debug(f"Используем ГЛОБАЛЬНЫЙ кеш для {key[:50]}...")
                    return _global_api_cache[key]
                else:
                    # Очищаем устаревший глобальный кеш
                    _global_api_cache.pop(key, None)
                    _cache_timestamps.pop(key, None)
            
            # Очистка истекших ключей локального кеша
            for k in list(timestamps.keys()):
                if current_time - timestamps[k] > seconds:
                    cache.pop(k, None)
                    timestamps.pop(k, None)
                    if k in key_order:
                        key_order.remove(k)
            
            # Если ключ в локальном кеше и не истек, возвращаем кэшированное значение
            if key in cache:
                timestamps[key] = current_time
                key_order.remove(key)
                key_order.append(key)
                return cache[key]
            
            # Вызываем функцию и сохраняем результат в ОБА кеша
            result = func(*args, **kwargs)
            
            # Локальный кеш
            cache[key] = result
            timestamps[key] = current_time
            key_order.append(key)
            
            # Глобальный кеш
            _global_api_cache[key] = result
            _cache_timestamps[key] = current_time
            
            # Проверяем размер локального кеша
            if len(cache) > maxsize:
                oldest_key = key_order.pop(0)
                cache.pop(oldest_key, None)
                timestamps.pop(oldest_key, None)
            
            return result
        
        def clear_cache():
            cache.clear()
            timestamps.clear()
            key_order.clear()
            
        wrapper.clear_cache = clear_cache
        return wrapper
    
    return decorator


# ========== УМНЫЕ ФУНКЦИИ КЕШИРОВАНИЯ ==========

def get_from_global_cache(cache_key: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные из глобального кеша, если они актуальны.
    
    Args:
        cache_key: Ключ для поиска в кеше
        
    Returns:
        Данные из кеша или None
    """
    current_time = time.time()
    
    if cache_key in _global_api_cache:
        cache_age = current_time - _cache_timestamps.get(cache_key, 0)
        if cache_age < _cache_timeout:
            logger.debug(f"Найдены актуальные данные в глобальном кеше (возраст: {cache_age:.1f}с)")
            return _global_api_cache[cache_key]
        else:
            # Удаляем устаревший кеш
            _global_api_cache.pop(cache_key, None)
            _cache_timestamps.pop(cache_key, None)
            logger.debug(f"Удален устаревший кеш (возраст: {cache_age:.1f}с)")
    
    return None


def save_to_global_cache(cache_key: str, data: Dict[str, Any]) -> None:
    """
    Сохраняет данные в глобальный кеш.
    
    Args:
        cache_key: Ключ для сохранения
        data: Данные для сохранения
    """
    current_time = time.time()
    _global_api_cache[cache_key] = data
    _cache_timestamps[cache_key] = current_time
    logger.debug(f"Данные сохранены в глобальный кеш: {cache_key[:50]}...")


def clear_global_cache() -> None:
    """Очищает глобальный кеш"""
    global _global_api_cache, _cache_timestamps
    _global_api_cache.clear()
    _cache_timestamps.clear()
    logger.info("Глобальный API кеш очищен")


# ========== НОВЫЕ ФУНКЦИИ БАТЧИНГА (с умным кешированием) ==========

def fetch_tokens_batch(addresses: List[str], max_batch_size: int = 30) -> Dict[str, Dict[str, Any]]:
    """
    Получает данные для нескольких токенов одним запросом (батчинг) с умным кешированием.
    """
    if not addresses:
        return {}
    
    # Фильтруем адреса - проверяем что УЖЕ есть в кеше
    addresses_to_fetch = []
    cached_results = {}
    
    for address in addresses[:max_batch_size]:
        cache_key = f"token_batch_{address}"
        cached_data = get_from_global_cache(cache_key)
        
        if cached_data:
            cached_results[address] = cached_data
            logger.debug(f"Токен {address[:10]}... взят из кеша")
        else:
            addresses_to_fetch.append(address)
    
    logger.info(f"Из {len(addresses)} токенов: {len(cached_results)} из кеша, {len(addresses_to_fetch)} нужно запросить")
    
    # Если все токены уже в кеше, возвращаем их
    if not addresses_to_fetch:
        return cached_results
    
    # Формируем URL для батч запроса только для недостающих токенов
    addresses_str = ",".join(addresses_to_fetch)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{addresses_str}"
    
    max_retries = 3
    batch_results = {}
    
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                logger.info(f"Батч запрос для {len(addresses_to_fetch)} новых токенов")
            
            if attempt > 0:
                delay = random.uniform(2.0, 5.0) * attempt
                logger.info(f"Повторный батч запрос (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
                time.sleep(delay)
            
            response = requests.get(url, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and data.get('pairs'):
                    for pair in data['pairs']:
                        base_token = pair.get('baseToken', {})
                        token_address = base_token.get('address', '')
                        
                        if token_address in addresses_to_fetch:
                            batch_results[token_address] = data
                            # СОХРАНЯЕМ в глобальный кеш
                            cache_key = f"token_batch_{token_address}"
                            save_to_global_cache(cache_key, data)
                    
                    logger.info(f"Получены данные для {len(batch_results)} токенов из {len(addresses_to_fetch)}")
                    
                    # Для токенов без данных создаем пустые результаты
                    for address in addresses_to_fetch:
                        if address not in batch_results:
                            empty_data = {"pairs": []}
                            batch_results[address] = empty_data
                            # Кешируем и пустые результаты
                            cache_key = f"token_batch_{address}"
                            save_to_global_cache(cache_key, empty_data)
                    
                    # Объединяем кешированные и новые результаты
                    all_results = {**cached_results, **batch_results}
                    return all_results
                else:
                    logger.warning(f"API вернуло пустые данные для батча")
                    continue
                    
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit (429) для батча. Ожидание {retry_after} секунд...")
                time.sleep(retry_after)
                continue
            elif response.status_code in [500, 502, 503, 504]:
                logger.warning(f"Серверная ошибка {response.status_code} для батча")
                continue
            else:
                logger.warning(f"Ошибка {response.status_code} при батч запросе")
                continue
                
        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут при батч запросе (попытка {attempt + 1})")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при батч запросе: {str(e)}")
            continue
        except Exception as e:
            logger.error(f"Неожиданная ошибка при батч запросе: {str(e)}")
            continue
    
    logger.error(f"Все попытки батч запроса исчерпаны")
    
    # Возвращаем хотя бы кешированные результаты + пустые для неполученных
    for address in addresses_to_fetch:
        if address not in batch_results:
            batch_results[address] = {"pairs": []}
    
    all_results = {**cached_results, **batch_results}
    return all_results


# ========== МОДИФИЦИРОВАННЫЕ СТАРЫЕ ФУНКЦИИ (с глобальным кешем) ==========

@timed_lru_cache(seconds=30)
def get_token_info_from_api(query: str) -> Optional[Dict[str, Any]]:
    """
    МОДИФИЦИРОВАННАЯ функция - теперь проверяет глобальный кеш ПЕРЕД запросом.
    """
    # Проверяем глобальный кеш (возможно батч уже получил эти данные)
    cache_key = f"token_batch_{query}"
    cached_data = get_from_global_cache(cache_key)
    
    if cached_data:
        logger.info(f"Токен {query[:10]}... найден в глобальном кеше (избежали дублирования!)")
        return cached_data
    
    # Если нет в глобальном кеше, делаем обычный запрос
    try:
        from config import DEXSCREENER_API_URL
        
        if len(query) >= 32 and all(c.isalnum() or c in '0x' for c in query):
            primary_url = f"https://api.dexscreener.com/latest/dex/tokens/{query}"
        else:
            primary_url = f"{DEXSCREENER_API_URL}?q={query}"
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if attempt == 0:
                    logger.debug(f"Индивидуальный запрос к API для токена: {query}")
                
                if attempt > 0:
                    delay = random.uniform(2.0, 5.0) * attempt
                    logger.info(f"Повторный запрос API (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
                    time.sleep(delay)
                
                response = requests.get(primary_url, timeout=20)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and data.get('pairs'):
                        logger.info(f"Успешно получены данные для токена {query}")
                        # СОХРАНЯЕМ в глобальный кеш для будущих батчей
                        save_to_global_cache(cache_key, data)
                        return data
                    else:
                        logger.warning(f"API вернуло пустые данные для токена {query}")
                        continue
                        
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit (429) для токена {query}. Ожидание {retry_after} секунд...")
                    time.sleep(retry_after)
                    continue
                elif response.status_code in [500, 502, 503, 504]:
                    logger.warning(f"Серверная ошибка {response.status_code} для токена {query}")
                    continue
                else:
                    logger.warning(f"Ошибка API {response.status_code} для токена {query}")
                    continue
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут API для токена {query} (попытка {attempt + 1})")
                continue
            except Exception as e:
                logger.error(f"Ошибка API для токена {query}: {str(e)}")
                continue
        
        logger.error(f"Все попытки API исчерпаны для токена {query}")
        return None
        
    except Exception as e:
        logger.error(f"Критическая ошибка в get_token_info_from_api для токена {query}: {str(e)}")
        return None


@timed_lru_cache(seconds=30)
def fetch_dex_data(contract_address: str) -> Dict[str, Any]:
    """
    МОДИФИЦИРОВАННАЯ функция - теперь проверяет глобальный кеш ПЕРЕД запросом.
    """
    # Проверяем глобальный кеш
    cache_key = f"token_batch_{contract_address}"
    cached_data = get_from_global_cache(cache_key)
    
    if cached_data:
        logger.info(f"DEX данные для {contract_address[:10]}... найдены в глобальном кеше")
        return cached_data
    
    # Остальная логика как была...
    url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                logger.info(f"Индивидуальный запрос данных о DEX для контракта: {contract_address}")
            
            if attempt > 0:
                delay = random.uniform(1.0, 3.0) * attempt
                logger.info(f"Повторный запрос DEX (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
                time.sleep(delay)
            
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Успешно получены данные о DEX для контракта: {contract_address}")
                # СОХРАНЯЕМ в глобальный кеш
                save_to_global_cache(cache_key, data)
                return data
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit (429) для контракта {contract_address}. Ожидание {retry_after} секунд...")
                time.sleep(retry_after)
                continue
            elif response.status_code in [500, 502, 503, 504]:
                logger.warning(f"Серверная ошибка {response.status_code} для контракта {contract_address}")
                continue
            else:
                logger.warning(f"Ошибка {response.status_code} при запросе данных о DEX для контракта {contract_address}")
                continue
                
        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут при запросе данных для контракта {contract_address} (попытка {attempt + 1})")
            continue
        except Exception as e:
            logger.error(f"Ошибка при запросе данных для контракта {contract_address}: {str(e)}")
            continue
    
    logger.error(f"Все попытки исчерпаны для контракта {contract_address}")
    return {"pairs": []}


# ========== ФУНКЦИИ СТАТИСТИКИ ==========

def get_cache_stats() -> Dict[str, Any]:
    """Возвращает статистику использования кеша"""
    current_time = time.time()
    
    # Подсчитываем актуальные записи
    fresh_cache_count = 0
    expired_cache_count = 0
    
    for key, timestamp in _cache_timestamps.items():
        age = current_time - timestamp
        if age < _cache_timeout:
            fresh_cache_count += 1
        else:
            expired_cache_count += 1
    
    return {
        "global_cache_size": len(_global_api_cache),
        "fresh_entries": fresh_cache_count,
        "expired_entries": expired_cache_count,
        "cache_timeout": _cache_timeout,
        "cache_hit_ratio": f"{(fresh_cache_count / max(len(_global_api_cache), 1)) * 100:.1f}%"
    }


# ========== ОСТАЛЬНЫЕ ФУНКЦИИ без изменений ==========

def process_batch_requests(token_batches: List[List[str]]) -> Dict[str, Dict[str, Any]]:
    """Обрабатывает несколько батчей токенов с задержками между запросами."""
    all_results = {}
    
    for i, batch in enumerate(token_batches):
        if not batch:
            continue
            
        logger.info(f"Обработка батча {i+1}/{len(token_batches)} с {len(batch)} токенами")
        
        if i > 0:
            delay = random.uniform(1.0, 3.0)
            logger.debug(f"Задержка {delay:.2f}с между батчами")
            time.sleep(delay)
        
        batch_results = fetch_tokens_batch(batch)
        all_results.update(batch_results)
    
    logger.info(f"Обработано {len(token_batches)} батчей, получено данных для {len(all_results)} токенов")
    return all_results


def split_into_batches(addresses: List[str], batch_size: int = 30) -> List[List[str]]:
    """Разбивает список адресов на батчи заданного размера."""
    batches = []
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        batches.append(batch)
    
    logger.debug(f"Разбито {len(addresses)} адресов на {len(batches)} батчей по {batch_size}")
    return batches


def validate_token_addresses(addresses: List[str]) -> List[str]:
    """Валидирует и очищает список адресов токенов."""
    valid_addresses = []
    
    for addr in addresses:
        if not addr or not isinstance(addr, str):
            continue
            
        clean_addr = addr.strip()
        
        if len(clean_addr) >= 32 and clean_addr.replace('0x', '').isalnum():
            valid_addresses.append(clean_addr)
        else:
            logger.warning(f"Некорректный адрес токена: {addr}")
    
    logger.debug(f"Валидировано {len(valid_addresses)} из {len(addresses)} адресов")
    return valid_addresses