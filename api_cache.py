# api_cache.py - ENHANCED VERSION with smart caching

import time
import logging
import requests
from typing import Dict, Any, Optional, Callable, List
from functools import wraps
import random

logger = logging.getLogger(__name__)

# GLOBAL CACHE for all API requests (old and new functions)
_global_api_cache: Dict[str, Dict[str, Any]] = {}
_cache_timestamps: Dict[str, float] = {}
_cache_timeout = 120  # 2 minutes - enough to avoid duplication

def timed_lru_cache(seconds: int = 60, maxsize: int = 128) -> Callable:
    """
    Decorator for caching function results with limited lifetime.
    """
    def decorator(func: Callable) -> Callable:
        cache = {}
        timestamps: Dict[str, float] = {}
        key_order = []
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            key = str(args) + str(kwargs)
            current_time = time.time()
            
            # FIRST check GLOBAL cache
            if key in _global_api_cache:
                cache_age = current_time - _cache_timestamps.get(key, 0)
                if cache_age < _cache_timeout:
                    logger.debug(f"Using GLOBAL cache for {key[:50]}...")
                    return _global_api_cache[key]
                else:
                    # Clear stale global cache
                    _global_api_cache.pop(key, None)
                    _cache_timestamps.pop(key, None)
            
            # Clear expired local cache keys
            for k in list(timestamps.keys()):
                if current_time - timestamps[k] > seconds:
                    cache.pop(k, None)
                    timestamps.pop(k, None)
                    if k in key_order:
                        key_order.remove(k)
            
            # If key is in local cache and not expired, return cached value
            if key in cache:
                timestamps[key] = current_time
                key_order.remove(key)
                key_order.append(key)
                return cache[key]
            
            # Call function and save result in BOTH caches
            result = func(*args, **kwargs)
            
            # Local cache
            cache[key] = result
            timestamps[key] = current_time
            key_order.append(key)
            
            # Global cache
            _global_api_cache[key] = result
            _cache_timestamps[key] = current_time
            
            # Check local cache size
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


# ========== SMART CACHING FUNCTIONS ==========

def get_from_global_cache(cache_key: str) -> Optional[Dict[str, Any]]:
    """
    Gets data from global cache if it's current.
    
    Args:
        cache_key: Key to search in cache
        
    Returns:
        Data from cache or None
    """
    current_time = time.time()
    
    if cache_key in _global_api_cache:
        cache_age = current_time - _cache_timestamps.get(cache_key, 0)
        if cache_age < _cache_timeout:
            logger.debug(f"Found current data in global cache (age: {cache_age:.1f}s)")
            return _global_api_cache[cache_key]
        else:
            # Remove stale cache
            _global_api_cache.pop(cache_key, None)
            _cache_timestamps.pop(cache_key, None)
            logger.debug(f"Removed stale cache (age: {cache_age:.1f}s)")
    
    return None


def save_to_global_cache(cache_key: str, data: Dict[str, Any]) -> None:
    """
    Saves data to global cache.
    
    Args:
        cache_key: Key for saving
        data: Data to save
    """
    current_time = time.time()
    _global_api_cache[cache_key] = data
    _cache_timestamps[cache_key] = current_time
    logger.debug(f"Data saved to global cache: {cache_key[:50]}...")




# ========== NEW BATCHING FUNCTIONS (with smart caching) ==========

def fetch_tokens_batch(addresses: List[str], max_batch_size: int = 30) -> Dict[str, Dict[str, Any]]:
    """
    Gets data for multiple tokens with one request (batching) with smart caching.
    """
    if not addresses:
        return {}
    
    # Filter addresses - check what's ALREADY in cache
    addresses_to_fetch = []
    cached_results = {}
    
    for address in addresses[:max_batch_size]:
        cache_key = f"token_batch_{address}"
        cached_data = get_from_global_cache(cache_key)
        
        if cached_data:
            cached_results[address] = cached_data
            logger.debug(f"Token {address[:10]}... taken from cache")
        else:
            addresses_to_fetch.append(address)
    
    logger.info(f"Of {len(addresses)} tokens: {len(cached_results)} from cache, {len(addresses_to_fetch)} need to request")
    
    # If all tokens are already in cache, return them
    if not addresses_to_fetch:
        return cached_results
    
    # Формируем URL для батч запроса только для недостающих tokens
    addresses_str = ",".join(addresses_to_fetch)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{addresses_str}"
    
    max_retries = 3
    batch_results = {}
    
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                logger.info(f"Батч request для {len(addresses_to_fetch)} новых tokens")
            
            if attempt > 0:
                delay = random.uniform(2.0, 5.0) * attempt
                logger.info(f"Повторный батч request (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
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
                            # Saving в глобальный кеш
                            cache_key = f"token_batch_{token_address}"
                            save_to_global_cache(cache_key, data)
                    
                    logger.info(f"Получены data для {len(batch_results)} tokens из {len(addresses_to_fetch)}")
                    
                    # Для tokens без данных Creating пустые results
                    for address in addresses_to_fetch:
                        if address not in batch_results:
                            empty_data = {"pairs": []}
                            batch_results[address] = empty_data
                            # Кешируем и пустые results
                            cache_key = f"token_batch_{address}"
                            save_to_global_cache(cache_key, empty_data)
                    
                    # Объединяем кешированные и новые results
                    all_results = {**cached_results, **batch_results}
                    return all_results
                else:
                    logger.warning(f"API вернуло пустые data для батча")
                    continue
                    
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit (429) для батча. Ожидание {retry_after} секунд...")
                time.sleep(retry_after)
                continue
            elif response.status_code in [500, 502, 503, 504]:
                logger.warning(f"Серверная Error {response.status_code} для батча")
                continue
            else:
                logger.warning(f"Error {response.status_code} при батч запросе")
                continue
                
        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут при батч запросе (попытка {attempt + 1})")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Error сети при батч запросе: {str(e)}")
            continue
        except Exception as e:
            logger.error(f"Неожиданная Error при батч запросе: {str(e)}")
            continue
    
    logger.error(f"Все попытки батч запроса исчерпаны")
    
    # Возвращаем хотя бы кешированные results + пустые для неполученных
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
    # Checking глобальный кеш (возможно батч уже получил эти data)
    cache_key = f"token_batch_{query}"
    cached_data = get_from_global_cache(cache_key)
    
    if cached_data:
        logger.info(f"token {query[:10]}... найден в глобальном кеше (избежали дублирования!)")
        return cached_data
    
    # Если нет в глобальном кеше, делаем обычный request
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
                    logger.debug(f"Индивидуальный request к API для token: {query}")
                
                if attempt > 0:
                    delay = random.uniform(2.0, 5.0) * attempt
                    logger.info(f"Повторный request API (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
                    time.sleep(delay)
                
                response = requests.get(primary_url, timeout=20)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and data.get('pairs'):
                        logger.info(f"Success получены data для token {query}")
                        # Saving в глобальный кеш для будущих батчей
                        save_to_global_cache(cache_key, data)
                        return data
                    else:
                        logger.warning(f"API вернуло пустые data для token {query}")
                        continue
                        
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit (429) для token {query}. Ожидание {retry_after} секунд...")
                    time.sleep(retry_after)
                    continue
                elif response.status_code in [500, 502, 503, 504]:
                    logger.warning(f"Серверная Error {response.status_code} для token {query}")
                    continue
                else:
                    logger.warning(f"API error {response.status_code} для token {query}")
                    continue
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут API для token {query} (попытка {attempt + 1})")
                continue
            except Exception as e:
                logger.error(f"API error для token {query}: {str(e)}")
                continue
        
        logger.error(f"Все попытки API исчерпаны для token {query}")
        return None
        
    except Exception as e:
        logger.error(f"Критическая Error in get_token_info_from_api для token {query}: {str(e)}")
        return None


@timed_lru_cache(seconds=30)
def fetch_dex_data(contract_address: str) -> Dict[str, Any]:
    """
    МОДИФИЦИРОВАННАЯ функция - теперь проверяет глобальный кеш ПЕРЕД запросом.
    """
    # Checking глобальный кеш
    cache_key = f"token_batch_{contract_address}"
    cached_data = get_from_global_cache(cache_key)
    
    if cached_data:
        logger.info(f"DEX data для {contract_address[:10]}... найдены в глобальном кеше")
        return cached_data
    
    # Остальная логика как была...
    url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                logger.info(f"Индивидуальный request данных о DEX для контракта: {contract_address}")
            
            if attempt > 0:
                delay = random.uniform(1.0, 3.0) * attempt
                logger.info(f"Повторный request DEX (попытка {attempt + 1}/{max_retries}) через {delay:.2f} сек...")
                time.sleep(delay)
            
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Success получены data о DEX для контракта: {contract_address}")
                # Saving в глобальный кеш
                save_to_global_cache(cache_key, data)
                return data
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit (429) для контракта {contract_address}. Ожидание {retry_after} секунд...")
                time.sleep(retry_after)
                continue
            elif response.status_code in [500, 502, 503, 504]:
                logger.warning(f"Серверная Error {response.status_code} для контракта {contract_address}")
                continue
            else:
                logger.warning(f"Error {response.status_code} при запросе данных о DEX для контракта {contract_address}")
                continue
                
        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут при запросе данных для контракта {contract_address} (попытка {attempt + 1})")
            continue
        except Exception as e:
            logger.error(f"Error при запросе данных для контракта {contract_address}: {str(e)}")
            continue
    
    logger.error(f"Все попытки исчерпаны для контракта {contract_address}")
    return {"pairs": []}




# ========== ОСТАЛЬНЫЕ ФУНКЦИИ без изменений ==========





