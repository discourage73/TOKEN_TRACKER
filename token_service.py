import logging
import asyncio
import aiohttp
import time
import sqlite3
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from user_database import user_db
from utils import format_enhanced_message, process_token_data
from notifications import send_growth_notification_to_user
from token_monitor_strategy import token_monitor_strategy
from batch_market_cap import batch_get_market_caps

# Настройка логирования
service_logger = logging.getLogger('token_service')
service_logger.setLevel(logging.INFO)

# Добавляем консольный handler если его нет
if not service_logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    service_logger.addHandler(console_handler)

# Глобальное хранилище мониторинга токенов (в памяти)
_monitored_tokens: Dict[str, Dict[str, Any]] = {}
_monitoring_active = False
_telegram_context = None

# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С ТОКЕНАМИ
# ============================================================================

async def get_token_info(
    token_query: str,
    chat_id: int,
    message_id: Optional[int] = None,
    context = None,
    force_refresh: bool = False
) -> bool:
    """
    Получает информацию о токене и отправляет пользователю.
    
    Args:
        token_query: Адрес токена
        chat_id: ID чата
        message_id: ID сообщения для редактирования (не используется в новой версии)
        context: Контекст бота
        force_refresh: Принудительное обновление данных
        
    Returns:
        True если операция успешна, False иначе
    """
    try:
        service_logger.info(f"Запрос информации о токене: {token_query}")
        
        # Получаем данные о токене через DexScreener API
        token_data = await fetch_token_from_dexscreener(token_query)
        
        if not token_data:
            service_logger.warning(f"Не удалось получить данные для токена: {token_query}")
            return False
        
        # Обрабатываем и форматируем данные
        processed_data = process_token_data(token_data)
        service_logger.info(f"[OK] Данные обработаны: {processed_data.get('ticker', 'Unknown')}")
        
        # Форматируем сообщение
        try:
            message_text = format_enhanced_message(processed_data)
            service_logger.info(f"[OK] Сообщение отформатировано, длина: {len(message_text)} символов")
        except UnicodeEncodeError as e:
            service_logger.error(f"Ошибка кодировки при форматировании: {e}")
            # Fallback: простое сообщение без эмодзи
            ticker = processed_data.get('ticker', 'Unknown')
            address = processed_data.get('ticker_address', 'Unknown')
            market_cap = processed_data.get('market_cap', 'Unknown')
            message_text = f"*{ticker}*\n\nAddress: `{address}`\nMarket Cap: {market_cap}"
        
        # Отправляем сообщение пользователю
        telegram_context = get_telegram_context()
        
        if not telegram_context:
            service_logger.error("Не удалось получить контекст бота")
            return False
        
        sent_message = await telegram_context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
        
        # Сохраняем связь пользователь-токен-сообщение
        user_db.save_user_token_message(
            token_query=token_query,
            user_id=chat_id,
            message_id=sent_message.message_id
        )
        
        # Добавляем токен в мониторинг
        add_token_to_monitoring(token_query, processed_data)
        
        service_logger.info(f"✅ Информация о токене {token_query} отправлена пользователю {chat_id}")
        return True
        
    except Exception as e:
        service_logger.error(f"Ошибка при получении информации о токене: {e}")
        return False

async def fetch_token_from_dexscreener(token_address: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные о токене из DexScreener API.
    
    Args:
        token_address: Адрес токена
        
    Returns:
        Данные о токене или None при ошибке
    """
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        service_logger.info(f"[API] Запрос к DexScreener API: {url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                service_logger.info(f"[API] Статус ответа API: {response.status}")
                
                if response.status == 200:
                    try:
                        data = await response.json()
                        service_logger.info(f"[API] JSON данные получены, тип: {type(data)}")
                        
                        # Проверяем что data не None
                        if data is None:
                            service_logger.error(f"[ERROR] API вернул None для токена {token_address}")
                            return None
                        
                        # Проверяем что data является словарем
                        if not isinstance(data, dict):
                            service_logger.error(f"[ERROR] API вернул не словарь: {type(data)} для токена {token_address}")
                            service_logger.error(f"Raw data: {data}")
                            return None
                        
                        pairs = data.get('pairs', [])
                        service_logger.info(f"[API] Получены данные: {len(pairs) if pairs else 0} пар")
                        
                        # Берем первую пару, если есть
                        if pairs and len(pairs) > 0:
                            first_pair = pairs[0]
                            symbol = first_pair.get('baseToken', {}).get('symbol', 'Unknown') if isinstance(first_pair, dict) else 'Unknown'
                            service_logger.info(f"[OK] Найдена пара: {symbol}")
                            return first_pair
                        else:
                            service_logger.warning(f"[ERROR] Нет пар для токена {token_address}")
                            service_logger.warning(f"Raw API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}")
                            return None
                            
                    except Exception as json_error:
                        service_logger.error(f"[ERROR] Ошибка парсинга JSON: {json_error}")
                        response_text = await response.text()
                        service_logger.error(f"Raw response text: {response_text[:500]}")
                        return None
                else:
                    service_logger.warning(f"[ERROR] API DexScreener вернул статус {response.status}")
                    response_text = await response.text()
                    service_logger.warning(f"Response text: {response_text}")
                    return None
                    
    except Exception as e:
        service_logger.error(f"[ERROR] Ошибка при запросе к DexScreener API: {e}")
        import traceback
        service_logger.error(traceback.format_exc())
        return None

# ============================================================================
# СИСТЕМА МОНИТОРИНГА ТОКЕНОВ
# ============================================================================

def add_token_to_monitoring(token_query: str, token_data: Dict[str, Any]) -> None:
    """
    Добавляет токен в систему мониторинга.
    
    Args:
        token_query: Запрос токена
        token_data: Данные о токене
    """
    try:
        monitoring_data = {
            'token_info': token_data,
            'initial_data': token_data.copy(),
            'added_time': time.time(),
            'first_seen': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'last_check': time.time(),
            'curr_mcap': token_data.get('raw_market_cap', 0),
            'ath_market_cap': token_data.get('raw_market_cap', 0),
            'last_alert_multiplier': 1.0
        }
        
        _monitored_tokens[token_query] = monitoring_data
        
        # Добавляем в базу данных для постоянного хранения
        save_to_mcap_monitoring(token_query, monitoring_data)
        
        # Синхронно получаем и сохраняем данные токена в таблицу tokens
        save_token_info_sync(token_query)
        
        service_logger.info(f"Токен {token_query} добавлен в мониторинг")
        
    except Exception as e:
        service_logger.error(f"Ошибка при добавлении токена в мониторинг: {e}")

def save_to_mcap_monitoring(token_query: str, monitoring_data: Dict[str, Any]) -> None:
    """Сохраняет данные токена в таблицу mcap_monitoring."""
    try:
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        initial_mcap = monitoring_data.get('initial_data', {}).get('raw_market_cap', 0)
        curr_mcap = monitoring_data.get('curr_mcap', 0)
        
        cursor.execute('''
            INSERT OR REPLACE INTO mcap_monitoring 
            (contract, initial_mcap, curr_mcap, ath_mcap, ath_time, is_active)
            VALUES (?, ?, ?, ?, datetime('now', 'localtime'), 1)
        ''', (token_query, initial_mcap, curr_mcap, initial_mcap))
        
        conn.commit()
        conn.close()
        
        
    except Exception as e:
        service_logger.error(f"Ошибка при сохранении в mcap_monitoring: {e}")

def save_token_info_sync(token_query: str) -> None:
    """Синхронно получает данные токена через API и сохраняет в таблицу tokens (только если данных еще нет)."""
    try:
        # Проверяем, есть ли уже данные в базе
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        cursor.execute('SELECT token_info, raw_api_data FROM tokens WHERE contract = ?', (token_query,))
        existing_data = cursor.fetchone()
        
        if existing_data and existing_data[0] and existing_data[1]:
            service_logger.info(f"📊 Данные токена {token_query[:8]}... уже есть в базе, пропускаем API запрос")
            conn.close()
            return
        
        import requests
        
        service_logger.info(f"🔍 Получаем данные для токена {token_query[:8]}...")
        
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_query}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            api_data = response.json()
            pairs = api_data.get('pairs', [])
            
            if pairs:
                # Ищем лучшую пару по ликвидности
                best_pair = max(pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0) or 0)
                
                if best_pair and best_pair.get('baseToken', {}).get('symbol'):
                    # Создаем token_info
                    token_info_data = {
                        'ticker': best_pair['baseToken']['symbol'],
                        'name': best_pair['baseToken'].get('name', ''),
                        'ticker_address': token_query,
                        'pair_address': best_pair.get('pairAddress', ''),
                        'chain_id': 'solana',
                        'market_cap': best_pair.get('marketCap', ''),
                        'liquidity': best_pair.get('liquidity', {}).get('usd', 0)
                    }
                    
                    raw_api_data_json = json.dumps(api_data, ensure_ascii=False)
                    token_info_json = json.dumps(token_info_data, ensure_ascii=False)
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO tokens 
                        (contract, token_info, raw_api_data, first_seen) 
                        VALUES (?, ?, ?, datetime('now', 'localtime'))
                    ''', (token_query, token_info_json, raw_api_data_json))
                    
                    # If record already exists but data is empty, update it
                    if cursor.rowcount == 0:
                        cursor.execute('''
                            UPDATE tokens 
                            SET token_info = ?, raw_api_data = ? 
                            WHERE contract = ? AND (token_info IS NULL OR raw_api_data IS NULL)
                        ''', (token_info_json, raw_api_data_json, token_query))
                    
                    conn.commit()
                    service_logger.info(f"✅ Данные токена {token_query[:8]}... сохранены -> {best_pair['baseToken']['symbol']}")
                else:
                    service_logger.warning(f"⚠️ Не удалось найти данные baseToken для {token_query[:8]}...")
            else:
                service_logger.warning(f"⚠️ Нет пар для токена {token_query[:8]}...")
        else:
            service_logger.warning(f"⚠️ API вернул код {response.status_code} для токена {token_query[:8]}...")
            
        conn.close()
            
    except Exception as e:
        service_logger.error(f"❌ Ошибка при получении данных токена {token_query[:8]}...: {e}")

def load_active_tokens_from_db() -> Dict[str, Dict[str, Any]]:
    """Загружает активные токены из таблицы mcap_monitoring с JOIN к tokens для signal_reached_time."""
    try:
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT m.contract, m.initial_mcap, m.curr_mcap, m.ath_mcap, m.updated_time, m.created_time, 
                   COALESCE(t.signal_reached_time, m.created_time) as signal_reached_time
            FROM mcap_monitoring m
            LEFT JOIN tokens t ON m.contract = t.contract
            WHERE m.is_active = 1
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        active_tokens = {}
        for row in rows:
            contract, initial_mcap, curr_mcap, ath_mcap, updated_time, created_time, signal_reached_time = row
            active_tokens[contract] = {
                'initial_mcap': initial_mcap or 0,
                'curr_mcap': curr_mcap or 0,
                'ath_mcap': ath_mcap or 0,
                'updated_time': updated_time,
                'created_time': created_time,
                'signal_reached_time': signal_reached_time or created_time  # fallback на created_time
            }
        
        return active_tokens
        
    except Exception as e:
        service_logger.error(f"Ошибка при загрузке токенов из БД: {e}")
        return {}

def update_mcap_in_db(token_query: str, curr_mcap: float, ath_mcap: float = None) -> None:
    """Обновляет текущий mcap токена в базе данных."""
    try:
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        if ath_mcap is not None:
            # Обновляем curr_mcap, ath_mcap И ath_time
            cursor.execute('''
                UPDATE mcap_monitoring 
                SET curr_mcap = ?, ath_mcap = ?, ath_time = datetime('now', 'localtime'), updated_time = datetime('now', 'localtime')
                WHERE contract = ?
            ''', (curr_mcap, ath_mcap, token_query))
        else:
            # Обновляем только curr_mcap
            cursor.execute('''
                UPDATE mcap_monitoring 
                SET curr_mcap = ?, updated_time = datetime('now', 'localtime')
                WHERE contract = ?
            ''', (curr_mcap, token_query))
        
        conn.commit()
        rows_affected = cursor.rowcount
        conn.close()
        
        if rows_affected > 0:
            service_logger.debug(f"Updated mcap for {token_query[:8]}...: ${curr_mcap:,.0f}")
        
    except Exception as e:
        service_logger.error(f"Ошибка при обновлении mcap в БД: {e}")

def deactivate_token_in_db(token_query: str) -> None:
    """Деактивирует токен в базе данных (устанавливает is_active = 0)."""
    try:
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE mcap_monitoring 
            SET is_active = 0, updated_time = datetime('now', 'localtime')
            WHERE contract = ?
        ''', (token_query,))
        
        conn.commit()
        rows_affected = cursor.rowcount
        conn.close()
        
        if rows_affected > 0:
            service_logger.info(f"💀 Токен {token_query[:8]}... деактивирован в БД")
        else:
            service_logger.warning(f"⚠️ Токен {token_query[:8]}... не найден для деактивации")
        
    except Exception as e:
        service_logger.error(f"Ошибка при деактивации токена в БД: {e}")

def get_monitored_tokens() -> Dict[str, Dict[str, Any]]:
    """Возвращает все токены в мониторинге."""
    return _monitored_tokens.copy()

def get_token_stats(days: int = 1) -> Dict[str, Any]:
    """Возвращает статистику токенов за указанный период, объединяя данные из tokens и mcap_monitoring.
    
    Args:
        days: Количество дней для анализа (1=daily, 7=weekly, 30=monthly)
    """
    try:
        import sqlite3
        from datetime import datetime, timedelta
        
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        # Время N дней назад
        period_start = datetime.now() - timedelta(days=days)
        period_start_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
        
        # Получаем новые токены за указанный период из mcap_monitoring таблицы
        cursor.execute('''
            SELECT COUNT(*) FROM mcap_monitoring 
            WHERE created_time >= ?
        ''', (period_start_str,))
        new_tokens = cursor.fetchone()[0]
        
        # Получаем токены с ростом от 1.5x до 2x (используем ath_mcap/initial_mcap)
        cursor.execute('''
            SELECT COUNT(*) FROM mcap_monitoring 
            WHERE (ath_mcap / initial_mcap) >= 1.5 
            AND (ath_mcap / initial_mcap) < 2 
            AND created_time >= ?
            AND initial_mcap > 0
        ''', (period_start_str,))
        growing_tokens_15x_only = cursor.fetchone()[0]
        
        # Получаем токены с ростом >= 2x (используем ath_mcap/initial_mcap)
        cursor.execute('''
            SELECT COUNT(*) FROM mcap_monitoring 
            WHERE (ath_mcap / initial_mcap) >= 2 
            AND created_time >= ?
            AND initial_mcap > 0
        ''', (period_start_str,))
        growing_tokens_2x = cursor.fetchone()[0]
        
        # Общее количество токенов с ростом >= 1.5x (для hitrate)
        total_growing_tokens_15x = growing_tokens_15x_only + growing_tokens_2x
        
        # Вычисляем hitrate (процент успешных токенов с ростом ≥1.5x от общего количества)
        # Используем total_growing_tokens_15x который включает ВСЕ токены ≥1.5x
        hitrate_percent = (total_growing_tokens_15x / new_tokens * 100) if new_tokens > 0 else 0
        
        # Определяем цветовую индикацию для hitrate
        hitrate_symbol = "🔴"  # <30%
        if hitrate_percent >= 70:
            hitrate_symbol = "🟣"  # >=70%
        elif hitrate_percent >= 50:
            hitrate_symbol = "🟢"  # >=50%
        elif hitrate_percent >= 30:
            hitrate_symbol = "🟡"  # >=30%
        
        # Вычисляем RUG ratio - процент неактивных токенов от общего числа за указанный период
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_count,
                COUNT(*) as total_count
            FROM mcap_monitoring 
            WHERE created_time >= ?
        ''', (period_start_str,))
        result = cursor.fetchone()
        active_count, total_count = result[0], result[1]
        rug_ratio = int(((total_count - active_count) / total_count * 100)) if total_count > 0 else 0
        
        # Получаем топ токены с наибольшими множителями за указанный период, объединяя данные
        cursor.execute('''
            SELECT m.contract, (m.ath_mcap / m.initial_mcap) as real_multiplier, t.token_info, t.raw_api_data 
            FROM mcap_monitoring m
            LEFT JOIN tokens t ON m.contract = t.contract  
            WHERE (m.ath_mcap / m.initial_mcap) >= 2 
            AND m.created_time >= ?
            AND m.initial_mcap > 0
            ORDER BY real_multiplier DESC 
            LIMIT 5
        ''', (period_start_str,))
        
        top_tokens = []
        for row in cursor.fetchall():
            contract, multiplier, token_info, raw_api_data = row
            
            # Извлекаем ticker из JSON token_info или raw_api_data
            token_name = contract[:8] + '...'  # fallback
            
            # Сначала пробуем token_info
            if token_info:
                try:
                    info = json.loads(token_info)
                    if 'ticker' in info and info['ticker']:
                        token_name = info['ticker']
                    elif 'name' in info and info['name']:
                        token_name = info['name']
                except Exception as e:
                    service_logger.debug(f"Failed to parse token_info for {contract}: {e}")
            
            # Если не нашли, пробуем raw_api_data
            if token_name.endswith('...') and raw_api_data:
                try:
                    raw_data = json.loads(raw_api_data)
                    # Ищем в разных возможных местах
                    if 'baseToken' in raw_data and 'symbol' in raw_data['baseToken']:
                        token_name = raw_data['baseToken']['symbol']
                    elif 'name' in raw_data:
                        token_name = raw_data['name']
                    elif 'symbol' in raw_data:
                        token_name = raw_data['symbol']
                except Exception as e:
                    service_logger.debug(f"Failed to parse raw_api_data for {contract}: {e}")
            
            # Если все еще не нашли, пробуем быстрый API запрос (только для топ токенов)
            if token_name.endswith('...'):
                try:
                    import requests
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{contract}"
                    response = requests.get(url, timeout=3)
                    if response.status_code == 200:
                        api_data = response.json()
                        pairs = api_data.get('pairs', [])
                        if pairs:
                            # Ищем лучшую пару по ликвидности
                            best_pair = max(pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0) or 0)
                            if best_pair and best_pair.get('baseToken', {}).get('symbol'):
                                token_name = best_pair['baseToken']['symbol']
                                service_logger.info(f"Got token name from API: {contract[:8]}... -> {token_name}")
                                
                                # Сохраняем полученные данные в базу для следующих раз
                                try:
                                    # Создаем token_info из данных лучшей пары
                                    token_info_data = {
                                        'ticker': best_pair['baseToken']['symbol'],
                                        'name': best_pair['baseToken'].get('name', ''),
                                        'ticker_address': contract,
                                        'pair_address': best_pair.get('pairAddress', ''),
                                        'chain_id': 'solana',
                                        'market_cap': best_pair.get('marketCap', ''),
                                        'liquidity': best_pair.get('liquidity', {}).get('usd', 0)
                                    }
                                    
                                    # Обновляем token_info и raw_api_data в базе
                                    update_cursor = conn.cursor()
                                    update_cursor.execute('''
                                        UPDATE tokens 
                                        SET token_info = ?, raw_api_data = ? 
                                        WHERE contract = ?
                                    ''', (json.dumps(token_info_data), json.dumps(api_data), contract))
                                    conn.commit()
                                    
                                    service_logger.info(f"Saved token data to database: {contract[:8]}...")
                                except Exception as save_e:
                                    service_logger.error(f"Failed to save token data to database for {contract}: {save_e}")
                except Exception as e:
                    service_logger.debug(f"Failed to get token name from API for {contract}: {e}")
                
            top_tokens.append({
                'name': token_name,
                'multiplier': round(multiplier, 1) if multiplier else 1.0,
                'contract': contract
            })
        
        conn.close()
        
        return {
            'new_tokens': new_tokens,
            'growing_tokens_15x': growing_tokens_15x_only,
            'growing_tokens_2x': growing_tokens_2x,
            'hitrate_percent': hitrate_percent,
            'hitrate_symbol': hitrate_symbol,
            'rug_ratio': rug_ratio,
            'top_tokens': top_tokens
        }
        
    except Exception as e:
        service_logger.error(f"Ошибка при получении ежедневной статистики: {e}")
        return {
            'new_tokens': 0,
            'growing_tokens_15x': 0,
            'growing_tokens_2x': 0,
            'hitrate_percent': 0,
            'hitrate_symbol': '🔴',
            'rug_ratio': 0,
            'top_tokens': []
        }

# Функция get_monitoring_stats удалена - не использовалась нигде

# ============================================================================
# СИСТЕМА БАТЧИНГ МОНИТОРИНГА
# ============================================================================

async def check_tokens_batch_monitoring() -> None:
    """
    Проверяет токены батчами используя token_monitor_strategy для оптимизации.
    """
    try:
        # Загружаем активные токены из базы данных
        active_tokens = load_active_tokens_from_db()
        
        if not active_tokens:
            service_logger.debug("Нет активных токенов в mcap_monitoring")
            return
        
        # Создаем словарь для стратегии мониторинга
        tokens_for_strategy = {}
        for token_query, db_data in active_tokens.items():
            # Преобразуем данные из БД в формат для стратегии
            tokens_for_strategy[token_query] = {
                'signal_reached_time': db_data.get('signal_reached_time'),
                'created_time': db_data.get('created_time'),
                'token_info': {'raw_market_cap': db_data.get('curr_mcap', 0)},
                'initial_data': {'raw_market_cap': db_data.get('initial_mcap', 0)},
                'hidden': False
            }
        
        # Сначала категоризируем все токены для статистики
        for token_query, token_data in tokens_for_strategy.items():
            if token_query not in token_monitor_strategy.token_categories:
                category = token_monitor_strategy.categorize_token(token_data)
                token_monitor_strategy.token_categories[token_query] = category
        
        # Получаем статистику по категориям
        from token_monitor_strategy import TokenCategory
        categories_stats = token_monitor_strategy.get_all_tokens_by_category()
        hot_count = len(categories_stats.get(TokenCategory.HOT, []))
        active_count = len(categories_stats.get(TokenCategory.ACTIVE, []))
        stable_count = len(categories_stats.get(TokenCategory.STABLE, []))
        inactive_count = len(categories_stats.get(TokenCategory.INACTIVE, []))
        
        service_logger.info(f"📊 Категории токенов: 🔥HOT={hot_count} ⚡ACTIVE={active_count} ⚖️STABLE={stable_count} 😴INACTIVE={inactive_count}")
        
        # Используем стратегию мониторинга для определения токенов к проверке
        tokens_to_check = token_monitor_strategy.get_tokens_for_check(tokens_for_strategy)
        
        if not tokens_to_check:
            service_logger.info("😌 Нет токенов для проверки согласно стратегии мониторинга - все токены ожидают своих интервалов")
            return
        
        # Показываем какие конкретно токены проверяются
        tokens_preview = []
        for i, token in enumerate(tokens_to_check[:3]):  # Показываем первые 3
            category = token_monitor_strategy.get_token_category(token)
            category_emoji = {"HOT": "🔥", "ACTIVE": "⚡", "STABLE": "⚖️", "INACTIVE": "😴"}
            emoji = category_emoji.get(category.name, "❓")
            tokens_preview.append(f"{emoji}{token[:8]}...")
        
        if len(tokens_to_check) > 3:
            tokens_preview.append(f"и ещё {len(tokens_to_check) - 3}")
        
        service_logger.info(f"🎯 Стратегический батч: проверяем {len(tokens_to_check)} из {len(active_tokens)} токенов")
        service_logger.info(f"📋 Проверяемые токены: {', '.join(tokens_preview)}")
        
        # Получаем актуальные маркет-капы батчем с защитой от timeout
        try:
            market_caps = await asyncio.wait_for(
                batch_get_market_caps(tokens_to_check), 
                timeout=180.0  # 3 минуты максимум на весь батч (увеличено)
            )
        except asyncio.TimeoutError:
            service_logger.warning(f"⚠️ TIMEOUT при батчинге {len(tokens_to_check)} токенов за 180s. Пропускаем цикл")
            # Не возвращаемся сразу, а продолжаем с пустыми результатами
            market_caps = {}
        except Exception as batch_error:
            service_logger.error(f"❌ Ошибка в батчинге market caps: {batch_error}")
            import traceback
            service_logger.error(f"Traceback: {traceback.format_exc()}")
            # Продолжаем с пустыми результатами вместо return
            market_caps = {}
        
        # Обрабатываем результаты
        growth_notifications = []
        
        for token_query, current_mcap in market_caps.items():
            if current_mcap is None:
                continue
            
            
            # НОВАЯ ЛОГИКА: Деактивируем токен если mcap < 25k
            if current_mcap < 25000:
                service_logger.warning(f"💀 Токен {token_query[:8]}... деактивирован: mcap ${current_mcap:,.0f} < $25,000")
                deactivate_token_in_db(token_query)
                continue  # Пропускаем дальнейшую обработку этого токена
            
            # Обновляем время проверки в стратегии
            token_monitor_strategy.update_check_time(token_query)
            
            # Обновляем категорию токена на основе новых данных
            old_category = token_monitor_strategy.get_token_category(token_query)
            updated_token_data = {
                'signal_reached_time': tokens_for_strategy[token_query]['signal_reached_time'],
                'created_time': tokens_for_strategy[token_query]['created_time'],
                'token_info': {'raw_market_cap': current_mcap},
                'initial_data': {'raw_market_cap': token_data.get('initial_mcap', 0)},
                'hidden': False
            }
            token_monitor_strategy.update_token_category(token_query, updated_token_data)
            
            # Получаем данные токена из базы
            token_data = active_tokens.get(token_query)
            if not token_data:
                continue
            
            # Получаем текущий ATH из базы данных
            old_ath_mcap = token_data.get('ath_mcap', 0)
            current_ath = old_ath_mcap
            
            # Проверяем и обновляем ATH
            if current_mcap > old_ath_mcap:
                # Новый ATH! Обновляем в БД
                current_ath = current_mcap
                update_mcap_in_db(token_query, current_mcap, current_ath)
                service_logger.info(f"🚀 Новый ATH для {token_query[:8]}...: ${current_mcap:,.0f}")
            else:
                # Обновляем только текущий mcap в БД
                update_mcap_in_db(token_query, current_mcap)
            
            # ПРАВИЛЬНАЯ ЛОГИКА ATH: уведомляем только при новом ATH или росте от initial_mcap
            initial_mcap = token_data.get('initial_mcap', 0)
            
            if initial_mcap and initial_mcap > 0:
                # Рассчитываем множитель роста от initial call
                growth_multiplier = current_mcap / initial_mcap
                
                # Округляем до целого числа (например 4.35x -> 4x)
                current_multiplier_rounded = int(growth_multiplier)
                
                # Отправляем уведомление только если множитель >= 2x и изменился
                if current_multiplier_rounded >= 2:
                    # Проверяем, не отправляли ли уже уведомление для этого множителя
                    if not await was_notification_sent(token_query, current_multiplier_rounded):
                        # Получаем тикер (используем данные из батчинга, если есть)
                        token_ticker = token_query[:8] + '...'  # Fallback
                        
                        growth_result = {
                            'token_query': token_query,
                            'token_name': token_ticker,  # Будем получать тикер позже
                            'current_mcap': current_mcap,
                            'initial_mcap': initial_mcap,
                            'ath_mcap': current_ath,
                            'multiplier': current_multiplier_rounded,  # Реальный округленный множитель
                            'growth_multiplier': growth_multiplier,  # Точный множитель для отображения
                            'market_cap_formatted': f"${current_mcap:,.0f}" if current_mcap >= 1000 else f"${current_mcap:.2f}"
                        }
                        growth_notifications.append(growth_result)
        
        # Отправляем уведомления о росте
        growth_notifications_sent = 0
        if growth_notifications:
            await send_batch_growth_notifications(growth_notifications)
            growth_notifications_sent = len(growth_notifications)
            
        # Финальная статистика
        categories_stats_final = token_monitor_strategy.get_all_tokens_by_category()
        hot_final = len(categories_stats_final.get(TokenCategory.HOT, []))
        active_final = len(categories_stats_final.get(TokenCategory.ACTIVE, []))
        stable_final = len(categories_stats_final.get(TokenCategory.STABLE, []))
        inactive_final = len(categories_stats_final.get(TokenCategory.INACTIVE, []))
        
        # Подсчитываем успешно обработанные токены
        processed_tokens = len([t for t in tokens_to_check if market_caps.get(t) is not None])
        service_logger.info(f"✅ Завершен стратегический батч: {processed_tokens}/{len(tokens_to_check)} токенов обработаны, {growth_notifications_sent} уведомлений")
        
        # Показываем изменения в категориях если есть
        if (hot_final != hot_count or active_final != active_count or 
            stable_final != stable_count or inactive_final != inactive_count):
            service_logger.info(f"🔄 Категории после обновления: 🔥HOT={hot_final} ⚡ACTIVE={active_final} ⚖️STABLE={stable_final} 😴INACTIVE={inactive_final}")
        
    except Exception as e:
        service_logger.error(f"Ошибка в стратегическом батч мониторинге: {e}")
        import traceback
        service_logger.error(f"Traceback: {traceback.format_exc()}")

def check_token_growth(
    token_query: str, 
    token_data: Dict[str, Any], 
    current_mcap: float
) -> Optional[Dict[str, Any]]:
    """
    Проверяет рост токена и определяет нужность уведомления.
    
    Returns:
        Данные для уведомления или None
    """
    try:
        initial_mcap = token_data.get('initial_data', {}).get('raw_market_cap', 0)
        last_alert_multiplier = token_data.get('last_alert_multiplier', 1.0)
        
        if not initial_mcap or current_mcap <= 0:
            return None
        
        # Вычисляем множитель роста
        current_multiplier = current_mcap / initial_mcap
        
        # Обновляем ATH если нужно
        ath_mcap = token_data.get('ath_market_cap', initial_mcap)
        if current_mcap > ath_mcap:
            token_data['ath_market_cap'] = current_mcap
            ath_mcap = current_mcap
        
        # Проверяем, нужно ли уведомление о росте
        # Уведомляем при 2x, 3x, 5x, 10x, 20x и т.д.
        notification_multipliers = [2, 3, 5, 10, 20, 50, 100]
        
        for multiplier in notification_multipliers:
            if (current_multiplier >= multiplier and 
                last_alert_multiplier < multiplier):
                
                # Обновляем последний множитель уведомления
                token_data['last_alert_multiplier'] = multiplier
                
                return {
                    'token_query': token_query,
                    'token_name': token_data.get('token_info', {}).get('ticker', 'Unknown'),
                    'multiplier': multiplier,
                    'current_mcap': current_mcap,
                    'market_cap_formatted': f"${current_mcap:,.0f}" if current_mcap >= 1000 else f"${current_mcap:.2f}"
                }
        
        return None
        
    except Exception as e:
        service_logger.error(f"Ошибка при проверке роста токена {token_query}: {e}")
        return None

async def broadcast_token_to_all_users(token_query: str, token_data: Dict[str, Any]) -> None:
    """Отправляет информацию о новом токене всем активным пользователям."""
    try:
        service_logger.info(f"🚀 Начинаем рассылку токена {token_query} всем активным пользователям")
        
        # Получаем всех активных пользователей
        all_users = user_db.get_all_users()
        active_users = [user for user in all_users if user.get('is_active', False)]
        
        if not active_users:
            service_logger.warning("Нет активных пользователей для рассылки")
            return
        
        service_logger.info(f"Найдено {len(active_users)} активных пользователей")
        
        # Форматируем сообщение один раз
        message_text = format_enhanced_message(token_data)
        
        # Получаем контекст бота
        telegram_context = get_telegram_context()
        if not telegram_context:
            service_logger.error("Не удалось получить контекст бота для рассылки")
            return
        
        # Отправляем токен каждому активному пользователю
        successful_sends = 0
        for user in active_users:
            user_id = user['user_id']
            try:
                sent_message = await telegram_context.bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                
                # Сохраняем связь пользователь-токен-сообщение
                user_db.save_user_token_message(
                    token_query=token_query,
                    user_id=user_id,
                    message_id=sent_message.message_id
                )
                
                successful_sends += 1
                service_logger.info(f"✅ Токен отправлен пользователю {user_id}")
                
                # Небольшая пауза между отправками
                await asyncio.sleep(0.2)
                
            except Exception as send_error:
                service_logger.error(f"❌ Ошибка отправки токена пользователю {user_id}: {send_error}")
                continue
        
        service_logger.info(f"🎯 Рассылка завершена: {successful_sends}/{len(active_users)} пользователей получили токен {token_query}")
        
        # ВАЖНО: Добавляем токен в мониторинг mcap_monitoring
        if successful_sends > 0:
            try:
                add_token_to_monitoring(token_query, token_data)
                service_logger.info(f"📊 Токен {token_query} добавлен в mcap_monitoring")
            except Exception as monitoring_error:
                service_logger.error(f"❌ Ошибка добавления токена в мониторинг: {monitoring_error}")
        
    except Exception as e:
        service_logger.error(f"Критическая ошибка при рассылке токена: {e}")

async def send_batch_growth_notifications(notifications: List[Dict[str, Any]]) -> None:
    """Отправляет уведомления о росте пользователям."""
    try:
        for notification in notifications:
            token_query = notification['token_query']
            threshold = notification['multiplier']
            
            # Проверяем, не отправляли ли уже уведомление для этого порога
            if await was_notification_sent(token_query, threshold):
                service_logger.debug(f"Уведомление {threshold}x для {token_query[:8]}... уже было отправлено")
                continue
            
            # Получаем тикер токена для красивого отображения
            try:
                token_api_data = await fetch_token_from_dexscreener(token_query)
                if token_api_data:
                    from utils import process_token_data
                    processed_data = process_token_data(token_api_data)
                    token_ticker = processed_data.get('ticker', token_query[:8] + '...')
                else:
                    token_ticker = token_query[:8] + '...'
            except:
                token_ticker = token_query[:8] + '...'
            
            # Обновляем имя токена
            notification['token_name'] = token_ticker
            
            # Получаем всех пользователей для этого токена
            users_for_token = user_db.get_all_users_for_token(token_query)
            
            if users_for_token:
                service_logger.info(f"📈 Отправляем уведомление {token_ticker} {threshold}x {len(users_for_token)} пользователям")
                
                for user_info in users_for_token:
                    user_id = user_info['user_id']
                    token_message_id = user_info.get('token_message_id')
                    
                    # Отправляем уведомление конкретному пользователю
                    await send_growth_notification_to_user(
                        user_id=user_id,
                        token_name=token_ticker,
                        multiplier=threshold,
                        market_cap=notification['market_cap_formatted'],
                        token_message_id=token_message_id,
                        contract_address=token_query
                    )
                    
                    # Небольшая пауза между уведомлениями
                    await asyncio.sleep(0.1)
                
                # Помечаем что уведомление отправлено
                await mark_notification_sent(token_query, threshold)
                
    except Exception as e:
        service_logger.error(f"Ошибка при отправке batch уведомлений: {e}")

async def was_notification_sent(token_query: str, multiplier: int) -> bool:
    """Проверяет, было ли уже отправлено уведомление для данного множителя."""
    try:
        import sqlite3
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_alert_multiplier FROM mcap_monitoring 
            WHERE contract = ?
        ''', (token_query,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            last_multiplier = result[0]
            return multiplier <= last_multiplier
        
        return False
        
    except Exception as e:
        service_logger.error(f"Ошибка проверки уведомления: {e}")
        return False

async def mark_notification_sent(token_query: str, multiplier: int) -> None:
    """Помечает что уведомление для данного множителя отправлено."""
    try:
        import sqlite3
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE mcap_monitoring 
            SET last_alert_multiplier = ?
            WHERE contract = ?
        ''', (multiplier, token_query))
        
        conn.commit()
        conn.close()
        
        service_logger.debug(f"Помечено уведомление {multiplier}x для {token_query[:8]}...")
        
    except Exception as e:
        service_logger.error(f"Ошибка пометки уведомления: {e}")


# ============================================================================
# СИСТЕМА МОНИТОРИНГА ЗАПУСК/ОСТАНОВКА
# ============================================================================

async def start_monitoring_system(application) -> None:
    """Запускает систему мониторинга токенов."""
    global _monitoring_active
    
    try:
        service_logger.info("🚀 Запуск системы мониторинга токенов")
        
        # Загружаем токены из базы данных
        active_tokens = load_active_tokens_from_db()
        service_logger.info(f"📦 Загружено {len(active_tokens)} активных токенов из базы данных")
        
        _monitoring_active = True
        
        # Запускаем основной цикл мониторинга
        asyncio.create_task(monitoring_loop())
        
        service_logger.info("✅ Система мониторинга токенов запущена")
        
    except Exception as e:
        service_logger.error(f"Ошибка при запуске системы мониторинга: {e}")
        raise

# Функция load_tokens_from_database удалена - была дубликатом load_active_tokens_from_db

async def monitoring_loop() -> None:
    """Основной цикл стратегического мониторинга."""
    consecutive_errors = 0
    max_errors = 5
    
    service_logger.info("🎯 Запуск стратегического цикла мониторинга")
    
    while _monitoring_active:
        try:
            service_logger.debug("🔄 Начинаем цикл стратегического мониторинга")
            
            # Проверяем токены батчем используя стратегию
            await check_tokens_batch_monitoring()
            
            # Сбрасываем счетчик ошибок при успешном выполнении
            consecutive_errors = 0
            
            # Пауза между циклами - меньше чем раньше, так как стратегия сама определяет частоту
            await asyncio.sleep(20)  # 20 секунд между проверками стратегии
            
        except asyncio.CancelledError:
            service_logger.info("🛑 Цикл стратегического мониторинга отменен")
            break
            
        except Exception as e:
            consecutive_errors += 1
            service_logger.error(f"❌ Ошибка в стратегическом цикле #{consecutive_errors}: {e}")
            
            # Импортируем traceback для детальной ошибки
            import traceback
            service_logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Если слишком много ошибок подряд, увеличиваем паузу
            if consecutive_errors >= max_errors:
                service_logger.critical(f"🚨 КРИТИЧНО: {consecutive_errors} ошибок подряд в стратегическом мониторинге! Увеличиваем паузу")
                await asyncio.sleep(300)  # 5 минут при критических ошибках
                consecutive_errors = 0  # Сбрасываем счетчик
            else:
                await asyncio.sleep(60)  # 1 минута при обычных ошибках
    
    service_logger.warning(f"⚠️ Стратегический цикл мониторинга завершен. _monitoring_active = {_monitoring_active}")

def stop_monitoring_system() -> None:
    """Останавливает систему мониторинга."""
    global _monitoring_active
    _monitoring_active = False
    service_logger.info("🛑 Система мониторинга токенов остановлена")

async def restart_monitoring_system() -> None:
    """Перезапускает систему мониторинга."""
    global _monitoring_active
    
    service_logger.warning("🔄 ПЕРЕЗАПУСК системы мониторинга")
    
    # Останавливаем старую систему
    stop_monitoring_system()
    await asyncio.sleep(2)
    
    # Запускаем новую
    _monitoring_active = True
    asyncio.create_task(monitoring_loop())
    service_logger.info("✅ Система мониторинга ПЕРЕЗАПУЩЕНА")

def is_monitoring_active() -> bool:
    """Проверяет, активна ли система мониторинга."""
    return _monitoring_active

# ============================================================================
# СТАТИСТИКА И ОТЧЕТЫ
# ============================================================================

async def send_token_stats(context, days: int = 1) -> None:
    """Отправляет статистику по токенам администраторам.
    
    Args:
        context: Контекст Telegram бота
        days: Количество дней для анализа (1=daily, 7=weekly, 30=monthly)
    """
    try:
        from config import CONTROL_ADMIN_IDS
        
        # Получаем статистику за указанный период
        stats = get_token_stats(days)
        
        # Определяем заголовок в зависимости от периода
        if days == 1:
            title = "Daily Token Statistics"
            period_text = "(24h)"
        elif days == 7:
            title = "Weekly Token Statistics" 
            period_text = "(7d)"
        else:
            title = "Monthly Token Statistics"
            period_text = f"({days}d)"
        
        # Формируем сообщение
        stats_text = (
            f"📊 {title}\n"
            f"Hitrate: {stats['hitrate_percent']:.1f}% {stats['hitrate_symbol']} (1.5x+)\n\n"
            f"> Total tokens: {stats['new_tokens']}\n"
            f"├ 1.5x-2x: {stats['growing_tokens_15x']}\n"
            f"├ ≥2x: {stats['growing_tokens_2x']}\n"
            f"└ RUG ratio: {stats['rug_ratio']}%\n\n"
            f"*🏆Top tokens {period_text}:*\n"
        )
        
        # Добавляем топ токены
        for i, token in enumerate(stats['top_tokens'][:5], 1):
            stats_text += f"{i}. {token['name']} - {token['multiplier']}x\n"
        
        stats_text += f"\n_Statistics on {datetime.now().strftime('%d.%m.%Y %H:%M')}_"
        
        # Отправляем статистику всем админам
        for admin_id in CONTROL_ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=stats_text,
                    parse_mode='Markdown'
                )
                service_logger.info(f"Ежедневная статистика отправлена админу {admin_id}")
            except Exception as e:
                service_logger.error(f"Ошибка отправки статистики админу {admin_id}: {e}")
                
    except Exception as e:
        service_logger.error(f"Ошибка при отправке статистики: {e}")

async def send_daily_token_stats(context) -> None:
    """Обратная совместимость - отправляет дневную статистику."""
    await send_token_stats(context, days=1)

# ============================================================================
# ФУНКЦИИ ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ
# ============================================================================

# Для совместимости с существующим кодом
def get_daily_token_stats() -> Dict[str, Any]:
    """Обратная совместимость - возвращает дневную статистику."""
    return get_token_stats(days=1)

def get_telegram_context():
    """Возвращает контекст бота (для совместимости)."""
    return _telegram_context

def set_telegram_context(context):
    """Устанавливает контекст бота для использования в token_service."""
    global _telegram_context
    _telegram_context = context

async def fetch_and_save_token_info(token_query: str) -> None:
    """Получает данные токена через API и сохраняет в таблицу tokens."""
    try:
        import requests
        
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_query}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            api_data = response.json()
            pairs = api_data.get('pairs', [])
            
            if pairs:
                # Ищем лучшую пару по ликвидности
                best_pair = max(pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0) or 0)
                
                if best_pair and best_pair.get('baseToken', {}).get('symbol'):
                    # Создаем token_info
                    token_info_data = {
                        'ticker': best_pair['baseToken']['symbol'],
                        'name': best_pair['baseToken'].get('name', ''),
                        'ticker_address': token_query,
                        'pair_address': best_pair.get('pairAddress', ''),
                        'chain_id': 'solana',
                        'market_cap': best_pair.get('marketCap', ''),
                        'liquidity': best_pair.get('liquidity', {}).get('usd', 0)
                    }
                    
                    # Сохраняем в базу данных
                    conn = sqlite3.connect("tokens_tracker_database.db")
                    cursor = conn.cursor()
                    
                    raw_api_data_json = json.dumps(api_data, ensure_ascii=False)
                    token_info_json = json.dumps(token_info_data, ensure_ascii=False)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO tokens 
                        (contract, token_info, raw_api_data, first_seen) 
                        VALUES (?, ?, ?, datetime('now', 'localtime'))
                    ''', (token_query, token_info_json, raw_api_data_json))
                    
                    conn.commit()
                    service_logger.info(f"📊 Данные токена {token_query[:8]}... сохранены -> {best_pair['baseToken']['symbol']}")
                    
                    conn.close()
                else:
                    service_logger.warning(f"⚠️ Не удалось найти данные baseToken для {token_query[:8]}...")
            else:
                service_logger.warning(f"⚠️ Нет торговых пар для токена {token_query[:8]}...")
        else:
            service_logger.warning(f"⚠️ API ошибка {response.status_code} для токена {token_query[:8]}...")
            
    except Exception as e:
        service_logger.error(f"❌ Ошибка получения данных токена {token_query[:8]}...: {e}")