import os
import logging
import time
import asyncio
import random
import datetime
from typing import Dict, Any, Optional, Union, List, Tuple
import functools

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from telegram.error import TimedOut, NetworkError

# Импортируем модули проекта
import token_storage
from utils import process_token_data, format_enhanced_message, format_number
from error_helpers import handle_exception
from api_cache import get_token_info_from_api
from http_client import http_client
from notifications import add_growth_notification
from token_monitor_strategy import token_monitor_strategy

# Настройка логгера
logger = logging.getLogger(__name__)

# Глобальный контекст Telegram
_telegram_context = None

def set_telegram_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Устанавливает глобальный контекст Telegram.
    
    Args:
        context: Контекст Telegram
    """
    global _telegram_context
    _telegram_context = context

def get_telegram_context() -> Optional[ContextTypes.DEFAULT_TYPE]:
    """
    Возвращает глобальный контекст Telegram.
    
    Returns:
        Контекст Telegram или None
    """
    return _telegram_context

@handle_exception(log_msg="Ошибка при получении информации о токене", notify_user=True)
async def get_token_info(
    query: str, 
    chat_id: Optional[int], 
    message_id: Optional[int] = None, 
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
    force_refresh: bool = False  # Добавляем параметр force_refresh
) -> Optional[Dict[str, Any]]:
    """Получает информацию о токене и отправляет или обновляет сообщение."""
    logger.info(f"Запрос информации о токене: {query}, force_refresh: {force_refresh}")
    
    # Сначала проверяем, есть ли уже данные о токене в хранилище
    stored_data = token_storage.get_token_data(query)
    
    token_data = None
    token_info = None
    need_fresh_data = force_refresh  # Если force_refresh=True, всегда запрашиваем свежие данные
    
    # Если данные уже есть в хранилище и не требуется принудительное обновление
    if stored_data and 'token_info' in stored_data and not force_refresh:
        last_update_time = stored_data.get('last_update_time', 0)
        current_time = time.time()
        
        # Если данные обновлялись менее 1 минуты назад, используем их
        if current_time - last_update_time < 60:  # 60 секунд = 1 минута
            logger.info(f"Используем кэшированные данные о токене {query}")
            token_info = stored_data['token_info']
            # Также получаем raw_api_data, если она есть
            token_data = stored_data.get('raw_api_data')
        else:
            logger.info(f"Данные о токене {query} устарели, запрашиваем свежие")
            need_fresh_data = True
    else:
        if force_refresh:
            logger.info(f"Запрошено принудительное обновление данных о токене {query}")
        else:
            logger.info(f"Данные о токене {query} не найдены в хранилище, запрашиваем с API")
        need_fresh_data = True
    
    # Если нужны свежие данные, запрашиваем их из API
    if need_fresh_data:
        api_data = await fetch_token_api_data(query)
        if not api_data:
            return await handle_api_error(query, chat_id, message_id, context)
        
        # Обрабатываем данные из API
        token_data, token_info = process_api_data(api_data)
        if not token_info:
            return await handle_api_error(query, chat_id, message_id, context)
    
    # Получаем и обрабатываем начальные данные
    initial_data = get_initial_data(query, token_info)
    
    # Формируем сообщение
    message = format_enhanced_message(token_info, initial_data)
    
    # Больше не создаём кнопку Refresh в одиночном окне
    reply_markup = None
    
    # Отправляем или обновляем сообщение, только если есть контекст и chat_id
    if context and chat_id:
        await send_or_update_message(
            query, chat_id, message_id, context, message, reply_markup, token_data, token_info, initial_data
        )
    return token_info

@handle_exception(log_msg="Ошибка при получении данных API")
async def fetch_token_api_data(query: str) -> Optional[Dict[str, Any]]:
    """Получает данные о токене из API с использованием кэширования."""
    from config import DEXSCREENER_API_URL
    
    # Используем кэшированную функцию для получения данных
    api_data = get_token_info_from_api(query)
    
    if api_data:
        logger.info(f"Получены данные из API для {query}")
        return api_data
    
    logger.warning(f"Не удалось получить данные из API для {query}")
    return None

@handle_exception(log_msg="Ошибка при обработке данных API")
def process_api_data(api_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """Обрабатывает данные, полученные от API."""
    pairs = api_data.get('pairs', [])
    
    if not pairs:
        return {}, None
    
    # Берем первый результат как наиболее релевантный
    token_data = pairs[0]
    raw_api_data = token_data
    
    # Обрабатываем данные с помощью существующей функции
    token_info = process_token_data(token_data)
    
    # Добавляем дополнительные поля, которые могут отсутствовать в token_info
    token_info['price_usd'] = token_data.get('priceUsd')  # Добавляем текущий курс
    
    if 'txns' in token_data:
        token_info['txns'] = token_data.get('txns', {})
        
    if 'labels' in token_data:
        token_info['labels'] = token_data.get('labels', [])
        
    if 'audit' in token_data:
        token_info['audit'] = token_data.get('audit', {})
        
    if 'pyr' in token_data:
        token_info['pyr'] = token_data.get('pyr')
    
    return raw_api_data, token_info

@handle_exception(log_msg="Ошибка при обработке ошибки API")
async def handle_api_error(
    query: str, 
    chat_id: Optional[int], 
    message_id: Optional[int], 
    context: Optional[ContextTypes.DEFAULT_TYPE]
) -> Optional[Dict[str, Any]]:
    """Обрабатывает ошибки API и возвращает хранящиеся данные, если они есть."""
    stored_data = token_storage.get_token_data(query)
    
    if stored_data and 'token_info' in stored_data:
        logger.info(f"Используем сохраненные данные для токена {query}")
        
        # Если есть context и chat_id, отправляем сообщение
        if context and chat_id:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Не удалось получить свежие данные для токена '{query}'. Показываю сохраненные данные."
            )
        
        return stored_data.get('token_info')
    
    if context and chat_id:
        if message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"Не удалось найти информацию о токене '{query}'."
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Не удалось найти информацию о токене '{query}'."
            )
    
    return None

@handle_exception(log_msg="Ошибка при получении начальных данных")
def get_initial_data(query: str, token_info: Dict[str, Any]) -> Dict[str, Any]:
    """Получает начальные данные о токене."""
    stored_data = token_storage.get_token_data(query)
    
    if stored_data and 'initial_data' in stored_data:
        return stored_data.get('initial_data')
    
    # Если нет сохраненных данных, создаем начальные
    current_time = datetime.datetime.now().strftime("%H:%M:%S")
    initial_data = {
        'time': current_time,
        'market_cap': token_info['market_cap'],
        'raw_market_cap': token_info['raw_market_cap'],
        'price_usd': token_info.get('price_usd', 0)
    }
    
    logger.info(f"Новый токен {query} добавлен с начальным Market Cap: {token_info['market_cap']}")
    
    return initial_data

@handle_exception(log_msg="Error sending/updating message")
async def send_or_update_message(
    query: str,
    chat_id: int,
    message_id: Optional[int],
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
    reply_markup: InlineKeyboardMarkup,
    token_data: Dict[str, Any],
    token_info: Dict[str, Any],
    initial_data: Dict[str, Any]
) -> None:
    """Sends new message or updates existing one."""
    if message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            
            # Update data in storage with same message_id
            token_data_to_store = {
                'last_update_time': time.time(),
                'message_id': message_id,  # Keep existing message_id
                'chat_id': chat_id,
                'initial_data': initial_data,
                'token_info': token_info,
                'last_alert_multiplier': token_storage.get_token_data(query).get('last_alert_multiplier', 1) if token_storage.get_token_data(query) else 1,
                'added_time': token_storage.get_token_data(query).get('added_time', time.time()) if token_storage.get_token_data(query) else time.time(),
                'raw_api_data': token_data,
                'current_price_usd': token_info.get('price_usd', 0)
            }
            
            token_storage.store_token_data(query, token_data_to_store)
            
            
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Error updating message: {e}")
    else:
        try:
            # Send to admin first
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            
            # NEW: Send to ALL recipients (admin + authorized users)
            from handlers.auth_middleware import get_user_db
            from config import CONTROL_ADMIN_IDS
            user_db = get_user_db()
            all_users = user_db.get_all_users()
            active_users = [user for user in all_users if user['is_active']]
            
            # Create list of all recipients
            recipients = []
            
            # Add active users (excluding admin to avoid duplicate - admin already got message above)
            for user in active_users:
                if user['user_id'] not in CONTROL_ADMIN_IDS:  # Skip admin (already sent above)
                    recipients.append(user)
            
            # Send to all recipients
            for user in recipients:
                try:
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=message,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                    logger.info(f"New token info sent to user {user['user_id']}")
                except Exception as e:
                    logger.error(f"Error sending new token to user {user['user_id']}: {e}")
            
            # Save token data with admin's message_id
            token_data_to_store = {
                'last_update_time': time.time(),
                'message_id': sent_msg.message_id,
                'chat_id': sent_msg.chat_id,
                'initial_data': initial_data,
                'token_info': token_info,
                'last_alert_multiplier': 1,
                'added_time': time.time(),
                'raw_api_data': token_data,
                'current_price_usd': token_info.get('price_usd', 0)
            }
            
            token_storage.store_token_data(query, token_data_to_store)
            if token_data:  # token_data - это raw API данные
                save_raw_api_data_to_tracker_db(query, token_data)

            logger.info(f"Sent new message for token {query} (message_id: {sent_msg.message_id})")
            
        except Exception as e:
            logger.error(f"Error sending new message: {e}")

def save_raw_api_data_to_tracker_db(contract_address: str, raw_api_data: dict):
    """
    Простая функция для записи raw API данных в tracker БД.
    """
    try:
        import json
        import sqlite3
        
        logger.info(f"🔍 ОТЛАДКА: Пытаемся записать API данные для {contract_address}")
        logger.info(f"🔍 ОТЛАДКА: Размер API данных: {len(str(raw_api_data))} символов")
        
        # Подключаемся к tracker БД
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        # Сначала проверяем, есть ли токен в tracker БД
        cursor.execute('SELECT contract FROM tokens WHERE contract = ?', (contract_address,))
        exists = cursor.fetchone()
        
        if not exists:
            logger.error(f"❌ ОТЛАДКА: Токен {contract_address} НЕ НАЙДЕН в tracker БД!")
            conn.close()
            return
        
        logger.info(f"✅ ОТЛАДКА: Токен {contract_address} найден в tracker БД")
        
        # Преобразуем в JSON
        raw_api_json = json.dumps(raw_api_data, ensure_ascii=False)
        
        # Обновляем запись
        cursor.execute('''
        UPDATE tokens 
        SET raw_api_data = ?
        WHERE contract = ?
        ''', (raw_api_json, contract_address))
        
        logger.info(f"🔍 ОТЛАДКА: cursor.rowcount = {cursor.rowcount}")
        
        conn.commit()
        conn.close()
        
        if cursor.rowcount > 0:
            logger.info(f"✅ Raw API данные записаны в tracker БД для {contract_address}")
        else:
            logger.warning(f"⚠️ Токен {contract_address} не найден в tracker БД")
            
    except Exception as e:
        logger.error(f"❌ Ошибка записи в tracker БД: {e}")
        import traceback
        logger.error(traceback.format_exc())

@handle_exception(log_msg="Ошибка при обработке адреса токена")
async def process_token_address(address: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает адрес токена, полученный от внешнего источника."""
    logger.info(f"Обработка адреса токена: {address}")
    
    try:
        # Уведомляем пользователя о начале обработки
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"Получен новый контракт: {address}\nИщу информацию о токене..."
        )
        
        # Проверяем, есть ли уже данные об этом токене
        stored_data = token_storage.get_token_data(address)
        
        # Устанавливаем глобальный контекст для использования в уведомлениях
        set_telegram_context(context)
        
        # Получаем информацию о токене
        result = await get_token_info(address, chat_id, stored_data.get('message_id') if stored_data else None, context)
        
        logger.info(f"Обработка токена {address}: {'успешно' if result else 'не удалось'}")
        
        # Удаляем сообщение о поиске
        try:
            await msg.delete()
        except Exception as e:
            logger.error(f"Ошибка при удалении сообщения о поиске: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке адреса токена: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Ошибка при обработке контракта {address}: {str(e)}"
            )
        except:
            pass

@handle_exception(log_msg="Ошибка при проверке маркет капа")
async def check_market_cap(
    query: str,
    chat_id: Optional[int] = None,
    message_id: Optional[int] = None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
    check_growth: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Проверяет маркет кап токена и при необходимости определяет множители роста.
    """
    try:
        # Получаем данные о токене из хранилища
        stored_data = token_storage.get_token_data(query)
        if not stored_data:
            logger.warning(f"Не найдены данные о токене {query} для проверки")
            return None
        
        # Запрашиваем API через кэшированную функцию
        api_data = get_token_info_from_api(query)
        
        if not api_data:
            logger.warning(f"API не вернуло данные для токена {query}")
            return None
        
        pairs = api_data.get('pairs', [])
        
        if not pairs:
            logger.warning(f"API не вернуло данные о парах для токена {query}")
            return None
        
        # Берем первый результат
        token_data = pairs[0]
        
        # Получаем и обновляем market cap
        market_cap = token_data.get('fdv')
        raw_market_cap = market_cap
        market_cap_formatted = format_number(market_cap)
        
        # Обновляем поля в хранилище
        if 'token_info' in stored_data:
            stored_data['token_info']['market_cap'] = market_cap_formatted
            stored_data['token_info']['raw_market_cap'] = raw_market_cap
            
            # Обновляем время последнего обновления
            stored_data['last_update_time'] = time.time()
            
            # Обновляем данные в хранилище
            token_storage.store_token_data(query, stored_data)
            
            # Обновляем ATH, если текущее значение выше
            if raw_market_cap:
                token_storage.update_token_ath(query, raw_market_cap)
            
            # Результат для возврата
            result = {
                'market_cap': market_cap_formatted,
                'raw_market_cap': raw_market_cap
            }
            
            # Если нужно проверить рост, добавляем соответствующие данные
            if check_growth:
                send_notification = False
                current_multiplier = 1
                multiplier = 1  # Инициализируем multiplier значением по умолчанию
                
                initial_data = stored_data.get('initial_data', {})
                initial_mcap = initial_data.get('raw_market_cap', 0)
                
                if initial_mcap and initial_mcap > 0 and raw_market_cap:
                    # Вычисляем множитель
                    multiplier = raw_market_cap / initial_mcap
                    current_multiplier = int(multiplier)
                    
                    # Проверяем, был ли уже отправлен алерт для данного множителя
                    last_alert_multiplier = stored_data.get('last_alert_multiplier', 1)
                    
                    # Если текущий множитель >= 2 и превышает предыдущий алерт
                    if current_multiplier >= 2 and current_multiplier > last_alert_multiplier:
                        # Проверяем стратегию мониторинга
                        growth_percent = (multiplier - 1) * 100
                        send_notification = token_monitor_strategy.should_notify_growth(query, growth_percent)
                        
                        if send_notification:
                            logger.info(f"Обнаружен новый множитель для токена {query}: x{current_multiplier} (предыдущий: x{last_alert_multiplier})")
                
                result.update({
                    'multiplier': multiplier,  # Теперь multiplier всегда определен
                    'current_multiplier': current_multiplier,
                    'send_notification': send_notification
                })
            
            # Обновляем категорию токена в стратегии
            token_monitor_strategy.update_token_category(query, stored_data)
            
            return result
        else:
            logger.warning(f"В хранилище нет поля token_info для токена {query}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при проверке маркет капа для токена {query}: {e}")
        return None

@handle_exception(log_msg="Ошибка в мониторинге токенов")
async def monitor_token_market_caps(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Отслеживает маркет кап токенов с адаптивной стратегией проверки.
    """
    try:
        # Получаем все активные токены
        active_tokens = token_storage.get_active_tokens()
        
        if not active_tokens:
            logger.info("Нет активных токенов для мониторинга маркет капа")
            return
            
        logger.info(f"Запущен мониторинг маркет капа, всего токенов: {len(active_tokens)}")
        
        # Получаем список токенов, которые нужно проверить сейчас
        tokens_to_check = token_monitor_strategy.get_tokens_for_check(active_tokens)
        
        if not tokens_to_check:
            logger.info("Нет токенов для проверки в текущем цикле")
            return
            
        logger.info(f"Проверка {len(tokens_to_check)} токенов из {len(active_tokens)}")
        
        # Сохраняем контекст для использования в уведомлениях
        set_telegram_context(context)
        
        # Для каждого токена обновляем маркет кап и проверяем рост
        for query in tokens_to_check:
            try:
                token_data = active_tokens[query]
                chat_id = token_data.get('chat_id')
                message_id = token_data.get('message_id')  # ID сообщения с информацией о токене
                
                if not chat_id or not message_id:
                    logger.warning(f"Нет chat_id или message_id для токена {query}")
                    continue
                
                # Получаем последние данные о маркет капе
                result = await check_market_cap(query, chat_id, message_id, context, check_growth=True)
                
                if result:
                    logger.debug(f"Проверка токена {query}: MC={result.get('market_cap')}, Multiplier={result.get('multiplier', 1)}")
                    
                    # Проверяем рост и отправляем уведомление, если нужно
                    if result.get('send_notification', False):
                        # Отправляем уведомление о росте как ответ на сообщение о токене
                        current_multiplier = result.get('current_multiplier', 1)
                        
                        # Получаем данные для уведомления
                        token_info = token_data.get('token_info', {})
                        ticker = token_info.get('ticker', 'Неизвестно')
                        market_cap = result.get('market_cap', 'Неизвестно')
                        
                        # Проверяем, был ли уже отправлен алерт для данного множителя
                        last_alert_multiplier = token_data.get('last_alert_multiplier', 1)
                        
                        if current_multiplier > last_alert_multiplier:
                            # Отправляем уведомление о росте как ответ на информационное сообщение
                            await add_growth_notification(chat_id, ticker, current_multiplier, market_cap, message_id)
                            
                            # Обновляем последний алерт
                            token_storage.update_token_field(query, 'last_alert_multiplier', current_multiplier)
                            logger.info(f"Отправлено уведомление о росте токена {ticker} до x{current_multiplier}")
                
                # Добавляем небольшую паузу между запросами к API
                await asyncio.sleep(random.uniform(1.0, 2.5))
                
            except Exception as e:
                logger.error(f"Ошибка при мониторинге токена {query}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Ошибка в задаче мониторинга маркет капа: {e}")
        import traceback
        logger.error(traceback.format_exc())

@handle_exception(log_msg="Error sending token statistics")
async def send_token_stats(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Sends token statistics for the last 12 hours to ALL authorized users.
    This function runs on schedule.
    """
    logger.info("=== STARTING TOKEN STATISTICS GENERATION ===")
    
    # Get all tokens for analytics
    all_tokens = token_storage.get_all_tokens_for_analytics()
    logger.info(f"Loaded tokens for analysis: {len(all_tokens)}")
    
    if not all_tokens:
        logger.info("No tokens to generate statistics")
        return
    
    # Current time
    current_time = time.time()
    logger.info(f"Current time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Time 12 hours ago
    time_12h_ago = current_time - (12 * 60 * 60)
    logger.info(f"Time 12 hours ago: {datetime.datetime.fromtimestamp(time_12h_ago).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Statistics counters
    total_tokens = 0
    tokens_1_5x = 0
    tokens_2x = 0
    tokens_5x = 0
    
    # List of tokens for detailed logging
    analyzed_tokens = []
    
    # Check each token
    for query, data in all_tokens.items():
        # Check if token was added within last 12 hours
        added_time = data.get('added_time', 0)
        
        if not added_time:
            logger.info(f"Token {query} has no add time, skipping")
            continue
            
        # Log token add time
        token_added_time = datetime.datetime.fromtimestamp(added_time).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Token {query} added: {token_added_time}")
        
        if added_time < time_12h_ago:
            logger.info(f"Token {query} added more than 12 hours ago, skipping")
            continue
            
        # Get initial market cap
        initial_mcap = 0
        if 'initial_data' in data and 'raw_market_cap' in data['initial_data']:
            initial_mcap = data['initial_data'].get('raw_market_cap', 0)
        
        # Use ATH market cap instead of current
        ath_market_cap = data.get('ath_market_cap', 0)
        
        # Log market caps
        logger.info(f"Token {query} - Initial mcap: {initial_mcap}, ATH mcap: {ath_market_cap}")
        
        # Skip tokens without market cap data
        if not initial_mcap or not ath_market_cap:
            logger.info(f"Token {query} has no market cap data, skipping")
            continue
        
        # Calculate multiplier based on ATH
        multiplier = ath_market_cap / initial_mcap if initial_mcap > 0 else 0
        logger.info(f"Token {query} - Multiplier: {multiplier:.2f}x")
        
        # Update counters - using mutually exclusive categories
        total_tokens += 1
        
        if multiplier >= 5:
            tokens_5x += 1
        elif multiplier >= 2:
            tokens_2x += 1
        elif multiplier >= 1.5:
            tokens_1_5x += 1
        
        # Add to list for detailed logging
        ticker = "Unknown"
        if 'token_info' in data and 'ticker' in data['token_info']:
            ticker = data['token_info']['ticker']
            
        analyzed_tokens.append({
            'query': query,
            'ticker': ticker,
            'added_time': token_added_time,
            'initial_mcap': initial_mcap,
            'ath_mcap': ath_market_cap,
            'multiplier': multiplier
        })
    
    # Log detailed token statistics
    logger.info(f"Analyzed tokens for last 12 hours: {total_tokens}")
    logger.info(f"Tokens with growth 1.5x to <2x: {tokens_1_5x}")
    logger.info(f"Tokens with growth 2x to <5x: {tokens_2x}")
    logger.info(f"Tokens with growth ≥5x: {tokens_5x}")
    
    for token in analyzed_tokens:
        logger.info(f"Token {token['ticker']} ({token['query']}): added {token['added_time']}, multiplier {token['multiplier']:.2f}x")
    
    # Generate statistics message
    if total_tokens > 0:
        # Calculate success rate (>=1.5x)
        successful_tokens = tokens_1_5x + tokens_2x + tokens_5x
        hitrate_percent = (successful_tokens / total_tokens) * 100 if total_tokens > 0 else 0
        
        # Determine symbol for success rate visualization
        hitrate_symbol = "🔴"  # <30%
        if hitrate_percent >= 70:
            hitrate_symbol = "🟣"  # >=70%
        elif hitrate_percent >= 50:
            hitrate_symbol = "🟢"  # >=50%
        elif hitrate_percent >= 30:
            hitrate_symbol = "🟡"  # >=30%
        
        message = (
            f"Token stats for the last 12 hours:\n"
            f"> Total tokens: {total_tokens}\n"
            f"├ 1.5x-2x: {tokens_1_5x}\n"
            f"├ 2x-5x: {tokens_2x}\n"
            f"└ ≥5x: {tokens_5x}\n\n"
            f"Hitrate: {hitrate_percent:.1f}% {hitrate_symbol} (1.5x+)"
        )
        
        # NEW: Get ALL recipients (admin + active users)
        from handlers.auth_middleware import get_user_db
        from config import CONTROL_ADMIN_IDS
        user_db = get_user_db()
        all_users = user_db.get_all_users()
        active_users = [user for user in all_users if user['is_active']]
        
        # Create list of all recipients
        recipients = []
        
        # Add admin (always receives notifications)
        for admin_id in CONTROL_ADMIN_IDS:
            recipients.append({'user_id': admin_id, 'username': 'admin'})
        
        # Add active users (excluding admin duplicate)
        for user in active_users:
            if user['user_id'] not in CONTROL_ADMIN_IDS:  # Avoid duplicate
                recipients.append(user)
        
        if not recipients:
            logger.warning("No recipients to send statistics")
            return
        
        logger.info(f"Sending statistics to {len(recipients)} recipients (admin + active users)")
        
        # Send message to ALL recipients
        success_count = 0
        for user in recipients:
            try:
                logger.info(f"Sending statistics to chat {user['user_id']}...")
                
                # Add retry handling for sending
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await context.bot.send_message(
                            chat_id=user['user_id'],
                            text=message
                        )
                        logger.info(f"Token statistics successfully sent to chat {user['user_id']}")
                        success_count += 1
                        break
                    except (TimedOut, NetworkError) as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Timeout sending statistics to chat {user['user_id']} (attempt {attempt+1}/{max_retries}): {e}")
                            await asyncio.sleep(2)
                        else:
                            logger.error(f"Failed to send statistics to chat {user['user_id']} after {max_retries} attempts: {e}")
            except Exception as e:
                logger.error(f"Error sending statistics to chat {user['user_id']}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"Statistics successfully sent to {success_count} out of {len(recipients)} chats")
    else:
        logger.info("No tokens in last 12 hours to generate statistics")
    
    logger.info("=== TOKEN STATISTICS GENERATION COMPLETED ===")

@handle_exception(log_msg="Error sending weekly token statistics")
async def send_weekly_token_stats(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Sends token statistics for the last 7 days to ALL authorized users.
    This function runs on schedule.
    """
    logger.info("=== STARTING WEEKLY TOKEN STATISTICS GENERATION ===")
    
    # Get all tokens for analytics
    all_tokens = token_storage.get_all_tokens_for_analytics()
    logger.info(f"Loaded tokens for weekly analysis: {len(all_tokens)}")
    
    if not all_tokens:
        logger.info("No tokens to generate weekly statistics")
        return
    
    # Current time
    current_time = time.time()
    logger.info(f"Current time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Time 7 days ago (7 * 24 * 60 * 60 seconds)
    time_7d_ago = current_time - (7 * 24 * 60 * 60)
    logger.info(f"Time 7 days ago: {datetime.datetime.fromtimestamp(time_7d_ago).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Weekly statistics counters (ОБНОВЛЕННЫЕ КАТЕГОРИИ)
    total_tokens = 0
    tokens_1_5x = 0    # 1.5x-2x
    tokens_2x = 0      # 2x-3x
    tokens_3x = 0      # 3x-4x
    tokens_4x = 0      # 4x-5x
    tokens_5x = 0      # 5x-10x
    tokens_10x = 0     # >10x
    
    # List of tokens for detailed logging
    analyzed_tokens = []
    
    # Check each token
    for query, data in all_tokens.items():
        # Check if token was added within last 7 days
        added_time = data.get('added_time', 0)
        
        if not added_time:
            logger.info(f"Weekly: Token {query} has no add time, skipping")
            continue
            
        # Log token add time
        token_added_time = datetime.datetime.fromtimestamp(added_time).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Weekly: Token {query} added: {token_added_time}")
        
        if added_time < time_7d_ago:
            logger.info(f"Weekly: Token {query} added more than 7 days ago, skipping")
            continue
            
        # Get initial market cap
        initial_mcap = 0
        if 'initial_data' in data and 'raw_market_cap' in data['initial_data']:
            initial_mcap = data['initial_data'].get('raw_market_cap', 0)
        
        # Use ATH market cap instead of current
        ath_market_cap = data.get('ath_market_cap', 0)
        
        # If no ATH data or initial data, skip
        if not initial_mcap or initial_mcap <= 0:
            logger.info(f"Weekly: Token {query} has no valid initial market cap, skipping")
            continue
            
        if not ath_market_cap or ath_market_cap <= 0:
            logger.info(f"Weekly: Token {query} has no ATH market cap data, skipping")
            continue
        
        # Calculate multiplier using ATH
        multiplier = ath_market_cap / initial_mcap
        
        # Add to total count
        total_tokens += 1
        
        # ОБНОВЛЕННЫЕ КАТЕГОРИИ РОСТА
        if multiplier >= 10:
            tokens_10x += 1
            category = ">10x"
        elif multiplier >= 5:
            tokens_5x += 1
            category = "5x-10x"
        elif multiplier >= 4:
            tokens_4x += 1
            category = "4x-5x"
        elif multiplier >= 3:
            tokens_3x += 1
            category = "3x-4x"
        elif multiplier >= 2:
            tokens_2x += 1
            category = "2x-3x"
        elif multiplier >= 1.5:
            tokens_1_5x += 1
            category = "1.5x-2x"
        else:
            category = "<1.5x"
        
        analyzed_tokens.append({
            'query': query,
            'multiplier': multiplier,
            'category': category,
            'initial_mcap': initial_mcap,
            'ath_mcap': ath_market_cap
        })
        
        logger.info(f"Weekly: Token {query}: {multiplier:.2f}x ({category})")
    
    if total_tokens > 0:
        # Calculate success rates for weekly stats (все токены от 1.5x+)
        successful_tokens = tokens_1_5x + tokens_2x + tokens_3x + tokens_4x + tokens_5x + tokens_10x
        hitrate_percent = (successful_tokens / total_tokens) * 100 if total_tokens > 0 else 0
        
        # Different symbol system for weekly (пороги для 1.5x+)
        hitrate_symbol = "🔴"  # <40%
        if hitrate_percent >= 80:
            hitrate_symbol = "🟣"  # >=80%
        elif hitrate_percent >= 60:
            hitrate_symbol = "🟢"  # >=60%
        elif hitrate_percent >= 40:
            hitrate_symbol = "🟡"  # >=40%
        
        # ОБНОВЛЕННОЕ СООБЩЕНИЕ С НОВЫМИ КАТЕГОРИЯМИ
        message = (
            f"📊 Weekly Token Stats (7 days):\n"
            f"> Total tokens: {total_tokens}\n"
            f"├ 1.5x-2x: {tokens_1_5x}\n"
            f"├ 2x-3x: {tokens_2x}\n"
            f"├ 3x-4x: {tokens_3x}\n"
            f"├ 4x-5x: {tokens_4x}\n"
            f"├ 5x-10x: {tokens_5x}\n"
            f"└ >10x: {tokens_10x}\n\n"
            f"Weekly Hitrate: {hitrate_percent:.1f}% {hitrate_symbol} (1.5x+)"
        )
        
        # NEW: Get ALL recipients (admin + active users) - такой же код как в 12h версии
        from handlers.auth_middleware import get_user_db
        from config import CONTROL_ADMIN_IDS
        user_db = get_user_db()
        all_users = user_db.get_all_users()
        active_users = [user for user in all_users if user['is_active']]
        
        # Create list of all recipients
        recipients = []
        
        # Add admin (always receives notifications)
        for admin_id in CONTROL_ADMIN_IDS:
            recipients.append({'user_id': admin_id, 'username': 'admin'})
        
        # Add active users (excluding admin duplicate)
        for user in active_users:
            if user['user_id'] not in CONTROL_ADMIN_IDS:  # Avoid duplicate
                recipients.append(user)
        
        if not recipients:
            logger.warning("No recipients to send weekly statistics")
            return
        
        logger.info(f"Sending weekly statistics to {len(recipients)} recipients (admin + active users)")
        
        # Send message to ALL recipients
        success_count = 0
        for user in recipients:
            try:
                logger.info(f"Sending weekly statistics to chat {user['user_id']}...")
                
                # Add retry handling for sending
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await context.bot.send_message(
                            chat_id=user['user_id'],
                            text=message
                        )
                        logger.info(f"Weekly token statistics successfully sent to chat {user['user_id']}")
                        success_count += 1
                        break
                    except (TimedOut, NetworkError) as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Timeout sending weekly statistics to chat {user['user_id']} (attempt {attempt+1}/{max_retries}): {e}")
                            await asyncio.sleep(2)
                        else:
                            logger.error(f"Failed to send weekly statistics to chat {user['user_id']} after {max_retries} attempts: {e}")
            except Exception as e:
                logger.error(f"Error sending weekly statistics to chat {user['user_id']}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"Weekly statistics successfully sent to {success_count} out of {len(recipients)} chats")
    else:
        logger.info("No tokens in last 7 days to generate weekly statistics")
    
    logger.info("=== WEEKLY TOKEN STATISTICS GENERATION COMPLETED ===")



# Добавить также необходимые импорты в начало файла, если их нет:
import datetime
from telegram.error import TimedOut, NetworkError

# Получает данные о сигналах из SQLite базы данных tracker'а.
def get_signals_data(contract_address: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные о сигналах из SQLite базы данных tracker'а.
    
    Args:
        contract_address: Адрес контракта токена
        
    Returns:
        Словарь с данными о сигналах или None
    """
    try:
        import sqlite3
        from datetime import datetime
        import json
        import logging
        
        logger = logging.getLogger(__name__)
        
        # Подключаемся к SQLite базе данных tracker'а
        TRACKER_DB_PATH = 'tokens_tracker_database.db'
        conn = sqlite3.connect(TRACKER_DB_PATH)
        cursor = conn.cursor()
        
        # SQL запрос для получения данных о сигналах
        cursor.execute('''
        SELECT channels, channel_times, first_seen 
        FROM tokens 
        WHERE contract = ?
        ''', (contract_address,))
        
        result = cursor.fetchone()
        conn.close()
        
        # Если токен не найден в tracker базе - это нормально
        if not result:
            logger.debug(f"Токен {contract_address} не найден в tracker базе (добавлен пользователем напрямую)")
            return {
                'total_signals': 0,
                'channels_list': 'Добавлен пользователем',
                'signal_times': []
            }
        
        channels_str, channel_times_str, first_seen = result
        
        # Парсим данные из SQLite базы (поля уже в нужном формате)
        channels_str, channel_times_str, first_seen = result
        
        # В SQLite каналы хранятся как строка "канал1, канал2"
        channels = channels_str.split(', ') if channels_str and channels_str.strip() else []
        
        # channel_times хранится как JSON строка
        try:
            channel_times = json.loads(channel_times_str) if channel_times_str and channel_times_str.strip() else {}
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Некорректный JSON в channel_times для {contract_address}: '{channel_times_str}'")
            channel_times = {}
        
        if not channels:
            logger.debug(f"У токена {contract_address} нет каналов (добавлен пользователем)")
            return {
                'total_signals': 0,
                'channels_list': 'Добавлен пользователем',
                'signal_times': []
            }
        
        # Создаем список сигналов с временными метками
        signal_times = []
        channels_with_times = []
        
        for channel in channels:
            if channel in channel_times:
                time_str = channel_times[channel]
                try:
                    # Преобразуем время из формата HH:MM:SS в полную дату
                    # Используем дату из first_seen или текущую дату
                    if first_seen:
                        base_date = first_seen.split(' ')[0]  # Берем только дату из "YYYY-MM-DD HH:MM:SS"
                    else:
                        base_date = datetime.now().strftime('%Y-%m-%d')
                    
                    full_datetime = f"{base_date} {time_str}"
                    
                    # Проверяем корректность формата времени
                    datetime.strptime(time_str, '%H:%M:%S')
                    
                    signal_times.append(full_datetime)
                    channels_with_times.append(f"{channel} ({time_str})")
                except ValueError:
                    # Если время в неправильном формате, используем как есть
                    signal_times.append(time_str)
                    channels_with_times.append(f"{channel} ({time_str})")
            else:
                channels_with_times.append(channel)
        
        logger.info(f"📊 Получены данные о сигналах для {contract_address}: {len(channels)} каналов")
        
        return {
            'total_signals': len(channels),
            'channels_list': ' | '.join(channels_with_times),
            'signal_times': signal_times
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных сигналов для {contract_address}: {e}")
        return None
# Генерация excel с токенами 
@handle_exception(log_msg="Ошибка при генерации полной аналитики")
async def generate_analytics_excel(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Генерирует Excel файл с ПОЛНОЙ аналитикой по всем токенам (включая старые)."""
    try:
        # Получаем ВСЕ токены (включая старше 3 дней)
        all_tokens = token_storage.get_all_tokens_for_analytics()
        
        if not all_tokens:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Нет токенов для генерации полной аналитики."
            )
            return
        
        logger.info(f"Генерация полной аналитики для {len(all_tokens)} токенов (включая старые)")
        
        # Подготавливаем данные для Excel (используем ту же логику что и в generate_excel)
        tokens_data = []
        
        for query, token_data in all_tokens.items():
            try:
                token_info = token_data.get('token_info', {})
                initial_data = token_data.get('initial_data', {})
                raw_api_data = token_data.get('raw_api_data', {})
                
                # Безопасное извлечение числовых значений
                def safe_float(value, default=0.0):
                    try:
                        if value is None:
                            return default
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                def safe_int(value, default=0):
                    try:
                        if value is None:
                            return default
                        return int(value)
                    except (ValueError, TypeError):
                        return default
                
                # Базовая информация о токене
                ticker = str(token_info.get('ticker', 'Неизвестно'))
                ticker_address = str(token_info.get('ticker_address', 'Неизвестно'))
                chain_id = str(token_info.get('chain_id', ''))
                pair_address = str(token_info.get('pair_address', ''))
                
                # Безопасно извлекаем числовые данные
                current_market_cap = safe_float(token_info.get('raw_market_cap'))
                initial_market_cap = safe_float(initial_data.get('raw_market_cap'))
                ath_market_cap = safe_float(token_data.get('ath_market_cap'))
                                
                # Вычисляем множители роста
                ath_multiplier = 1.0
                
                if initial_market_cap > 0 and ath_market_cap > 0:
                    ath_multiplier = round(ath_market_cap / initial_market_cap, 2)
                
                # Временные данные
                added_time = "Неизвестно"
                if token_data.get('added_time'):
                    try:
                        added_time = datetime.datetime.fromtimestamp(safe_float(token_data.get('added_time'))).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        added_time = "Неизвестно"
                
                ath_time = "Неизвестно"
                if token_data.get('ath_time'):
                    try:
                        ath_time = datetime.datetime.fromtimestamp(safe_float(token_data.get('ath_time'))).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        ath_time = "Неизвестно"
                
                # Объемы торгов
                volume_5m = str(token_info.get('volume_5m', 'Неизвестно'))
                volume_1h = str(token_info.get('volume_1h', 'Неизвестно'))
                token_age = str(token_info.get('token_age', 'Неизвестно'))
                
                # Данные о транзакциях из API
                txns_data = raw_api_data.get('txns', {}) if raw_api_data else {}
                buys_5m = safe_int(txns_data.get('m5', {}).get('buys', 0) if isinstance(txns_data.get('m5'), dict) else 0)
                sells_5m = safe_int(txns_data.get('m5', {}).get('sells', 0) if isinstance(txns_data.get('m5'), dict) else 0)
                buys_1h = safe_int(txns_data.get('h1', {}).get('buys', 0) if isinstance(txns_data.get('h1'), dict) else 0)
                sells_1h = safe_int(txns_data.get('h1', {}).get('sells', 0) if isinstance(txns_data.get('h1'), dict) else 0)
                buys_24h = safe_int(txns_data.get('h24', {}).get('buys', 0) if isinstance(txns_data.get('h24'), dict) else 0)
                sells_24h = safe_int(txns_data.get('h24', {}).get('sells', 0) if isinstance(txns_data.get('h24'), dict) else 0)
                
                # Дополнительная информация
                dex_id = str(raw_api_data.get('dexId', 'Неизвестно')) if raw_api_data else 'Неизвестно'
                liquidity_usd = safe_float(raw_api_data.get('liquidity', {}).get('usd', 0) if raw_api_data else 0)
                                
                # Ссылки
                dexscreener_link = str(token_info.get('dexscreener_link', ''))
                axiom_link = str(token_info.get('axiom_link', ''))
                
                # Создаем строку для Excel с безопасным преобразованием типов
                row = {
                    # Основная информация
                    'Ticker': ticker,
                    'DEX': dex_id,
                    'Contract Address': ticker_address,
                    'Pair Address': pair_address,
                    'Chain ID': chain_id,
                    
                    'Token Age': token_age,
                    
                    # Временные данные
                    'Added Time': added_time,
                    'ATH Time': ath_time,
                    
                    # Цены и маркет кап
                    
                    'Initial Market Cap': format_number(initial_market_cap),
                    'ATH Market Cap': format_number(ath_market_cap),
                    'ATH Multiplier': f"{ath_multiplier}",
                    
                    'Current Market Cap': format_number(current_market_cap),
                                        
                    # Ликвидность
                    'Liquidity USD': format_number(liquidity_usd),
                                        
                    # Служебная информация
                    'Last Update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Добавляем данные о социальных сетях и веб-сайтах
                websites = []
                socials = []
                
                if isinstance(token_info.get('websites'), list):
                    websites = [str(site.get('url', '')) for site in token_info['websites'] if site.get('url')]
                
                if isinstance(token_info.get('socials'), list):
                    socials = [f"{str(social.get('type', ''))}:{str(social.get('url', ''))}" 
                              for social in token_info['socials'] if social.get('url')]
                
                row['Websites'] = ' | '.join(websites[:3]) if websites else ''
                row['Socials'] = ' | '.join(socials[:3]) if socials else ''
                
                # Дополнительные данные из API
                if raw_api_data:
                    row['FDV'] = safe_float(raw_api_data.get('fdv', 0))
                    row['Price Native'] = safe_float(raw_api_data.get('priceNative', 0))
                    
                    # Безопасно извлекаем изменения цены
                    price_change = raw_api_data.get('priceChange', {})
                    if isinstance(price_change, dict):
                        row['Price Change 5m'] = safe_float(price_change.get('m5', 0))
                        row['Price Change 1h'] = safe_float(price_change.get('h1', 0))
                        row['Price Change 6h'] = safe_float(price_change.get('h6', 0))
                        row['Price Change 24h'] = safe_float(price_change.get('h24', 0))
                    else:
                        row['Price Change 5m'] = 0
                        row['Price Change 1h'] = 0
                        row['Price Change 6h'] = 0
                        row['Price Change 24h'] = 0
                
                # Добавляем данные о сигналах из tracker базы
                from token_service import get_signals_data
                signals_data = get_signals_data(ticker_address)
                if signals_data:
                    row['Общее кол-во сигналов'] = signals_data['total_signals']
                    row['Каналы'] = signals_data['channels_list']
                    
                    # Добавляем отдельные сигналы с временными метками
                    for i, signal_time in enumerate(signals_data['signal_times'], 1):
                        row[f'{i} сигнал'] = signal_time
                else:
                    row['Общее кол-во сигналов'] = 0
                    row['Каналы'] = ''
                
                tokens_data.append(row)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке токена {query} для полной аналитики: {e}")
                
                # Создаем минимальную запись с ошибкой
                tokens_data.append({
                    'Ticker': str(token_data.get('token_info', {}).get('ticker', 'Ошибка')),
                    'Error': str(e)
                })
        
        if not tokens_data:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Не удалось обработать ни одного токена для полной аналитики."
            )
            return
        
        # Создаем DataFrame и сохраняем Excel файл
        import pandas as pd
        df = pd.DataFrame(tokens_data)
        
        # Сортируем по ATH множителю (по убыванию)
        if 'ATH Multiplier' in df.columns:
            try:
                df['ATH_Multiplier_Sort'] = df['ATH Multiplier'].astype(str).str.replace('x', '').astype(float)
                df = df.sort_values('ATH_Multiplier_Sort', ascending=False)
                df = df.drop('ATH_Multiplier_Sort', axis=1)
            except Exception as sort_e:
                logger.error(f"Ошибка при сортировке полной аналитики: {sort_e}")
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'full_analytics_{timestamp}.xlsx'
        
        # Настраиваем параметры Excel файла
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Full Analytics')
            
            # Получаем объект листа для форматирования
            worksheet = writer.sheets['Full Analytics']
            
            # Настраиваем ширину столбцов
            for idx, col in enumerate(df.columns):
                try:
                    max_len = max(
                        df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                        len(str(col))
                    )
                    
                    col_width = min(max_len + 2, 50)
                    
                    if idx < 26:
                        col_letter = chr(65 + idx)
                    else:
                        col_letter = chr(65 + idx // 26 - 1) + chr(65 + idx % 26)
                        
                    worksheet.column_dimensions[col_letter].width = col_width
                except Exception as col_e:
                    logger.error(f"Ошибка при настройке ширины столбца {idx}: {col_e}")
        
        # Отправляем файл пользователю
        try:
            with open(filename, 'rb') as excel_file:
                await context.bot.send_document(
                    chat_id=chat_id, 
                    document=excel_file, 
                    caption=f"📊 Let's analyze!!! ({len(tokens_data)} tokens)"
                )
            
            # Удаляем временный файл
            os.remove(filename)
            logger.info(f"Полная аналитика с {len(tokens_data)} токенами успешно отправлена")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке файла полной аналитики: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text="Не удалось отправить файл полной аналитики. Пожалуйста, попробуйте позже."
            )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации полной аналитики: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await context.bot.send_message(
            chat_id=chat_id,
            text="Произошла ошибка при генерации полной аналитики. Пожалуйста, попробуйте позже."
        )

@handle_exception(log_msg="Ошибка при запуске системы мониторинга")
async def start_token_monitoring_system(telegram_context):
    """Запуск системы мониторинга токенов с планировщиком."""
    try:
        # Импорты
        from task_scheduler import scheduler, TaskPriority
        import datetime
        
        # Запускаем планировщик задач
        scheduler.start()
        logger.info("Планировщик задач запущен")
        
        # Добавляем периодические задачи в планировщик
        scheduler.schedule_task(
            "token_monitor", 
            monitor_token_market_caps, 
            delay=5,  # через 5 секунд после запуска
            interval=30,  # каждые 30 секунд
            priority=TaskPriority.HIGH,
            context=telegram_context
        )
        
        # Настраиваем отправку статистики
        morning_time = datetime.time(8, 0, 0)  # 08:00:00
        evening_time = datetime.time(20, 0, 0)  # 20:00:00
        
        now = datetime.datetime.now().time()
        
        # Вычисляем, сколько секунд до следующего запуска утром
        morning_seconds = (
            (datetime.datetime.combine(datetime.date.today(), morning_time) - 
            datetime.datetime.combine(datetime.date.today(), now)).total_seconds()
        )
        if morning_seconds < 0:
            morning_seconds += 24 * 60 * 60  # Добавляем сутки
            
        # Вычисляем, сколько секунд до следующего запуска вечером
        evening_seconds = (
            (datetime.datetime.combine(datetime.date.today(), evening_time) - 
            datetime.datetime.combine(datetime.date.today(), now)).total_seconds()
        )
        if evening_seconds < 0:
            evening_seconds += 24 * 60 * 60  # Добавляем сутки
        
        # Планируем утреннюю статистику
        scheduler.schedule_task(
            "stats_morning", 
            send_token_stats, 
            delay=morning_seconds,  # через вычисленное время
            interval=24 * 60 * 60,  # каждые 24 часа
            priority=TaskPriority.NORMAL,
            context=telegram_context
        )
        
        # Планируем вечернюю статистику
        scheduler.schedule_task(
            "stats_evening", 
            send_token_stats, 
            delay=evening_seconds,  # через вычисленное время
            interval=24 * 60 * 60,  # каждые 24 часа
            priority=TaskPriority.NORMAL,
            context=telegram_context
        )
        
        logger.info("Система мониторинга токенов запущена")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка запуска системы мониторинга: {e}")
        return False