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
from handlers.auth_middleware import user_required, admin_required
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
    
    # Проверяем, нужно ли отправлять уведомление о росте
    growth_notification = check_growth_notification(query, token_info, initial_data)
    
    # Формируем сообщение
    message = format_enhanced_message(token_info, initial_data)
    
    # Больше не создаём кнопку Refresh в одиночном окне
    reply_markup = None
    
    # Отправляем или обновляем сообщение, только если есть контекст и chat_id
    if context and chat_id:
        await send_or_update_message(
            query, chat_id, message_id, context, message, reply_markup, token_data, token_info, initial_data
        )
        
        # Отправляем уведомление о росте, если нужно
        if growth_notification and context and chat_id:
            query = growth_notification['query']
            current_multiplier = growth_notification['current_multiplier']
            token_info = growth_notification['token_info']
            ticker = token_info['ticker']
            market_cap = token_info['market_cap']
            
            # Используем уже существующую функцию из notifications.py
            add_growth_notification(chat_id, ticker, current_multiplier, market_cap)
            
            # Обновляем последний алерт
            token_storage.update_token_field(query, 'last_alert_multiplier', current_multiplier)
    
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

@handle_exception(log_msg="Ошибка при проверке роста")
def check_growth_notification(
    query: str,
    token_info: Dict[str, Any], 
    initial_data: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Проверяет, нужно ли отправить уведомление о росте."""
    if not initial_data or 'raw_market_cap' not in initial_data or not initial_data['raw_market_cap']:
        return None
    
    if 'raw_market_cap' not in token_info or not token_info['raw_market_cap']:
        return None
    
    initial_mcap = initial_data['raw_market_cap']
    current_mcap = token_info['raw_market_cap']
    
    # Вычисляем множитель
    if initial_mcap <= 0:
        return None
    
    multiplier = current_mcap / initial_mcap
    current_multiplier = int(multiplier)
    
    # Если множитель < 2, не отправляем уведомление
    if current_multiplier < 2:
        return None
    
    # Проверяем, был ли уже отправлен алерт для данного множителя
    stored_data = token_storage.get_token_data(query)
    if not stored_data:
        return None
    
    last_alert_multiplier = stored_data.get('last_alert_multiplier', 1)
    
    # Если текущий множитель <= предыдущий алерт, не отправляем уведомление
    if current_multiplier <= last_alert_multiplier:
        return None
    
    # Проверяем стратегию мониторинга
    growth_percent = (multiplier - 1) * 100
    
    if not token_monitor_strategy.should_notify_growth(query, growth_percent):
        logger.info(f"Уведомление о росте токена {query} не отправлено: не соответствует стратегии")
        return None
        
    return {
        'query': query,
        'current_multiplier': current_multiplier,
        'token_info': token_info
    }

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
            logger.info(f"Updated message for token {query} (message_id: {message_id})")
            
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
            logger.info(f"Sent new message for token {query} (message_id: {sent_msg.message_id})")
            
        except Exception as e:
            logger.error(f"Error sending new message: {e}")

@handle_exception(log_msg="Ошибка при отправке уведомления о росте")
async def send_growth_notification_handler(
    growth_data: Dict[str, Any],
    chat_id: int,
    message_id: int,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Обработчик отправки уведомления о росте токена.
    После отправки уведомления сразу фиксирует новый ATH, если он выше предыдущего.
    """
    # Извлекаем данные
    query              = growth_data['query']
    current_multiplier = growth_data['current_multiplier']
    token_info         = growth_data['token_info']
    ticker             = token_info['ticker']
    market_cap_str     = token_info['market_cap']
    raw_mcap           = token_info['raw_market_cap']  # числовое значение

    # 1. Отправляем уведомление пользователю
    await add_growth_notification(
        chat_id=chat_id,
        ticker=ticker,
        multiplier=current_multiplier,
        market_cap=market_cap_str,
        reply_to_message_id=message_id
    )

    # 2. Обновляем в БД последний отправленный множитель
    token_storage.update_token_field(
        query=query,
        field_name='last_alert_multiplier',
        value=current_multiplier
    )

    # 3. Фиксируем новый ATH, если он выше предыдущего
    #    Внутри update_token_ath есть сравнение и условие перезаписи
    token_storage.update_token_ath(query=query, new_mcap=raw_mcap)
    logger.info(f"ATH для токена {query} обновлён до {raw_mcap}")

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

def get_signals_data(contract_address: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные о сигналах из tracker базы данных.
    
    Args:
        contract_address: Адрес контракта токена
        
    Returns:
        Словарь с данными о сигналах или None
    """
    try:
        import json
        import os
        from datetime import datetime
        
        # Загружаем данные из tracker базы
        tracker_db_file = 'tokens_tracker_database.json'
        if not os.path.exists(tracker_db_file):
            return None
            
        with open(tracker_db_file, 'r', encoding='utf-8') as f:
            tracker_db = json.load(f)
        
        # Ищем токен по адресу контракта
        token_data = tracker_db.get(contract_address)
        if not token_data:
            return None
        
        # Получаем список каналов и времена сигналов
        channels = token_data.get('channels', [])
        channel_times = token_data.get('channel_times', {})
        first_seen = token_data.get('first_seen', '')
        
        if not channels:
            return None
        
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
                        # Если есть first_seen, используем его дату
                        base_date = datetime.now().strftime('%Y-%m-%d')
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
    
# ========== НОВЫЕ ФУНКЦИИ БАТЧИНГА ==========
# token_service.py (дополнения к существующему файлу)

import logging
import asyncio
import time
from typing import Dict, Any, Optional, List, Tuple
from enum import Enum

# Импорты из существующих модулей
from task_scheduler import scheduler, TaskPriority
from api_cache import (
    fetch_tokens_batch, 
    process_batch_requests, 
    split_into_batches,
    validate_token_addresses
)
import token_storage

logger = logging.getLogger(__name__)

# ========== НОВЫЕ КЛАССЫ ДЛЯ ПРИОРИТЕТОВ ==========

class TokenPriority(Enum):
    """Приоритеты токенов для мониторинга"""
    HIGH = "high"      # Токены с недавним ростом - каждые 30 секунд
    NORMAL = "normal"  # Обычные токены - каждые 5 минут  
    LOW = "low"        # Неактивные токены - каждые 30 минут


class TokenBatch:
    """Класс для представления батча токенов"""
    def __init__(self, addresses: List[str], priority: TokenPriority):
        self.addresses = addresses
        self.priority = priority
        self.last_update = 0
        self.update_count = 0
    
    def __len__(self):
        return len(self.addresses)
    
    def should_update(self, current_time: float) -> bool:
        """Проверяет, нужно ли обновлять этот батч"""
        intervals = {
            TokenPriority.HIGH: 30,      # 30 секунд
            TokenPriority.NORMAL: 300,   # 5 минут
            TokenPriority.LOW: 1800      # 30 минут
        }
        
        interval = intervals.get(self.priority, 300)
        return (current_time - self.last_update) >= interval


# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ БАТЧ-МОНИТОРИНГА ==========

# Хранилище батчей по приоритетам
_token_batches: Dict[TokenPriority, List[TokenBatch]] = {
    TokenPriority.HIGH: [],
    TokenPriority.NORMAL: [],
    TokenPriority.LOW: []
}

# Флаг активности мониторинга
_batch_monitoring_active = False

# Статистика мониторинга
_monitoring_stats = {
    "total_updates": 0,
    "total_tokens_processed": 0,
    "last_update_time": 0,
    "errors_count": 0
}


# ========== ФУНКЦИИ ОПРЕДЕЛЕНИЯ ПРИОРИТЕТОВ ==========

def determine_token_priority(token_data: Dict[str, Any]) -> TokenPriority:
    """
    Определяет приоритет токена на основе его активности.
    
    Args:
        token_data: Данные токена из базы
        
    Returns:
        Приоритет токена
    """
    try:
        # Получаем данные для анализа
        current_multiplier = token_data.get('current_multiplier', 1.0)
        last_alert_multiplier = token_data.get('last_alert_multiplier', 1.0)
        last_updated = token_data.get('last_updated', 0)
        
        current_time = time.time()
        time_since_update = current_time - last_updated
        
        # HIGH приоритет: токены с недавним ростом или высокой активностью
        if (current_multiplier > last_alert_multiplier * 1.1 or  # Рост на 10%+
            current_multiplier >= 2.0 or                         # Мультипликатор >= 2x
            time_since_update < 3600):                           # Обновлялся в последний час
            return TokenPriority.HIGH
        
        # LOW приоритет: неактивные токены
        elif (current_multiplier < 1.5 and                      # Низкий рост
              time_since_update > 86400):                       # Не обновлялся > 24 часов
            return TokenPriority.LOW
        
        # NORMAL приоритет: все остальные
        else:
            return TokenPriority.NORMAL
            
    except Exception as e:
        logger.error(f"Ошибка при определении приоритета токена: {e}")
        return TokenPriority.NORMAL


def categorize_tokens_by_priority() -> Dict[TokenPriority, List[str]]:
    """
    Категоризирует все токены по приоритетам.
    
    Returns:
        Словарь {приоритет: [список_адресов]}
    """
    try:
        # Получаем все активные токены
        all_tokens = token_storage.get_all_tokens()
        
        if not all_tokens:
            logger.warning("Нет токенов для категоризации")
            return {priority: [] for priority in TokenPriority}
        
        # Группируем по приоритетам
        prioritized_tokens = {priority: [] for priority in TokenPriority}
        
        for token_data in all_tokens:
            # Если token_data - строка, преобразуем в словарь
            if isinstance(token_data, str):
                token_data = {'query': token_data, 'is_active': True}
    
            if not isinstance(token_data, dict):
                continue
        
            if not token_data.get('is_active', True):
                continue
        
            address = token_data.get('query', '')
            if not address:
                continue
        
            priority = determine_token_priority(token_data)
            prioritized_tokens[priority].append(address)
        
        # Логируем статистику
        for priority, addresses in prioritized_tokens.items():
            logger.info(f"Приоритет {priority.value}: {len(addresses)} токенов")
        
        return prioritized_tokens
        
    except Exception as e:
        logger.error(f"Ошибка при категоризации токенов: {e}")
        return {priority: [] for priority in TokenPriority}


# ========== ФУНКЦИИ СОЗДАНИЯ БАТЧЕЙ ==========

def create_token_batches(max_batch_size: int = 30) -> None:
    """
    Создает батчи токенов, сгруппированные по приоритетам.
    
    Args:
        max_batch_size: Максимальный размер одного батча
    """
    global _token_batches
    
    try:
        logger.info("Создание батчей токенов по приоритетам...")
        
        # Получаем токены, сгруппированные по приоритетам
        prioritized_tokens = categorize_tokens_by_priority()
        
        # Очищаем существующие батчи
        _token_batches = {priority: [] for priority in TokenPriority}
        
        # Создаем батчи для каждого приоритета
        for priority, addresses in prioritized_tokens.items():
            if not addresses:
                continue
                
            # Валидируем адреса
            valid_addresses = validate_token_addresses(addresses)
            
            if not valid_addresses:
                logger.warning(f"Нет валидных адресов для приоритета {priority.value}")
                continue
            
            # Разбиваем на батчи
            address_batches = split_into_batches(valid_addresses, max_batch_size)
            
            # Создаем объекты TokenBatch
            for batch_addresses in address_batches:
                batch = TokenBatch(batch_addresses, priority)
                _token_batches[priority].append(batch)
            
            logger.info(f"Создано {len(address_batches)} батчей для приоритета {priority.value}")
        
        # Общая статистика
        total_batches = sum(len(batches) for batches in _token_batches.values())
        total_tokens = sum(len(batch) for batches in _token_batches.values() for batch in batches)
        
        logger.info(f"Всего создано {total_batches} батчей для {total_tokens} токенов")
        
    except Exception as e:
        logger.error(f"Ошибка при создании батчей: {e}")
        import traceback
        logger.error(traceback.format_exc())


# ========== ФУНКЦИИ МОНИТОРИНГА БАТЧЕЙ ==========

async def process_token_batch(batch: TokenBatch) -> None:
    """
    Обрабатывает один батч токенов.
    
    Args:
        batch: Батч для обработки
    """
    try:
        current_time = time.time()
        
        # Проверяем, нужно ли обновлять этот батч
        if not batch.should_update(current_time):
            return
        
        logger.info(f"Обработка батча приоритета {batch.priority.value} с {len(batch)} токенами")
        
        # Получаем данные для всех токенов в батче
        batch_results = fetch_tokens_batch(batch.addresses)
        
        # Обрабатываем результаты для каждого токена
        updated_count = 0
        error_count = 0
        
        for address, api_data in batch_results.items():
            try:
                # Вызываем существующую функцию обработки одного токена
                # Передаем None для context и chat_id, так как это фоновый процесс
                token_info = await get_token_info(address, context=None, chat_id=None, 
                                                message_id=None)
                
                if token_info:
                    updated_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке токена {address}: {e}")
                error_count += 1
        
        # Обновляем статистику батча
        batch.last_update = current_time
        batch.update_count += 1
        
        # Обновляем глобальную статистику
        _monitoring_stats["total_updates"] += 1
        _monitoring_stats["total_tokens_processed"] += updated_count
        _monitoring_stats["last_update_time"] = current_time
        _monitoring_stats["errors_count"] += error_count
        
        logger.info(f"Батч обработан: {updated_count} успешно, {error_count} ошибок")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке батча: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def monitor_high_priority_tokens() -> None:
    """Мониторит токены высокого приоритета (каждые 30 секунд)"""
    if not _batch_monitoring_active:
        return
        
    try:
        high_priority_batches = _token_batches.get(TokenPriority.HIGH, [])
        
        if not high_priority_batches:
            logger.debug("Нет батчей высокого приоритета для мониторинга")
            return
        
        logger.debug(f"Мониторинг {len(high_priority_batches)} HIGH приоритетных батчей")
        
        for batch in high_priority_batches:
            await process_token_batch(batch)
            
            # Небольшая задержка между батчами
            await asyncio.sleep(1)
        
    except Exception as e:
        logger.error(f"Ошибка в мониторинге HIGH приоритета: {e}")


async def monitor_normal_priority_tokens() -> None:
    """Мониторит токены нормального приоритета (каждые 5 минут)"""
    if not _batch_monitoring_active:
        return
        
    try:
        normal_priority_batches = _token_batches.get(TokenPriority.NORMAL, [])
        
        if not normal_priority_batches:
            logger.debug("Нет батчей нормального приоритета для мониторинга")
            return
        
        logger.info(f"Мониторинг {len(normal_priority_batches)} NORMAL приоритетных батчей")
        
        for batch in normal_priority_batches:
            await process_token_batch(batch)
            
            # Задержка между батчами
            await asyncio.sleep(2)
        
    except Exception as e:
        logger.error(f"Ошибка в мониторинге NORMAL приоритета: {e}")


async def monitor_low_priority_tokens() -> None:
    """Мониторит токены низкого приоритета (каждые 30 минут)"""
    if not _batch_monitoring_active:
        return
        
    try:
        low_priority_batches = _token_batches.get(TokenPriority.LOW, [])
        
        if not low_priority_batches:
            logger.debug("Нет батчей низкого приоритета для мониторинга")
            return
        
        logger.info(f"Мониторинг {len(low_priority_batches)} LOW приоритетных батчей")
        
        for batch in low_priority_batches:
            await process_token_batch(batch)
            
            # Большая задержка между батчами низкого приоритета
            await asyncio.sleep(5)
        
    except Exception as e:
        logger.error(f"Ошибка в мониторинге LOW приоритета: {e}")


# ========== ФУНКЦИИ УПРАВЛЕНИЯ МОНИТОРИНГОМ ==========

async def start_batch_monitoring_system() -> None:
    """Запускает систему батч-мониторинга с использованием task_scheduler"""
    global _batch_monitoring_active
    
    try:
        logger.info("Запуск системы батч-мониторинга токенов...")
        
        # Создаем начальные батчи
        create_token_batches()
        
        # Активируем мониторинг
        _batch_monitoring_active = True
        
        # Планируем задачи мониторинга с разными интервалами
        
        # HIGH приоритет: каждые 30 секунд
        scheduler.schedule_task(
            "monitor_high_priority",
            monitor_high_priority_tokens,
            delay=10,        # Начинаем через 10 секунд
            interval=30,     # Повторяем каждые 30 секунд
            priority=TaskPriority.HIGH
        )
        
        # NORMAL приоритет: каждые 5 минут
        scheduler.schedule_task(
            "monitor_normal_priority", 
            monitor_normal_priority_tokens,
            delay=60,        # Начинаем через 1 минуту
            interval=300,    # Повторяем каждые 5 минут
            priority=TaskPriority.NORMAL
        )
        
        # LOW приоритет: каждые 30 минут
        scheduler.schedule_task(
            "monitor_low_priority",
            monitor_low_priority_tokens,
            delay=120,       # Начинаем через 2 минуты
            interval=1800,   # Повторяем каждые 30 минут
            priority=TaskPriority.LOW
        )
        
        # Пересоздание батчей каждые 10 минут (для перераспределения приоритетов)
        scheduler.schedule_task(
            "recreate_batches",
            recreate_token_batches,
            delay=600,       # Начинаем через 10 минут
            interval=600,    # Повторяем каждые 10 минут
            priority=TaskPriority.NORMAL
        )
        
        # Логирование статистики каждые 5 минут
        scheduler.schedule_task(
            "log_monitoring_stats",
            log_monitoring_statistics,
            delay=300,       # Начинаем через 5 минут
            interval=300,    # Повторяем каждые 5 минут
            priority=TaskPriority.LOW
        )
        
        logger.info("Система батч-мониторинга запущена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при запуске системы батч-мониторинга: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def stop_batch_monitoring_system() -> None:
    """Останавливает систему батч-мониторинга"""
    global _batch_monitoring_active
    
    try:
        logger.info("Остановка системы батч-мониторинга...")
        
        # Деактивируем мониторинг
        _batch_monitoring_active = False
        
        # Отменяем все задачи мониторинга
        monitoring_tasks = [
            "monitor_high_priority",
            "monitor_normal_priority", 
            "monitor_low_priority",
            "recreate_batches",
            "log_monitoring_stats"
        ]
        
        for task_id in monitoring_tasks:
            if scheduler.cancel_task(task_id):
                logger.info(f"Задача {task_id} отменена")
        
        # Очищаем батчи
        global _token_batches
        _token_batches = {priority: [] for priority in TokenPriority}
        
        logger.info("Система батч-мониторинга остановлена")
        
    except Exception as e:
        logger.error(f"Ошибка при остановке системы батч-мониторинга: {e}")


async def recreate_token_batches() -> None:
    """Пересоздает батчи токенов (для перераспределения приоритетов)"""
    try:
        logger.info("Пересоздание батчей токенов...")
        create_token_batches()
        logger.info("Батчи пересозданы успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при пересоздании батчей: {e}")


async def log_monitoring_statistics() -> None:
    """Логирует статистику мониторинга"""
    try:
        stats = get_monitoring_statistics()
        
        logger.info("=== СТАТИСТИКА БАТЧ-МОНИТОРИНГА ===")
        logger.info(f"Всего обновлений: {stats['total_updates']}")
        logger.info(f"Токенов обработано: {stats['total_tokens_processed']}")
        logger.info(f"Ошибок: {stats['errors_count']}")
        logger.info(f"Активных батчей: {stats['active_batches']}")
        logger.info(f"Последнее обновление: {stats['last_update_ago']:.1f} сек назад")
        logger.info("=====================================")
        
    except Exception as e:
        logger.error(f"Ошибка при логировании статистики: {e}")


# ========== ФУНКЦИИ ПОЛУЧЕНИЯ СТАТИСТИКИ ==========

def get_monitoring_statistics() -> Dict[str, Any]:
    """Возвращает статистику работы батч-мониторинга"""
    current_time = time.time()
    
    # Подсчитываем активные батчи
    active_batches = sum(len(batches) for batches in _token_batches.values())
    
    # Время с последнего обновления
    last_update_ago = current_time - _monitoring_stats.get("last_update_time", current_time)
    
    return {
        "active": _batch_monitoring_active,
        "total_updates": _monitoring_stats.get("total_updates", 0),
        "total_tokens_processed": _monitoring_stats.get("total_tokens_processed", 0),
        "errors_count": _monitoring_stats.get("errors_count", 0),
        "active_batches": active_batches,
        "last_update_time": _monitoring_stats.get("last_update_time", 0),
        "last_update_ago": last_update_ago,
        "batches_by_priority": {
            priority.value: len(batches) 
            for priority, batches in _token_batches.items()
        }
    }


def get_batch_details() -> Dict[str, Any]:
    """Возвращает детальную информацию о батчах"""
    details = {}
    
    for priority, batches in _token_batches.items():
        priority_details = []
        
        for i, batch in enumerate(batches):
            batch_info = {
                "batch_id": i,
                "size": len(batch),
                "last_update": batch.last_update,
                "update_count": batch.update_count,
                "addresses": batch.addresses[:3]  # Показываем только первые 3 адреса
            }
            priority_details.append(batch_info)
        
        details[priority.value] = {
            "count": len(batches),
            "total_tokens": sum(len(batch) for batch in batches),
            "batches": priority_details
        }
    
    return details


# ========== ФУНКЦИИ ИНТЕГРАЦИИ С СУЩЕСТВУЮЩИМ КОДОМ ==========

async def start_token_monitoring_system(application):
    """
    Модифицированная функция запуска системы мониторинга.
    Интегрируется с существующим кодом и добавляет батч-мониторинг.
    """
    try:
        logger.info("Запуск системы мониторинга токенов...")
        
        # Запускаем планировщик задач
        scheduler.start()
        
        # Запускаем существующую систему мониторинга (если есть)
        # ... здесь код существующей функции start_token_monitoring_system ...
        
        # Запускаем НОВУЮ систему батч-мониторинга
        await start_batch_monitoring_system()
        
        logger.info("Система мониторинга токенов запущена полностью")
        
    except Exception as e:
        logger.error(f"Ошибка при запуске системы мониторинга: {e}")
        import traceback
        logger.error(traceback.format_exc())


# ========== ФУНКЦИИ ДЛЯ АДМИН ПАНЕЛИ ==========

def format_monitoring_status() -> str:
    """Форматирует статус мониторинга для отображения в админ панели"""
    try:
        stats = get_monitoring_statistics()
        
        status = "🔄" if stats["active"] else "⏸️"
        
        message = f"""📊 *Статус батч-мониторинга* {status}

📈 *Статистика:*
• Всего обновлений: {stats['total_updates']}
• Токенов обработано: {stats['total_tokens_processed']}
• Ошибок: {stats['errors_count']}
• Активных батчей: {stats['active_batches']}

🎯 *Батчи по приоритетам:*
• HIGH (30 сек): {stats['batches_by_priority']['high']} батчей
• NORMAL (5 мин): {stats['batches_by_priority']['normal']} батчей  
• LOW (30 мин): {stats['batches_by_priority']['low']} батчей

⏰ Последнее обновление: {stats['last_update_ago']:.1f} сек назад"""

        return message
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании статуса: {e}")
        return "❌ Ошибка при получении статуса мониторинга"


# ========== ЭКСПОРТ НОВЫХ ФУНКЦИЙ ==========

__all__ = [
    # Классы
    'TokenPriority',
    'TokenBatch',
    
    # Основные функции мониторинга
    'start_batch_monitoring_system',
    'stop_batch_monitoring_system',
    'create_token_batches',
    
    # Функции статистики
    'get_monitoring_statistics',
    'get_batch_details',
    'format_monitoring_status',
    
    # Функции приоритетов
    'determine_token_priority',
    'categorize_tokens_by_priority'
]