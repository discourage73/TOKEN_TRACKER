import datetime
from typing import Dict, Any, Optional, Union, List

import logging
logger = logging.getLogger(__name__)

def format_number(value: Union[int, float, str]) -> str:
    """Форматирует числовое значение для отображения."""
    if isinstance(value, (int, float)):
        if value >= 1000000000:
            return f"${value / 1000000000:.2f}B"
        elif value >= 1000000:
            return f"${value / 1000000:.2f}M"
        elif value >= 1000:
            return f"${value / 1000:.2f}K"
        else:
            return f"${value:.2f}"
    elif isinstance(value, str):
        try:
            value = float(value)
            return format_number(value)
        except (ValueError, TypeError):
            return value
    else:
        return "Неизвестно"

def format_enhanced_message(token_info: Dict[str, Any], initial_data: Optional[Dict[str, Any]] = None) -> str:
    """Форматирует расширенное сообщение с дополнительной информацией о токене."""
    try:
        # Создаем ссылку на поиск адреса в Twitter/X.com
        ticker_address = token_info.get('ticker_address', '')
        twitter_search_link = f"https://twitter.com/search?q={ticker_address}"
        
        message = f"🪙 *Ticker*: {token_info.get('ticker', 'Неизвестно')} [🔍]({twitter_search_link})\n"
        message += f"📝 *CA*: `{token_info.get('ticker_address', 'Неизвестно')}`\n\n"
        
        # Блок с ссылками на сайты (перемещен вверх)
        if 'websites' in token_info and token_info.get('websites'):
            website_links = [f"[{website.get('label', 'Website')}]({website.get('url', '')})" 
                           for website in token_info.get('websites', []) if website.get('url')]
            
            if website_links:
                message += f"🌐 *Website*: {' | '.join(website_links)}\n"
                logger.info(f"Добавлены ссылки на сайты: {website_links}")
        
        # Блок с ссылками на соцсети (перемещен вверх)
        if 'socials' in token_info and token_info.get('socials'):
            social_links = [f"[{social.get('type', '').capitalize()}]({social.get('url', '')})" 
                          for social in token_info.get('socials', []) if social.get('url') and social.get('type')]
            
            if social_links:
                message += f"📱 *Social*: {' | '.join(social_links)}\n\n"
                logger.info(f"Добавлены ссылки на соцсети: {social_links}")
        else:
            message += "\n"  # Добавляем дополнительный перенос, если нет соцсетей
        
        current_time = datetime.datetime.now().strftime("%d.%m.%y %H:%M:%S")
        message += f"💰 *Market Cap*: {token_info.get('market_cap', 'Неизвестно')} | _Time: {current_time}_\n"
        
        message += f"⏱️ *Token age*: {token_info.get('token_age', 'Неизвестно')}\n\n"
        
        # Блок с объемами торгов
        volumes_block = ""
        if token_info.get('volume_5m', 'Неизвестно') != "Неизвестно":
            volumes_block += f"📈 *Volume (5m)*: {token_info['volume_5m']}\n"
            
        if token_info.get('volume_1h', 'Неизвестно') != "Неизвестно":
            volumes_block += f"📈 *Volume (1h)*: {token_info['volume_1h']}\n"
        
        if volumes_block:
            message += volumes_block + "\n"
        
        # Получаем адрес токена для ссылок
        ticker_address = token_info.get('ticker_address', '')
        pair_address = token_info.get('pair_address', '')
        
        # Создаем ссылку на GMGN
        gmgn_link = f"https://gmgn.ai/sol/token/{ticker_address}"
        
        # Блок с ссылками на торговые площадки
        message += f"🔎 *Ссылки*: [DexScreener]({token_info.get('dexscreener_link', '#')}) | [Axiom]({token_info.get('axiom_link', '#')}) | [GMGN]({gmgn_link})\n\n"
                               
        logger.info("Форматирование сообщения завершено успешно")
        return message
    except Exception as e:
        logger.error(f"Ошибка при форматировании сообщения: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # В случае ошибки возвращаем базовое сообщение
        return f"🪙 *Ticker*: {token_info.get('ticker', 'Неизвестно')}\n📝 *CA*: `{token_info.get('ticker_address', 'Неизвестно')}`\n\n💰 *Market Cap*: {token_info.get('market_cap', 'Неизвестно')}\n\n_Ошибка при форматировании полного сообщения_"

def calculate_token_age(timestamp: Optional[int]) -> str:
    """Рассчитывает возраст токена от времени создания с детальной разбивкой."""
    if not timestamp:
        return "Unknown"
    
    try:
        # Преобразование timestamp из миллисекунд в секунды
        creation_time = datetime.datetime.fromtimestamp(timestamp / 1000)
        now = datetime.datetime.now()
        delta = now - creation_time
        
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        
        result = []
        
        if days > 0:
            days_str = "day" if days == 1 else "days"
            result.append(f"{days} {days_str}")
        
        if hours > 0:
            hours_str = "hour" if hours == 1 else "hours"
            result.append(f"{hours} {hours_str}")
        
        if minutes > 0 and days == 0:  # Показываем минуты только если прошло меньше дня
            minutes_str = "minute" if minutes == 1 else "minutes"
            result.append(f"{minutes} {minutes_str}")
        
        if not result:
            return "Less than a minute"
        
        return " ".join(result)
    except Exception as e:
        logger.error(f"Ошибка при расчете возраста токена: {e}")
        return "Unknown"

def process_token_data(token_data: Dict[str, Any]) -> Dict[str, Any]:
    """Обрабатывает данные о токене."""
    # Извлекаем нужную информацию
    base_token = token_data.get('baseToken', {})
    ticker = base_token.get('symbol', 'Неизвестно').upper()
    ticker_address = base_token.get('address', 'Неизвестно')
    pair_address = token_data.get('pairAddress', '')
    chain_id = token_data.get('chainId', '')
    
    # Получаем market cap, если доступно
    market_cap = token_data.get('fdv')
    raw_market_cap = market_cap  # Сохраняем исходное значение
    market_cap_formatted = format_number(market_cap)
    
    # Создаем ссылки
    dexscreener_link = f"https://dexscreener.com/{chain_id}/{pair_address}"
    axiom_link = f"https://axiom.trade/meme/{pair_address}"
    
    # Получаем объем торгов
    volume_data = token_data.get('volume', {})
    volume_5m = volume_data.get('m5')
    volume_1h = volume_data.get('h1')
    
    # Форматируем объемы
    volume_5m_formatted = format_number(volume_5m)
    volume_1h_formatted = format_number(volume_1h)
    
    # Получаем время создания токена
    pair_created_at = token_data.get('pairCreatedAt')
    token_age = calculate_token_age(pair_created_at)
    
    # Получаем информацию о социальных сетях и сайтах
    info = token_data.get('info', {})
    websites = info.get('websites', [])
    socials = info.get('socials', [])
    
    return {
        'ticker': ticker,
        'ticker_address': ticker_address,
        'pair_address': pair_address,
        'chain_id': chain_id,
        'market_cap': market_cap_formatted,
        'raw_market_cap': raw_market_cap,
        'volume_5m': volume_5m_formatted,
        'volume_1h': volume_1h_formatted,
        'token_age': token_age,
        'dexscreener_link': dexscreener_link,
        'axiom_link': axiom_link,
        'websites': websites,
        'socials': socials
    }

def format_tokens_list(tokens_data: Dict[str, Dict[str, Any]], page: int = 0, tokens_per_page: int = 10) -> tuple:
    """
    Форматирует список токенов для отображения с процентами от ATH.
    Возвращает кортеж (message, total_pages, current_page)
    """
    if not tokens_data:
        return ("Нет активных токенов в списке отслеживаемых.", 1, 0)
    
    # Получаем количество скрытых токенов для информации
    hidden_info = ""
    try:
        # Импортируем модуль token_storage, если он еще не импортирован
        import token_storage as ts
        hidden_tokens_count = len(ts.get_hidden_tokens())
        hidden_info = f" (скрытых: {hidden_tokens_count})" if hidden_tokens_count > 0 else ""
    except Exception as e:
        logger.error(f"Ошибка при получении скрытых токенов: {str(e)}")
    
    # Подготавливаем данные токенов для сортировки
    token_info_list = []
    
    try:
        for query, data in tokens_data.items():
            # Пропускаем скрытые токены
            if data.get('hidden', False):
                continue
                
            # Безопасно получаем данные с проверками на None
            token_info = {}
            token_info['query'] = query
            
            # Получаем тикер
            token_info['ticker'] = query
            if data.get('token_info', {}).get('ticker'):
                token_info['ticker'] = data['token_info']['ticker']
            
            # Получаем время добавления и преобразуем его в полную дату и время
            token_info['initial_time'] = "Неизвестно"
            token_info['added_date'] = ""
            
            if data.get('added_time'):
                # Преобразуем timestamp в дату и время
                import datetime
                added_datetime = datetime.datetime.fromtimestamp(data.get('added_time', 0))
                token_info['initial_time'] = added_datetime.strftime("%H:%M:%S")
                token_info['added_date'] = added_datetime.strftime("%Y-%m-%d")
                token_info['full_datetime'] = added_datetime.strftime("%Y-%m-%d %H:%M:%S")
            elif data.get('initial_data', {}).get('time'):
                token_info['initial_time'] = data['initial_data']['time']
            
            # Получаем начальный маркет кап
            token_info['initial_market_cap'] = 0
            if data.get('initial_data', {}).get('raw_market_cap'):
                token_info['initial_market_cap'] = data['initial_data']['raw_market_cap']
            
            # Получаем текущий маркет кап
            token_info['current_market_cap'] = 0
            if data.get('token_info', {}).get('raw_market_cap'):
                token_info['current_market_cap'] = data['token_info']['raw_market_cap']
            
            # Получаем ATH маркет кап
            token_info['ath_market_cap'] = data.get('ath_market_cap', 0)
            
            # Если ATH не установлен или меньше начального, используем начальный как ATH
            if not token_info['ath_market_cap'] or (token_info['initial_market_cap'] > token_info['ath_market_cap']):
                token_info['ath_market_cap'] = token_info['initial_market_cap']
            
            # Безопасно вычисляем проценты для ATH и текущего значения
            token_info['ath_percent'] = 0
            if token_info['initial_market_cap'] and token_info['ath_market_cap'] and token_info['initial_market_cap'] > 0:
                token_info['ath_percent'] = ((token_info['ath_market_cap'] / token_info['initial_market_cap']) - 1) * 100
            
            token_info['curr_percent'] = 0
            if token_info['initial_market_cap'] and token_info['current_market_cap'] and token_info['initial_market_cap'] > 0:
                token_info['curr_percent'] = ((token_info['current_market_cap'] / token_info['initial_market_cap']) - 1) * 100
            
            # Получаем ссылку на DexScreener
            token_info['dexscreener_link'] = "#"
            if data.get('token_info', {}).get('dexscreener_link'):
                token_info['dexscreener_link'] = data['token_info']['dexscreener_link']
            
            token_info_list.append(token_info)
        
        # Сортируем токены по проценту роста ATH (от наибольшего к наименьшему)
        token_info_list.sort(key=lambda x: x.get('ath_percent', 0), reverse=True)
    except Exception as e:
        logger.error(f"Ошибка при подготовке данных токенов: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return ("Произошла ошибка при формировании списка токенов. Пожалуйста, попробуйте позже.", 1, 0)
    
    # Расчет количества страниц
    total_tokens = len(token_info_list)
    total_pages = (total_tokens + tokens_per_page - 1) // tokens_per_page  # Округление вверх
    
    # Проверка валидности номера страницы
    if page < 0:
        page = 0
    elif page >= total_pages and total_pages > 0:
        page = total_pages - 1
    
    # Начало и конец диапазона токенов для текущей страницы
    start_idx = page * tokens_per_page
    end_idx = min(start_idx + tokens_per_page, total_tokens)
    
    # Токены для текущей страницы
    page_tokens = token_info_list[start_idx:end_idx]
    
    # Заголовок сообщения
    message = f"📋 *Список отслеживаемых токенов ({total_tokens} шт.){hidden_info}*\n"
    message += f"Страница {page + 1} из {total_pages}\n\n"
    
    # Загрузим tracker_db для получения эмодзи токенов
    tracker_emojis = {}
    try:
        import sqlite3
        import os
        
        # Путь к SQL базе данных трекера (правильный путь из проекта)
        TRACKER_DB_PATH = 'tokens_tracker_database.db'
        
        # Проверяем, существует ли SQL база данных
        if os.path.exists(TRACKER_DB_PATH):
            conn = sqlite3.connect(TRACKER_DB_PATH)
            cursor = conn.cursor()
            
            # Получаем все эмоджи из таблицы tokens
            cursor.execute('SELECT contract, emojis FROM tokens WHERE emojis IS NOT NULL AND emojis != ""')
            rows = cursor.fetchall()
            
            # Заполняем словарь эмоджи (contract используется как query)
            for contract, emojis in rows:
                if emojis:
                    tracker_emojis[contract] = emojis
            
            conn.close()
            logger.info(f"Загружено {len(tracker_emojis)} эмоджи из SQL базы данных")
        else:
            logger.warning(f"SQL база данных {TRACKER_DB_PATH} не найдена")
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке эмоджи из SQL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Форматируем список токенов для текущей страницы
    try:
        for i, token in enumerate(page_tokens, start=start_idx + 1):
            ticker = token.get('ticker', 'Неизвестно')
            query = token.get('query', '')
            
            # Получаем полную дату и время
            if token.get('full_datetime'):
                date_time_str = token.get('full_datetime')
            else:
                added_date = token.get('added_date', '')
                initial_time = token.get('initial_time', 'Неизвестно')
                date_time_str = f"{added_date} {initial_time}" if added_date else initial_time
            
            dexscreener_link = token.get('dexscreener_link', '#')
            
            # Безопасное форматирование чисел
            initial_mc = format_number(token.get('initial_market_cap', 0)) if token.get('initial_market_cap') else "Неизвестно"
            current_mc = format_number(token.get('current_market_cap', 0)) if token.get('current_market_cap') else "Неизвестно"
            ath_mc = format_number(token.get('ath_market_cap', 0)) if token.get('ath_market_cap') else "Неизвестно"
            
            # Форматируем проценты для ATH и текущего значения
            ath_percent = token.get('ath_percent', 0)
            curr_percent = token.get('curr_percent', 0)
            
            ath_percent_str = f"+{ath_percent:.1f}%" if ath_percent >= 0 else f"{ath_percent:.1f}%"
            curr_percent_str = f"+{curr_percent:.1f}%" if curr_percent >= 0 else f"{curr_percent:.1f}%"
            
            # Получаем эмодзи для токена из tracker_db
            emojis = tracker_emojis.get(query, "")
            
            # Добавляем информацию о токене в сообщение со ссылкой в названии тикера
            message += f"{i}. [{ticker}]({dexscreener_link}):\n"
            message += f"   Time: {date_time_str} Mcap: {initial_mc}\n"
            message += f"   {ath_percent_str} ATH {ath_mc}\n"
            message += f"   {curr_percent_str} CURR {current_mc}\n"
            
            # Добавляем строку эмодзи после строки с CURR, если они есть
            if emojis:
                message += f"   {emojis}\n"
            
            message += "\n"
    except Exception as e:
        logger.error(f"Ошибка при форматировании списка токенов: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return ("Произошла ошибка при форматировании списка токенов. Пожалуйста, попробуйте позже.", 1, 0)
    
    # Добавляем информацию о командах
    if page == total_pages - 1:  # Только на последней странице
        message += f"Используйте `/clear` для управления токенами.\n"
        message += f"Отправьте `/excel` для формирования Excel файла со всеми данными."
    
    return (message, total_pages, page)