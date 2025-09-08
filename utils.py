from datetime import datetime
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
        return "Unknown"

def format_enhanced_message(token_info: Dict[str, Any], initial_data: Optional[Dict[str, Any]] = None) -> str:
    """Форматирует расширенное сообщение с дополнительной информацией о токене."""
    try:
        # Создаем ссылку на поиск адреса в Twitter/X.com
        ticker_address = token_info.get('ticker_address', '')
        twitter_search_link = f"https://twitter.com/search?q={ticker_address}"
        
        message = f"💰 *Ticker*: {token_info.get('ticker', 'Unknown')} [🔍]({twitter_search_link})\n"
        message += f"📝 *CA*: `{token_info.get('ticker_address', 'Unknown')}`\n\n"
        
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
        
        current_time = datetime.now().strftime("%d.%m.%y %H:%M:%S")
        message += f"💰 *Market Cap*: {token_info.get('market_cap', 'Unknown')}\n"
        message += f"⏱️ _Time: {current_time}_\n"
        message += f"🌱 *Token age*: {token_info.get('token_age', 'Unknown')}\n\n"
        
        # Блок с объемами торгов
        volumes_block = ""
        if token_info.get('volume_5m', 'Unknown') != "Unknown":
            volumes_block += f"📈 *Volume (5m)*: {token_info['volume_5m']}\n"
            
        if token_info.get('volume_1h', 'Unknown') != "Unknown":
            volumes_block += f"📈 *Volume (1h)*: {token_info['volume_1h']}\n"
        
        if volumes_block:
            message += volumes_block + "\n"
        
        # Получаем адрес токена для ссылок
        ticker_address = token_info.get('ticker_address', '')
        pair_address = token_info.get('pair_address', '')
        
        # Создаем ссылку на GMGN
        gmgn_link = f"https://gmgn.ai/sol/token/{ticker_address}"
        
        # Блок с ссылками на торговые площадки
        message += f"🔎 [DexScreener]({token_info.get('dexscreener_link', '#')}) | [Axiom]({token_info.get('axiom_link', '#')}) | [GMGN]({gmgn_link})\n\n"
                               
        logger.info("Форматирование сообщения завершено успешно")
        return message
    except Exception as e:
        logger.error(f"Ошибка при форматировании сообщения: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # В случае ошибки возвращаем базовое сообщение
        return f"🪙 *Ticker*: {token_info.get('ticker', 'Unknown')}\n📝 *CA*: `{token_info.get('ticker_address', 'Unknown')}`\n\n💰 *Market Cap*: {token_info.get('market_cap', 'Unknown')}\n\n_Ошибка при форматировании полного сообщения_"

def process_token_data(token_data: Dict[str, Any]) -> Dict[str, Any]:
    """Обрабатывает данные о токене."""
    # Извлекаем нужную информацию
    base_token = token_data.get('baseToken', {})
    ticker = base_token.get('symbol', 'Unknown').upper()
    ticker_address = base_token.get('address', 'Unknown')
    pair_address = token_data.get('pairAddress', '')
    chain_id = token_data.get('chainId', '')
    
    # Получаем Market Cap, используя ту же логику что и в batch_market_cap
    market_cap = None
    
    # 1. Сначала пробуем fdv (fully diluted value)
    market_cap = token_data.get('fdv')
    if market_cap:
        pass  # Используем fdv
    else:
        # 2. Пробуем marketCap
        market_cap = token_data.get('marketCap')
        if not market_cap:
            # 3. Пробуем вычислить из цены и liquidity
            price_usd = token_data.get('priceUsd')
            liquidity = token_data.get('liquidity', {})
            if price_usd and liquidity.get('base'):
                try:
                    # Примерная оценка market cap = price * base_liquidity * 2
                    market_cap = float(price_usd) * float(liquidity['base']) * 2
                except (ValueError, TypeError):
                    pass
            
            # 4. Если ничего не найдено, возвращаем liquidity в USD как приближение
            if not market_cap and liquidity.get('usd'):
                try:
                    market_cap = float(liquidity['usd'])
                except (ValueError, TypeError):
                    pass
    
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
    if token_data.get('pairCreatedAt'):
        delta = datetime.now() - datetime.fromtimestamp(token_data.get('pairCreatedAt')/1000)
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        
        # Формируем строку возраста токена с учетом дней, часов и минут
        if days > 0:
            if hours > 0 and minutes > 0:
                token_age = f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                token_age = f"{days}d {hours}h"
            else:
                token_age = f"{days}d"
        elif hours > 0:
            if minutes > 0:
                token_age = f"{hours}h {minutes}m"
            else:
                token_age = f"{hours}h"
        else:
            token_age = f"{minutes}m" if minutes > 0 else "< 1m"
    else:
        token_age = "Unknown"
    
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
        return ("Нет active tokens в списке отслеживаемых.", 1, 0)
    
    # Скрытые Tokens не используются в новой системе
    hidden_info = ""
    
    # Подготавливаем данные токенов для сортировки
    token_info_list = []
    
    try:
        # Обрабатываем как словарь или список
        if isinstance(tokens_data, dict):
            items = tokens_data.items()
        elif isinstance(tokens_data, list):
            items = enumerate(tokens_data)
        else:
            items = []
            
        for query, data in items:
            # Пропускаем None данные и скрытые Tokens
            if not data or not isinstance(data, dict):
                continue
            if data.get('hidden', False):
                continue
                
            # Безопасно получаем данные с проверками на None
            token_info = {}
            token_info['query'] = query
            
            # Получаем тикер
            token_info['ticker'] = query
            token_info_data = data.get('token_info')
            if token_info_data and isinstance(token_info_data, dict) and token_info_data.get('ticker'):
                token_info['ticker'] = data['token_info']['ticker']
            
            # Получаем время добавления из tracker DB (first_seen)
            token_info['initial_time'] = "Unknown"
            token_info['added_date'] = ""
            
            first_seen = data.get('first_seen')
            if first_seen:
                try:
                    # first_seen в формате "YYYY-MM-DD HH:MM:SS"
                    import datetime
                    added_datetime = datetime.datetime.strptime(first_seen, "%Y-%m-%d %H:%M:%S")
                    token_info['initial_time'] = added_datetime.strftime("%H:%M:%S")
                    token_info['added_date'] = added_datetime.strftime("%Y-%m-%d")
                    token_info['full_datetime'] = added_datetime.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    # Fallback если формат неправильный
                    token_info['initial_time'] = str(first_seen)
            elif data.get('added_time'):
                # Fallback на старый формат (timestamp)
                try:
                    import datetime
                    added_datetime = datetime.datetime.fromtimestamp(data.get('added_time', 0))
                    token_info['initial_time'] = added_datetime.strftime("%H:%M:%S")
                    token_info['added_date'] = added_datetime.strftime("%Y-%m-%d")
                    token_info['full_datetime'] = added_datetime.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
            
            # Получаем начальный маркет кап
            token_info['initial_market_cap'] = 0
            initial_data = data.get('initial_data') if data else None
            if initial_data and isinstance(initial_data, dict) and initial_data.get('raw_market_cap'):
                token_info['initial_market_cap'] = initial_data['raw_market_cap']
            
            # Получаем текущий маркет кап из мониторинга (приоритет) или из token_info (fallback)
            token_info['current_market_cap'] = 0
            if data and data.get('curr_mcap'):
                # Используем актуальный маркет кап из мониторинга
                token_info['current_market_cap'] = data['curr_mcap']
            elif token_info_data and isinstance(token_info_data, dict) and token_info_data.get('raw_market_cap'):
                # Fallback на статический маркет кап из token_info
                token_info['current_market_cap'] = token_info_data['raw_market_cap']
            
            # Получаем ATH маркет кап
            token_info['ath_market_cap'] = data.get('ath_market_cap', 0) if data else 0
            
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
            if data and token_info_data and isinstance(token_info_data, dict) and token_info_data.get('dexscreener_link'):
                token_info['dexscreener_link'] = token_info_data['dexscreener_link']
            
            token_info_list.append(token_info)
        
        # Сортируем Tokens по проценту роста ATH (от наибольшего к наименьшему)
        token_info_list.sort(key=lambda x: x.get('ath_percent', 0), reverse=True)
    except Exception as e:
        logger.error(f"Ошибка при подготовке данных токенов: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return ("An error occurred при формировании списка токенов. Пожалуйста, попробуйте позже.", 1, 0)
    
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
    
    # Tokens для текущей страницы
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
            ticker = token.get('ticker', 'Unknown')
            query = token.get('query', '')
            
            # Получаем полную дату и время
            if token.get('full_datetime'):
                date_time_str = token.get('full_datetime')
            else:
                added_date = token.get('added_date', '')
                initial_time = token.get('initial_time', 'Unknown')
                date_time_str = f"{added_date} {initial_time}" if added_date else initial_time
            
            dexscreener_link = token.get('dexscreener_link', '#')
            
            # Безопасное форматирование чисел
            initial_mc = format_number(token.get('initial_market_cap', 0)) if token.get('initial_market_cap') else "Unknown"
            current_mc = format_number(token.get('current_market_cap', 0)) if token.get('current_market_cap') else "Unknown"
            ath_mc = format_number(token.get('ath_market_cap', 0)) if token.get('ath_market_cap') else "Unknown"
            
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
        return ("An error occurred при форматировании списка токенов. Пожалуйста, попробуйте позже.", 1, 0)
    
    # Добавляем информацию о командах
    if page == total_pages - 1:  # Только на последней странице
        message += f"Используйте `/clear` для управления токенами.\n"
        message += f"Отправьте `/excel` для формирования Excel файла со всеми данными."
    
    return (message, total_pages, page)

def format_hotboard_message() -> str:
    """Форматирует сообщение с HOT BOARD токенами"""
    import sqlite3
    
    try:
        conn = sqlite3.connect("tokens_tracker_database.db")
        cursor = conn.cursor()
        
        # Получаем данные из hotboard, сортируем по ath_multiplier по убыванию
        cursor.execute('''
        SELECT contract, ticker, initial_mcap, initial_time, ath_mcap, ath_multiplier
        FROM hotboard 
        ORDER BY ath_multiplier DESC
        ''')
        
        hotboard_data = cursor.fetchall()
        conn.close()
        
        if not hotboard_data:
            return "🔥 HOT BOARD\n\nСписок пуст. Добавьте Tokens для отображения."
        
        message = "🔥 HOT BOARD\n\n"
        
        for i, (contract, ticker, initial_mcap, initial_time, ath_mcap, ath_multiplier) in enumerate(hotboard_data, 1):
            # Форматируем множитель
            multiplier_str = f"{ath_multiplier:.0f}X" if ath_multiplier else "1X"
            
            # Форматируем тикер с ссылкой на DexScreener
            if ticker:
                dexscreener_url = f"https://dexscreener.com/solana/{contract}"
                ticker_display = f"[{ticker}]({dexscreener_url})"
            else:
                ticker_display = "???"
            
            message += f"{i}. *{multiplier_str}* Ticker: {ticker_display}\n"
            
            # Добавляем информацию о Called at (initial mcap) и времени
            if initial_mcap and initial_time:
                # Форматируем initial_mcap
                if initial_mcap >= 1000000:
                    mcap_formatted = f"${initial_mcap/1000000:.1f}M"
                elif initial_mcap >= 1000:
                    mcap_formatted = f"${initial_mcap/1000:.0f}K"
                else:
                    mcap_formatted = f"${initial_mcap:.0f}"
                
                # Форматируем время - берем только дату и время без секунд
                try:
                    from datetime import datetime
                    if len(initial_time) > 16:  # Если есть секунды
                        time_formatted = initial_time[:16]  # Обрезаем секунды
                    else:
                        time_formatted = initial_time
                except:
                    time_formatted = initial_time
                
                message += f"   └ _Called at {mcap_formatted}_ • {time_formatted}\n"
            
            message += "\n"  # Пустая строка между токенами
        
        return message
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании HOT BOARD: {e}")
        return "🔥 HOT BOARD\n\nОшибка загрузки данных."