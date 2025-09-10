from telethon import TelegramClient, events
import asyncio
import logging
import sys
import re
import json
import os
import time
import sqlite3
import signal
from datetime import datetime, timedelta

# Настройка безопасного логирования с обработкой ошибок кодировки
class UnicodeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            # Заменяем проблемные символы на '?'
            msg = self.format(record)
            try:
                stream = self.stream
                stream.write(msg.encode(stream.encoding, errors='replace').decode(stream.encoding) + self.terminator)
                self.flush()
            except (UnicodeError, IOError):
                self.handleError(record)
        except Exception:
            self.handleError(record)

# Добавляем собственный обработчик для консоли
console_handler = UnicodeStreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)

# Безопасное преобразование строк для логирования
def safe_str(text):
    """Безопасное преобразование текста с эмодзи для логирования."""
    if text is None:
        return "None"
    try:
        # Ограничиваем длину и безопасно представляем текст
        return str(text[:100]).replace('\n', ' ') + "..."
    except:
        return "[Текст содержит непечатаемые символы]"

# Импортируем конфигурацию
from config import TELEGRAM_TOKEN, DEXSCREENER_API_URL, API_ID, API_HASH, TARGET_BOT

# Минимальное количество каналов для отправки сигнала в RadarDexBot
MIN_SIGNALS = 22  # Токен должен появиться минимум в 20 каналах

def set_min_signals(new_value):
    """Функция для изменения MIN_SIGNALS в рантайме."""
    global MIN_SIGNALS
    MIN_SIGNALS = new_value
    logger.info(f"⚙️ MIN_SIGNALS изменен на {new_value}")
    return MIN_SIGNALS

# Словарь соответствия тегов и эмодзи
TAG_EMOJI_MAP = {
    "snipeKOL": "🎯",     # дартс
    "snipeGEM": "💎",     # бриллиант
    "TG_KOL": "🍀",       # клевер
    "EarlyGEM": "💎",     # бриллиант
    "EarlyKOL": "⚡",      # молния
    "SmartMoney": "💵",   # доллар
    "Whale Bought": "🐋", # кит
    "Volume alert": "🚀", # ракета
    "AlphAI_KOL": "🐂"    # бык
}

# Каналы для мониторинга (ID канала -> имя)
SOURCE_CHANNELS = {
    2121262250: {"name": "@DoctoreDegens", "tag": "EarlyGEM"},
    2055101998: {"name": "@metagambler", "tag": "EarlyGEM"},
    1500214409: {"name": "@GemsmineEth", "tag": "TG_KOL"},
    1794471884: {"name": "@MineGems", "tag": "EarlyGEM"},
    1603469217: {"name": "@ZionGems", "tag": "EarlyGEM"},
    2366686880: {"name": "@Ranma_Calls_Solana", "tag": "EarlyGEM"},
    1818702441: {"name": "@michiosuzukiofsatoshicalls", "tag": "EarlyGEM"},
    1763265784: {"name": "@MarkDegens", "tag": "EarlyGEM"},
    2284638367: {"name": "@GemDynasty", "tag": "TG_KOL"},
    1554385364: {"name": "@SultanPlays", "tag": "TG_KOL"},
    1756488143: {"name": "@lowtaxsolana", "tag": "EarlyGEM"},
    1883929251: {"name": "@gogetagambles", "tag": "EarlyGEM"},
    2362597228: {"name": "@Parkergamblles", "tag": "EarlyGEM"},
    2696740432: {"name": "@cringemonke2", "tag": "SmartMoney"},
    2051055592: {"name": "@Mrbigbagcalls", "tag": "TG_KOL"},
    2299508637: {"name": "@BaddiesAi", "tag": "EarlyGEM"},
    2441888429: {"name": "@BasedchadsGamble", "tag": "BasedchadsGamble"},
    2514471362: {"name": "@cringemonke", "tag": "Whale Bought"},
    2030684366: {"name": "@uranusX100", "tag": "EarlyGEM"},
    2483474049: {"name": "@solhousesignal", "tag": "TG_KOL"},
    2093384030: {"name": "@solearlytrending", "tag": "TG_KOL"},
    2253840165: {"name": "@the_defi_investor", "tag": "EarlyGEM"},
    2307208059: {"name": "@MemesDontLies", "tag": "TG_KOL"},
    2314485533: {"name": "@kolsignal", "tag": "TG_KOL"},
    2441746747: {"name": "@DegenRaydiumSig", "tag": "TG_KOL"},
    2141713314: {"name": "@TheDegenBoysLounge", "tag": "TG_KOL"},
    #2318939340: {"name": "@SolanaXpertWallet", "tag": "SmartMoney"},
    2352003756: {"name": "@SolanaWhalesMarket", "tag": "Whale Bought"},
    1437584820: {"name": "@Pumpfunpremium", "tag": "TG_KOL"},
    2380594298: {"name": "@whaleBuyBotFree", "tag": "Whale Bought"},
    2249008099: {"name": "@SIGNALMEVX", "tag": "TG_KOL"},
    1700113598: {"name": "@KhronosAllChain", "tag": "TG_KOL"},
    #2305781763: {"name": "@mevxpfdexpaid", "tag": "TG_KOL"},
    2589360530: {"name": "@AveSignalMonitor", "tag": "TG_KOL"},
    2531914184: {"name": "@astrasolcalls", "tag": "TG_KOL"},
    1419575394: {"name": "@WizzyTrades", "tag": "TG_KOL"},
    1955928748: {"name": "@XAceCalls", "tag": "TG_KOL"},
    1845214884: {"name": "@TehPump_Calls", "tag": "TG_KOL"},
    2715551388: {"name": "@degenjamesai", "tag": "TG_KOL"},
    2521423800: {"name": "@RobinHood_Smarts", "tag": "TG_KOL"},
    1537838682: {"name": "@bruisersgambles", "tag": "TG_KOL"},
    2636521153: {"name": "@quaintsignals", "tag": "TG_KOL"},
    2020729829: {"name": "@shitcoingemsalert", "tag": "TG_KOL"},
    1523240618: {"name": "@mewgambles", "tag": "TG_KOL"},
    1998961899: {"name": "@gem_tools_calls", "tag": "TG_KOL"},
    2097131181: {"name": "@onchainalphatrench", "tag": "TG_KOL"},
    2439158541: {"name": "@PnutHiddenGems", "tag": "TG_KOL"},
    2250330423: {"name": "@Jessieewow", "tag": "EarlyGEM"},
    2216480403: {"name": "@solana_gold_miner", "tag": "TG_KOL"},
    2300385026: {"name": "@ChapoInsider", "tag": "EarlyGEM"},
    1758611100: {"name": "@mad_apes_gambles", "tag": "TG_KOL"},
    2536988241: {"name": "@AlphaONE_volumealerts", "tag": "Volume alert"},
    2534842510: {"name": "@AlphAI_signals_sol_en", "tag": "AlphAI_KOL"}
}

# Словарь для динамического хранения имен каналов
channel_names_cache = {}

# Файлы базы данных
TRACKER_DB_PATH = "tokens_tracker_database.db" 

# Хранилище токенов
tokens_db = {}

# Получение имени канала по ID
def get_channel_name(chat_id):
    """Синхронная обертка для получения имени канала из словаря."""
    # Обрабатываем ID с префиксом '-100'
    if str(chat_id).startswith('-100'):
        # Для каналов с ID вида -1001234567890
        stripped_id = int(str(chat_id)[4:])  # Удаляем -100 из начала
    else:
        stripped_id = chat_id
    
    # Пытаемся найти в словаре
    channel_info = SOURCE_CHANNELS.get(stripped_id)
    if channel_info:
        if isinstance(channel_info, dict):
            return channel_info["name"]
        return channel_info
    else:
        # Если не нашли, возвращаем общее обозначение
        return f"@channel_{abs(stripped_id)}"

def get_channel_emojis_by_names(channel_names):
    """Получает эмодзи каналов по их именам."""
    emojis = ""
    for name in channel_names:
        # Ищем канал по имени
        for chat_id, info in SOURCE_CHANNELS.items():
            if isinstance(info, dict) and info["name"] == name:
                tag = info["tag"]
                emoji = TAG_EMOJI_MAP.get(tag, "🍀")  # Используем клевер по умолчанию
                emojis += emoji
                break
            elif info == name:  # Для обратной совместимости
                emojis += "🍀"  # Используем клевер по умолчанию
                break
    
    return emojis

def extract_solana_contracts(text):
    """Извлекает адреса контрактов Solana из текста."""
    if not text:
        return []
        
    # Паттерн для контрактов Solana: начинаются обычно с определенных букв и имеют 32-44 символа
    pattern = r"\b[a-zA-Z0-9]{32,44}\b"
    potential_contracts = re.findall(pattern, text)
    
    # Отфильтровываем кошельки разработчиков и оставляем только контракты токенов
    filtered_contracts = []
    for contract in potential_contracts:
        # Преобразуем в нижний регистр для проверок на содержимое
        contract_lower = contract.lower()
        
        # Проверяем основные ключевые слова
        if ('pump' in contract_lower or 
            'moon' in contract_lower or 
            'bonk' in contract_lower or
            re.match(r'^[0-9]', contract)):
            filtered_contracts.append(contract)
            continue
            
        # Дополнительные признаки токенов:
        
        # 1. Начинается с заглавной буквы и имеет определенные паттерны заглавных букв
        if re.match(r'^[A-Z]', contract) and len(re.findall(r'[A-Z]', contract)) >= 3:
            # Паттерн начинается с заглавной буквы и имеет не менее 3 заглавных букв
            filtered_contracts.append(contract)
            continue
            
        # 2. Имеет чередование регистров (маленькая-большая буква)
        if re.search(r'[a-z][A-Z][a-z]', contract) or re.search(r'[A-Z][a-z][A-Z]', contract):
            filtered_contracts.append(contract)
            continue
            
        # 3. Содержит много цифр (не менее 5) и много заглавных букв (не менее 5)
        digit_count = sum(c.isdigit() for c in contract)
        upper_count = sum(c.isupper() for c in contract)
        if digit_count >= 5 and upper_count >= 5:
            filtered_contracts.append(contract)
            continue
            
        # 4. Проверка на специфичные последовательности
        if any(seq in contract for seq in ['Fg', 'Hc', 'Dk', 'CHL', 'GukM']):
            filtered_contracts.append(contract)
            continue
    
    return filtered_contracts

# SQLite функции для сохранения ВСЕХ токенов
def init_tracker_db():
    """Инициализирует таблицу для ВСЕХ токенов."""
    try:
        conn = sqlite3.connect(TRACKER_DB_PATH)
        cursor = conn.cursor()
        
        # Создаем таблицу для ВСЕХ токенов с новыми полями
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            contract TEXT PRIMARY KEY,
            channels TEXT DEFAULT '',
            channel_times TEXT DEFAULT '',
            channel_count INTEGER DEFAULT 0,
            first_seen TEXT NOT NULL,
            signal_reached_time TEXT DEFAULT NULL,
            time_to_threshold TEXT DEFAULT NULL,
            emojis TEXT DEFAULT '',
            updated_at TEXT,
            message_sent INTEGER DEFAULT 0,
            message_id INTEGER DEFAULT 0
        )
        ''')
        
        # Создаем индексы для оптимизации
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_channel_count 
        ON tokens(channel_count)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_first_seen 
        ON tokens(first_seen)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_message_sent 
        ON tokens(message_sent)
        ''')
        
        conn.commit()
        conn.close()
        logger.info("SQLite таблица для ВСЕХ токенов инициализирована")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации SQLite БД: {e}")

def save_tokens_to_db():
    """Сохраняет ВСЕ данные tokens_db в SQLite базу данных."""
    try:
        conn = sqlite3.connect(TRACKER_DB_PATH)
        cursor = conn.cursor()
        
        for contract, data in tokens_db.items():
            # Получаем данные из tokens_db
            channels = ', '.join(data.get('channels', []))
            channel_count = data.get('channel_count', 0)
            first_seen = data.get('first_seen', '')
            signal_reached_time = data.get('signal_reached_time', None)
            time_to_threshold = data.get('time_to_threshold', None)
            message_sent = 1 if data.get('message_sent', False) else 0
            message_id = data.get('message_id', 0)
            emojis = data.get('emojis', '')
            
            # Сериализуем channel_times в JSON строку
            channel_times = json.dumps(data.get('channel_times', {}), ensure_ascii=False)
            
            # Используем локальное время для updated_at
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Сначала получаем существующие token_info и raw_api_data
            cursor.execute('SELECT token_info, raw_api_data FROM tokens WHERE contract = ?', (contract,))
            existing_data = cursor.fetchone()
            existing_token_info = existing_data[0] if existing_data else None
            existing_raw_api_data = existing_data[1] if existing_data else None
            
            # Используем INSERT OR REPLACE, сохраняя существующие token_info и raw_api_data
            cursor.execute('''
            INSERT OR REPLACE INTO tokens 
            (contract, channels, channel_times, channel_count, first_seen, signal_reached_time, 
             time_to_threshold, message_sent, message_id, emojis, updated_at, token_info, raw_api_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (contract, channels, channel_times, channel_count, first_seen, signal_reached_time,
                  time_to_threshold, message_sent, message_id, emojis, current_time, 
                  existing_token_info, existing_raw_api_data))
        
        conn.commit()
        conn.close()
        logger.info(f"Сохранено {len(tokens_db)} ВСЕХ токенов в SQLite базу данных")
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении всех токенов в SQLite: {e}")
        import traceback
        logger.error(traceback.format_exc())

def load_tokens_from_db():
    """Загружает данные tokens_db из SQLite базы данных."""
    global tokens_db
    try:
        if not os.path.exists(TRACKER_DB_PATH):
            logger.info("SQLite база не найдена")
            return
            
        conn = sqlite3.connect(TRACKER_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM tokens')
        rows = cursor.fetchall()
        
        # Получаем названия колонок
        column_names = [description[0] for description in cursor.description]
        
        for row in rows:
            # Конвертируем строку в словарь
            row_dict = dict(zip(column_names, row))
            contract = row_dict['contract']
            
            # Десериализуем channel_times из JSON
            channel_times = json.loads(row_dict['channel_times']) if row_dict['channel_times'] else {}
            
            # Конвертируем channels обратно в список
            channels = row_dict['channels'].split(', ') if row_dict['channels'] else []
            
            # Сохраняем полный формат времени
            first_seen = row_dict['first_seen']
            # Если формат неполный, добавляем текущую дату
            if first_seen and len(first_seen) == 8:  # Только время HH:MM:SS
                current_date = datetime.now().strftime("%Y-%m-%d")
                first_seen = f"{current_date} {first_seen}"
            elif not first_seen:
                first_seen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Формируем данные для tokens_db
            tokens_db[contract] = {
                'channels': channels,
                'channel_times': channel_times,
                'channel_count': row_dict['channel_count'],
                'first_seen': first_seen,
                'signal_reached_time': row_dict.get('signal_reached_time'),
                'time_to_threshold': row_dict.get('time_to_threshold'),
                'message_sent': bool(row_dict['message_sent']),
                'message_id': row_dict['message_id'],
                'emojis': row_dict['emojis']
            }
        
        conn.close()
        logger.info(f"Загружено {len(tokens_db)} ВСЕХ токенов из SQLite базы данных")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке всех токенов из SQLite: {e}")
        import traceback
        logger.error(traceback.format_exc())

TOKEN_LIFETIME_MINUTES = 2880  # через 2 дня токен удаляется из базы

def cleanup_old_tokens():
    global tokens_db
    try:
        logger.info("🧹 Запуск cleanup_old_tokens")
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=TOKEN_LIFETIME_MINUTES)
        cutoff_time_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"⏰ Удаляем Tokens старше: {cutoff_time_str}")
        
        conn = sqlite3.connect(TRACKER_DB_PATH)
        cursor = conn.cursor()
        
        # Сначала считаем (удаляем только неотправленные токены)
        cursor.execute('SELECT COUNT(*) FROM tokens WHERE first_seen < ? AND message_sent = 0', 
                      (cutoff_time_str,))
        to_delete = cursor.fetchone()[0]
        logger.info(f"📊 Найдено {to_delete} неотправленных токенов для удаления")
        
        cursor.execute('''
        DELETE FROM tokens 
        WHERE first_seen < ? 
        AND message_sent = 0
        ''', (cutoff_time_str,))
        
        deleted = cursor.rowcount
        
        # Логируем статистику отправленных токенов (которые НЕ удаляем)
        cursor.execute('SELECT COUNT(*) FROM tokens WHERE first_seen < ? AND message_sent = 1', 
                      (cutoff_time_str,))
        sent_tokens_count = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Удалено {deleted} неотправленных токенов")
        logger.info(f"💌 Сохранено {sent_tokens_count} отправленных токенов (защищены от удаления)")

        if deleted > 0:
            # Удаляем из памяти тоже
            cutoff_time_obj = current_time - timedelta(minutes=TOKEN_LIFETIME_MINUTES)
            
            tokens_to_remove = []
            for contract, data in tokens_db.items():
                try:
                    first_seen_str = data.get('first_seen', '')
                    if first_seen_str:
                        first_seen_obj = datetime.strptime(first_seen_str, "%Y-%m-%d %H:%M:%S")
                        channel_count = data.get('channel_count', 0)
                        
                        message_sent = data.get('message_sent', False)
                        if first_seen_obj < cutoff_time_obj and not message_sent:
                            tokens_to_remove.append(contract)
                except:
                    continue
            
            # Удаляем из памяти
            for contract in tokens_to_remove:
                del tokens_db[contract]
            
            logger.info(f"🧹 Удалено из памяти: {len(tokens_to_remove)} токенов")
            
    except Exception as e:
        logger.error(f"❌ Ошибка cleanup_old_tokens: {e}")
        import traceback
        logger.error(traceback.format_exc())

# Функция загрузки базы данных
def load_database():
    global tokens_db
    try:
        # Инициализируем SQLite базу для ВСЕХ токенов
        init_tracker_db()
        
        # Загружаем ВСЕ Tokens из SQLite
        load_tokens_from_db()
        
        # Если tokens_db пустая, создаем новую
        if not tokens_db:
            logger.info("База данных SQLite пустая, создаем новую")
            tokens_db = {}

    except Exception as e:
        logger.error(f"Ошибка при загрузке базы данных: {e}")
        tokens_db = {}

# Функция сохранения базы данных
def save_database():
    try:
        # Сохраняем ВСЕ Tokens в SQLite
        save_tokens_to_db()
    except Exception as e:
        logger.error(f"Ошибка при сохранении базы данных: {e}")

async def main():
    # Явный вывод о запуске программы
    logger.info("Скрипт запущен!")
    
    # Инициализируем базу данных SQLite
    init_tracker_db()
    
    # Загружаем базу данных токенов
    load_database()
    
    # Подключаемся к Telegram с улучшенными параметрами
    client = TelegramClient(
        'test_session', 
        API_ID, 
        API_HASH,
        connection_retries=10,
        retry_delay=5,
        auto_reconnect=True,
        request_retries=10
    )
    
    await client.start()
    
    logger.info("Подключение к Telegram успешно установлено")
      
    # Регистрируем обработчик событий
    @client.on(events.NewMessage(chats=list(SOURCE_CHANNELS.keys())))
    async def handler(event):
        try:
            # Получаем имя канала из нашего словаря
            channel_name = get_channel_name(event.chat_id)
            logger.info(f"Получено новое сообщение из канала {channel_name} (ID: {event.chat_id})")
            
            # Безопасно логируем текст сообщения
            text = getattr(event.message, 'text', None)
            logger.info(f"Текст сообщения: {safe_str(text)}")
            
            # Извлекаем контракты Solana из текста
            contracts = extract_solana_contracts(text)
            
            if contracts:
                logger.info(f"Найдены контракты: {contracts}")
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                for contract in contracts:
                    logger.info(f"Обрабатываем Contract: {contract}")
                    
                    # Проверяем, существует ли уже этот токен в базе
                    if contract in tokens_db:
                        # Если этот канал еще не зарегистрирован для этого токена
                        if channel_name not in tokens_db[contract]["channels"]:
                            tokens_db[contract]["channels"].append(channel_name)
                            tokens_db[contract]["channel_times"][channel_name] = current_time
                            tokens_db[contract]["channel_count"] += 1
                            
                            logger.info(f"Токен {contract} появился в новом канале. Всего каналов: {tokens_db[contract]['channel_count']}")
                            
                            # Обновляем эмодзи при каждом новом канале
                            emojis = get_channel_emojis_by_names(tokens_db[contract]["channels"])
                            tokens_db[contract]["emojis"] = emojis
                            logger.info(f"Обновлены эмодзи для токена {contract}: {emojis}")
                                                        
                            # Если токен набрал нужное количество каналов и сообщение еще не отправлено в RadarDexBot
                            
                            if tokens_db[contract]["channel_count"] >= MIN_SIGNALS and not tokens_db[contract]["message_sent"]:
                                # Рассчитываем время до достижения порога
                                first_seen_time = datetime.strptime(tokens_db[contract]["first_seen"], "%Y-%m-%d %H:%M:%S")
                                current_datetime = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
                                time_diff = current_datetime - first_seen_time
    
                                # Простой расчет в минутах
                                total_minutes = int(time_diff.total_seconds() / 60)
                                time_to_threshold = f"{total_minutes} min"
    
                                # Сохраняем время достижения порога
                                tokens_db[contract]["signal_reached_time"] = current_time
                                tokens_db[contract]["time_to_threshold"] = time_to_threshold
    
                                # Отправляем номер контракта с эмодзи в RadarDexBot
                                try:
                                    sent_message = await client.send_message(
                                        TARGET_BOT,
                                        f"Contract: {contract}\n{emojis} ({time_to_threshold})"  # ← ИСПРАВЛЕНО
                                    )
                                    tokens_db[contract]["message_sent"] = True
                                    tokens_db[contract]["message_id"] = sent_message.id
                                    tokens_db[contract]["emojis"] = emojis
                                    logger.info(f"Номер контракта {contract} с эмодзи {emojis} отправлен боту {TARGET_BOT}, ID сообщения: {sent_message.id}")
        
                                except Exception as e:
                                    logger.error(f"Ошибка при отправке номера контракта: {e}")
                            
                            # Сохраняем базу данных после обновления
                            save_database()
                    else:
                        # Создаем новую запись о токене
                        tokens_db[contract] = {
                            "channels": [channel_name],
                            "channel_times": {channel_name: current_time},
                            "channel_count": 1,
                            "first_seen": current_time,
                            "signal_reached_time": None,
                            "time_to_threshold": None,
                            "message_sent": False,
                            "message_id": 0,
                            "emojis": ""
                        }
                        
                        logger.info(f"Новый токен {contract} добавлен. Обнаружен в 1 из {MIN_SIGNALS} необходимых каналов")
                        
                        # Сохраняем базу данных после добавления нового токена
                        save_database()
            else:
                logger.info("Контракты Solana в сообщении не найдены")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Запускаем периодическое сохранение базы данных
    async def periodic_save():
        while True:
            try:
                logger.info("🔄 Начинаем periodic_save cycle")
                await asyncio.sleep(300)  # 5 минут
                save_database()
                logger.info("💾 save_database() выполнена")
                cleanup_old_tokens()  # Очистка каждые 5 минут
                logger.info("🧹 cleanup_old_tokens() выполнена")
                    
            except Exception as e:
                logger.error(f"❌ Error in periodic_save: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)
    
    # Запускаем фоновую задачу сохранения
    asyncio.ensure_future(periodic_save())
    
    logger.info(f"Бот запущен и отслеживает каналы: {len(SOURCE_CHANNELS)} шт. MIN_SIGNALS={MIN_SIGNALS}")
    
    # Держим соединение активным
    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Error in основном цикле: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Сохраняем базу данных перед выходом
        save_database()
        await client.disconnect()
        logger.info("Соединение закрыто")

if __name__ == "__main__":
   # Обработчик сигнала прерывания
   def signal_handler(sig, frame):
       logger.info("Получен сигнал прерывания, выполняется выход...")
       print("\n[INFO] Завершение работы системы...")
       sys.exit(0)
   
   # Регистрируем обработчик сигнала прерывания
   signal.signal(signal.SIGINT, signal_handler)
   
   # Запускаем бота
   asyncio.run(main())