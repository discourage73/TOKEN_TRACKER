import sqlite3
import json
import time
import logging
import os
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime


import contextlib
import sqlite3
from typing import Generator, Any

logger = logging.getLogger(__name__)

# Путь к файлу базы данных
DB_PATH = "tokens_database.db"


@contextlib.contextmanager
def db_connection() -> Generator[sqlite3.Connection, None, None]:
    """
    Контекстный менеджер для работы с подключением к базе данных.
    
    Yields:
        Подключение к базе данных
    """
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при работе с базой данных: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        conn.close()


def get_db_connection() -> sqlite3.Connection:
    """
    Создает подключение к базе данных SQLite.
    
    Returns:
        Подключение к базе данных
    """
    # Проверяем, существует ли файл БД
    db_exists = os.path.exists(DB_PATH)
    
    # Создаем подключение
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Для получения результатов в виде словаря
    
    # Если база не существовала, инициализируем таблицы
    if not db_exists:
        init_database(conn)
    
    return conn

def delete_all_tokens() -> int:
    """
    Удаляет все токены из базы данных одним запросом.
    Возвращает количество удаленных токенов.
    """
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            
            # Сначала считаем количество токенов
            cursor.execute('SELECT COUNT(*) FROM tokens')
            token_count = cursor.fetchone()[0]
            
            if token_count == 0:
                return 0
            
            # Удаляем все связанные данные (порядок важен из-за foreign keys)
            cursor.execute('DELETE FROM market_caps')
            cursor.execute('DELETE FROM ath_data') 
            cursor.execute('DELETE FROM token_data')
            cursor.execute('DELETE FROM tokens')
            
            logger.info(f"Удалено {token_count} токенов из базы данных")
            return token_count
            
    except Exception as e:
        logger.error(f"Ошибка при массовом удалении токенов: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0

def init_database(conn: sqlite3.Connection) -> None:
    """
    Инициализирует структуру базы данных.
    
    Args:
        conn: Подключение к базе данных
    """
    cursor = conn.cursor()
    
    # Таблица с основной информацией о токенах (БЕЗ поля is_hidden)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT UNIQUE,
        ticker TEXT,
        ticker_address TEXT,
        added_time REAL,
        last_update_time REAL,
        chat_id INTEGER,
        message_id INTEGER
    )
    ''')
    
    # Таблица с данными о маркет капе
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS market_caps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id INTEGER,
        timestamp REAL,
        market_cap REAL,
        price_usd REAL,
        volume_5m TEXT,
        volume_1h TEXT,
        FOREIGN KEY (token_id) REFERENCES tokens(id)
    )
    ''')
    
    # Таблица с ATH данными
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ath_data (
        token_id INTEGER PRIMARY KEY,
        ath_market_cap REAL,
        ath_time REAL,
        last_alert_multiplier REAL DEFAULT 1,
        FOREIGN KEY (token_id) REFERENCES tokens(id)
    )
    ''')
    
    # Таблица для хранения произвольных JSON данных
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS token_data (
        token_id INTEGER PRIMARY KEY,
        token_info TEXT,  -- JSON
        initial_data TEXT,  -- JSON
        raw_api_data TEXT,  -- JSON
        FOREIGN KEY (token_id) REFERENCES tokens(id)
    )
    ''')
    
    conn.commit()
    
    # Создаем индексы для оптимизации
    create_indexes(conn)
    
    logger.info("База данных SQLite инициализирована с индексами")

def create_indexes(conn: sqlite3.Connection) -> None:
    """
    Создает индексы для оптимизации производительности SQL запросов.
    """
    cursor = conn.cursor()
    
    try:
        # Индекс по времени добавления (для фильтрации активных токенов)
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_added_time 
        ON tokens(added_time)
        ''')
        
        # Индекс по времени последнего обновления (для мониторинга)
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_last_update 
        ON tokens(last_update_time)
        ''')
        
        # Индекс по ATH времени для анализа максимумов
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ath_time 
        ON ath_data(ath_time)
        ''')
        
        # Индекс по маркет капу для сортировки и анализа
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ath_market_cap 
        ON ath_data(ath_market_cap)
        ''')
        
        # Индекс по времени в market_caps для временных запросов
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_market_caps_timestamp 
        ON market_caps(timestamp)
        ''')
        
        # Индекс по token_id в связанных таблицах для JOIN операций
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_market_caps_token_id 
        ON market_caps(token_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ath_data_token_id 
        ON ath_data(token_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_token_data_token_id 
        ON token_data(token_id)
        ''')
        
        conn.commit()
        logger.info("Все индексы успешно созданы")
        
    except Exception as e:
        logger.error(f"Ошибка при создании индексов: {str(e)}")
        conn.rollback()
        raise

def store_token_data(query: str, data: Dict[str, Any]) -> None:
    """
    Сохраняет данные о токене в базу данных.
    
    Args:
        query: Запрос токена
        data: Данные о токене
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Добавляем время добавления токена, если его нет
        if 'added_time' not in data:
            data['added_time'] = time.time()
        
        # Получаем базовую информацию для таблицы tokens
        token_info = data.get('token_info', {})
        ticker = token_info.get('ticker', 'Неизвестно')
        ticker_address = token_info.get('ticker_address', 'Неизвестно')
        added_time = data.get('added_time')
        last_update_time = data.get('last_update_time', time.time())
        chat_id = data.get('chat_id', 0)
        message_id = data.get('message_id', 0)
        
        # Вставляем или обновляем запись в таблице tokens (БЕЗ is_hidden)
        cursor.execute('''
        INSERT INTO tokens (query, ticker, ticker_address, added_time, last_update_time, chat_id, message_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(query) DO UPDATE SET
            ticker = excluded.ticker,
            ticker_address = excluded.ticker_address,
            last_update_time = excluded.last_update_time,
            chat_id = excluded.chat_id,
            message_id = excluded.message_id
        ''', (query, ticker, ticker_address, added_time, last_update_time, chat_id, message_id))
        
        # Получаем ID токена
        cursor.execute('SELECT id FROM tokens WHERE query = ?', (query,))
        token_id = cursor.fetchone()['id']
        
        # Сохраняем рыночные данные
        if 'token_info' in data and 'raw_market_cap' in token_info:
            market_cap = token_info.get('raw_market_cap', 0)
            price_usd = token_info.get('price_usd', 0)
            volume_5m = token_info.get('volume_5m', 'Неизвестно')
            volume_1h = token_info.get('volume_1h', 'Неизвестно')
            
            cursor.execute('''
            INSERT INTO market_caps (token_id, timestamp, market_cap, price_usd, volume_5m, volume_1h)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (token_id, time.time(), market_cap, price_usd, volume_5m, volume_1h))
        
        # Обновляем или вставляем ATH данные
        ath_market_cap = data.get('ath_market_cap', 0)
        ath_time = data.get('ath_time', time.time())
        last_alert_multiplier = data.get('last_alert_multiplier', 1)
        
        cursor.execute('''
        INSERT INTO ath_data (token_id, ath_market_cap, ath_time, last_alert_multiplier)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(token_id) DO UPDATE SET
            ath_market_cap = excluded.ath_market_cap,
            ath_time = excluded.ath_time,
            last_alert_multiplier = excluded.last_alert_multiplier
        ''', (token_id, ath_market_cap, ath_time, last_alert_multiplier))
        
        # Сохраняем JSON данные
        token_info_json = json.dumps(token_info, ensure_ascii=False) if token_info else None
        initial_data_json = json.dumps(data.get('initial_data', {}), ensure_ascii=False) if 'initial_data' in data else None
        raw_api_data_json = json.dumps(data.get('raw_api_data', {}), ensure_ascii=False) if 'raw_api_data' in data else None
        
        cursor.execute('''
        INSERT INTO token_data (token_id, token_info, initial_data, raw_api_data)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(token_id) DO UPDATE SET
            token_info = excluded.token_info,
            initial_data = excluded.initial_data,
            raw_api_data = excluded.raw_api_data
        ''', (token_id, token_info_json, initial_data_json, raw_api_data_json))
        
        conn.commit()
        logger.debug(f"Данные о токене '{query}' сохранены в базе данных")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в базу данных: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if 'conn' in locals():
            conn.close()

def get_token_data(query: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные о токене из базы данных.
    
    Args:
        query: Запрос токена
        
    Returns:
        Данные о токене или None, если токен не найден
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем базовую информацию о токене
        cursor.execute('''
        SELECT t.*, a.ath_market_cap, a.ath_time, a.last_alert_multiplier
        FROM tokens t
        LEFT JOIN ath_data a ON t.id = a.token_id
        WHERE t.query = ?
        ''', (query,))
        
        token_row = cursor.fetchone()
        
        if not token_row:
            logger.info(f"Данные о токене '{query}' не найдены в базе данных")
            return None
        
        # Преобразуем Row в dict
        token_data = dict(token_row)
        token_id = token_data['id']
        
        # Получаем JSON данные
        cursor.execute('''
        SELECT token_info, initial_data, raw_api_data
        FROM token_data
        WHERE token_id = ?
        ''', (token_id,))
        
        json_row = cursor.fetchone()
        
        if json_row:
            if json_row['token_info']:
                token_data['token_info'] = json.loads(json_row['token_info'])
            if json_row['initial_data']:
                token_data['initial_data'] = json.loads(json_row['initial_data'])
            if json_row['raw_api_data']:
                token_data['raw_api_data'] = json.loads(json_row['raw_api_data'])
        
        # Получаем последний маркет кап
        cursor.execute('''
        SELECT * FROM market_caps
        WHERE token_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        ''', (token_id,))
        
        market_cap_row = cursor.fetchone()
        
        if market_cap_row:
            # Если market_cap есть в token_info, но не в json_data, добавляем
            if 'token_info' not in token_data:
                token_data['token_info'] = {}
            
            token_data['token_info']['raw_market_cap'] = market_cap_row['market_cap']
            token_data['token_info']['price_usd'] = market_cap_row['price_usd']
            token_data['token_info']['volume_5m'] = market_cap_row['volume_5m']
            token_data['token_info']['volume_1h'] = market_cap_row['volume_1h']
        
        logger.debug(f"Данные о токене '{query}' получены из базы данных")
        return token_data
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных из базы данных: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def get_all_tokens() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает словарь со всеми отслеживаемыми токенами.
    
    Returns:
        Словарь с данными токенов
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Простой запрос без фильтрации по is_hidden
        sql = 'SELECT t.query FROM tokens t'
        
        cursor.execute(sql)
        token_rows = cursor.fetchall()
        
        # Для каждого токена получаем полные данные
        tokens_data = {}
        for row in token_rows:
            query = row['query']
            token_data = get_token_data(query)
            if token_data:
                tokens_data[query] = token_data
        
        return tokens_data
        
    except Exception as e:
        logger.error(f"Ошибка при получении всех токенов: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    finally:
        if 'conn' in locals():
            conn.close()

def update_token_field(query: str, field: str, value: Any) -> bool:
    """
    Обновляет значение поля в данных о токене.
    
    Args:
        query: Запрос токена
        field: Имя поля
        value: Новое значение
        
    Returns:
        True при успешном обновлении, иначе False
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем ID токена
        cursor.execute('SELECT id FROM tokens WHERE query = ?', (query,))
        row = cursor.fetchone()
        
        if not row:
            logger.warning(f"Не удалось обновить поле '{field}' для токена '{query}': токен не найден")
            return False
        
        token_id = row['id']
        
        # Определяем таблицу и колонку для обновления
        if field in ['ticker', 'ticker_address', 'chat_id', 'message_id', 'last_update_time']:
            cursor.execute(f'UPDATE tokens SET {field} = ? WHERE id = ?', (value, token_id))
            
        elif field in ['ath_market_cap', 'ath_time', 'last_alert_multiplier']:
            cursor.execute(f'''
            INSERT INTO ath_data (token_id, {field})
            VALUES (?, ?)
            ON CONFLICT(token_id) DO UPDATE SET
                {field} = excluded.{field}
            ''', (token_id, value))
            
        else:
            # Получаем текущие JSON данные
            cursor.execute('SELECT token_info, initial_data, raw_api_data FROM token_data WHERE token_id = ?', (token_id,))
            json_row = cursor.fetchone()
            
            if not json_row:
                logger.warning(f"Не удалось обновить поле '{field}' для токена '{query}': данные не найдены")
                return False
            
            # Определяем, к какому JSON-полю относится field
            # Например, 'token_info.raw_market_cap'
            if '.' in field:
                json_field, sub_field = field.split('.', 1)
                json_data = json.loads(json_row[json_field]) if json_row[json_field] else {}
                json_data[sub_field] = value
                cursor.execute(f'UPDATE token_data SET {json_field} = ? WHERE token_id = ?', 
                              (json.dumps(json_data, ensure_ascii=False), token_id))
            else:
                logger.warning(f"Не удалось обновить поле '{field}': неизвестное поле")
                return False
        
        conn.commit()
        logger.debug(f"Поле '{field}' для токена '{query}' обновлено на значение '{value}'")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении поля: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def delete_token(query: str) -> bool:
    """
    Удаляет токен из базы данных.
    
    Args:
        query: Запрос токена
        
    Returns:
        True при успешном удалении, иначе False
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем ID токена
        cursor.execute('SELECT id FROM tokens WHERE query = ?', (query,))
        row = cursor.fetchone()
        
        if not row:
            logger.warning(f"Не удалось удалить токен '{query}': токен не найден")
            return False
        
        token_id = row['id']
        
        # Удаляем все связанные данные
        cursor.execute('DELETE FROM market_caps WHERE token_id = ?', (token_id,))
        cursor.execute('DELETE FROM ath_data WHERE token_id = ?', (token_id,))
        cursor.execute('DELETE FROM token_data WHERE token_id = ?', (token_id,))
        cursor.execute('DELETE FROM tokens WHERE id = ?', (token_id,))
        
        conn.commit()
        logger.info(f"Токен '{query}' удален из базы данных")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при удалении токена: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def update_token_ath(query: str, current_mcap: float) -> bool:
    """
    Обновляет ATH маркет капа токена, если текущее значение выше.
    
    Args:
        query: Запрос токена
        current_mcap: Текущий маркет кап
        
    Returns:
        True, если ATH обновлен, иначе False
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем ID токена и текущий ATH
        cursor.execute('''
        SELECT t.id, a.ath_market_cap
        FROM tokens t
        LEFT JOIN ath_data a ON t.id = a.token_id
        WHERE t.query = ?
        ''', (query,))
        
        row = cursor.fetchone()
        
        if not row:
            logger.warning(f"Не удалось обновить ATH для токена '{query}': токен не найден")
            return False
        
        token_id = row['id']
        current_ath = row['ath_market_cap'] or 0
        
        # Если текущий маркет кап выше ATH, обновляем
        if current_mcap > current_ath:
            cursor.execute('''
            INSERT INTO ath_data (token_id, ath_market_cap, ath_time)
            VALUES (?, ?, ?)
            ON CONFLICT(token_id) DO UPDATE SET
                ath_market_cap = excluded.ath_market_cap,
                ath_time = excluded.ath_time
            ''', (token_id, current_mcap, time.time()))
            
            conn.commit()
            logger.debug(f"Обновлен ATH для токена '{query}': {current_mcap}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении ATH: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()