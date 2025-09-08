import sqlite3
import logging
from typing import List, Dict, Any, Optional, Tuple 

logger = logging.getLogger(__name__)

class UserDatabase:
    """table в tokens_tracker_database.db"""
    
    def __init__(self, db_path: str = "tokens_tracker_database.db"):
        self.db_path = db_path
        self.init_users_table()
        self.init_potential_users_table()
        self.init_user_token_messages_table()
    
# table потенциальных users. Те кто нажали старт ,появляются в функции добавить пользователя
#    
    def init_potential_users_table(self):
        """Creates table потенциальных пользователей"""
        try:
            logger.info("🔧 НАЧАЛО создания table potential_users")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            logger.info("🔧 Выполняю CREATE TABLE...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS potential_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    first_contact TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("table potential_users создана")
            
        except Exception as e:
            logger.error(f"Error создания table potential_users: {e}")

    def add_potential_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None) -> bool:
        """Добавляет потенциального пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO potential_users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name))
            
            conn.commit()
            conn.close()
            logger.info(f"Потенциальный user {user_id} добавлен")
            return True
            
        except Exception as e:
            logger.error(f"Error добавления потенциального пользователя {user_id}: {e}")
            return False

    def get_potential_users(self) -> List[Dict[str, Any]]:
        """Получает список потенциальных пользователей (которые НЕ авторизованы)"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Выбираем тех, кто есть в potential_users, но НЕТ в users (или inactive)
            cursor.execute('''
                SELECT p.* FROM potential_users p
                LEFT JOIN users u ON p.user_id = u.user_id AND u.is_active = 1
                WHERE u.user_id IS NULL
                ORDER BY p.first_contact DESC
            ''')
            
            potential = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return potential
            
        except Exception as e:
            logger.error(f"Error получения потенциальных пользователей: {e}")
            return []

    def remove_potential_user(self, user_id: int) -> bool:
        """Удаляет из потенциальных пользователей (после добавления в основную базу)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM potential_users WHERE user_id = ?', (user_id,))
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"Потенциальный user {user_id} удален")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error удаления потенциального пользователя {user_id}: {e}")
            return False

# table users. Те кого кого добавили в рассылку и работа с ними

    def init_users_table(self):
        """Creates table пользователей в существующей базе tracker'а"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Creating таблицу пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("table пользователей создана в tokens_tracker_database.db")
            
        except Exception as e:
            logger.error(f"Error создания table пользователей: {e}")
    
    def is_user_authorized(self, user_id: int) -> bool:
        """Checks user authorization"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT is_active FROM users WHERE user_id = ? AND is_active = 1', (user_id,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"Error проверки пользователя {user_id}: {e}")
            return False
    
    def add_user(self, user_id: int, username: str = None) -> bool:
        """Добавляет пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, username)
                VALUES (?, ?)
            ''', (user_id, username))
            conn.commit()
            conn.close()
            logger.info(f"user {user_id} добавлен")
            return True
        except Exception as e:
            logger.error(f"Error добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int) -> bool:
        """Удаляет пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Сначала Checking, есть ли user
            cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
            existing_user = cursor.fetchone()
            
            if not existing_user:
                logger.warning(f"User {user_id} not found in database")
                conn.close()
                return False
            
            # Deleting пользователя
            cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
            conn.commit()
            
            # Checking, что удаление прошло Success
            rows_affected = cursor.rowcount
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"User {user_id} successfully removed")
                return True
            else:
                logger.warning(f"Failed to remove user {user_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error removing user {user_id}: {e}")
            return False

    def activate_user(self, user_id: int) -> bool:
        """Активирует пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Checking, существует ли user
            cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
            if not cursor.fetchone():
                logger.warning(f"User {user_id} not found for activation")
                conn.close()
                return False
            
            # Активируем пользователя
            cursor.execute('UPDATE users SET is_active = 1 WHERE user_id = ?', (user_id,))
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"User {user_id} activated successfully")
                return True
            else:
                logger.warning(f"Failed to activate user {user_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error activating user {user_id}: {e}")
            return False

    def deactivate_user(self, user_id: int) -> bool:
        """Деактивирует пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Checking, существует ли user
            cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
            if not cursor.fetchone():
                logger.warning(f"User {user_id} not found for deactivation")
                conn.close()
                return False
            
            # Деактивируем пользователя
            cursor.execute('UPDATE users SET is_active = 0 WHERE user_id = ?', (user_id,))
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"User {user_id} deactivated successfully")
                return True
            else:
                logger.warning(f"Failed to deactivate user {user_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deactivating user {user_id}: {e}")
            return False

    def update_user_status(self, user_id: int, is_active: bool) -> bool:
        """Обновляет статус пользователя (активен/неактивен)"""
        if is_active:
            return self.activate_user(user_id)
        else:
            return self.deactivate_user(user_id)

    def authorize_potential_user(self, user_id: int) -> bool:
        """Авторизует потенциального пользователя (перемещает из potential_users в users)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Получаем данные из potential_users
            cursor.execute('SELECT username, first_name, last_name FROM potential_users WHERE user_id = ?', (user_id,))
            potential_user = cursor.fetchone()
            
            if not potential_user:
                logger.warning(f"Потенциальный user {user_id} not found")
                conn.close()
                return False
            
            username, first_name, last_name = potential_user
            
            # Добавляем в users
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, username, is_active, added_date)
                VALUES (?, ?, 1, datetime('now'))
            ''', (user_id, username))
            
            # Удаляем из potential_users
            cursor.execute('DELETE FROM potential_users WHERE user_id = ?', (user_id,))
            
            conn.commit()
            conn.close()
            
            logger.info(f"User {user_id} успешно авторизован")
            return True
            
        except Exception as e:
            logger.error(f"Error авторизации потенциального user {user_id}: {e}")
            return False

    def get_all_users(self) -> List[Dict[str, Any]]:
        """Получает всех пользователей"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users ORDER BY added_date DESC')
            users = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return users
        except Exception as e:
            logger.error(f"Error получения пользователей: {e}")
            return []
        
# table user_token_messages_table. Присвоещение сообщениям id у каждого пользователя и reply уведомления о росте
    def init_user_token_messages_table(self):
        """НОВАЯ ФУНКЦИЯ: Creates table для связи token-user-message"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
        
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_token_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_query TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    token_message_id INTEGER,
                    growth_message_id INTEGER,
                    current_multiplier INTEGER DEFAULT 1,
                    token_sent_at TIMESTAMP DEFAULT (datetime('now', '+3 hours')),
                    growth_updated_at TIMESTAMP,
                    UNIQUE(token_query, user_id)
                )
            ''')
        
            # Creating индексы
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_token_messages_token_user 
                ON user_token_messages(token_query, user_id)
            ''')
        
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_token_messages_user_id 
                ON user_token_messages(user_id)
            ''')
        
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_token_messages_token_sent_at 
                ON user_token_messages(token_sent_at)
            ''')
        
            conn.commit()
            conn.close()
            logger.info("table user_token_messages создана Success")
        
        except Exception as e:
            logger.error(f"Error создания table user_token_messages: {e}")

    def save_user_token_message(self, token_query: str, user_id: int, message_id: int) -> bool:
        """НОВАЯ ФУНКЦИЯ: Сохраняет ID messages о токене for user"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO user_token_messages 
                (token_query, user_id, token_message_id, token_sent_at)
                VALUES (?, ?, ?, datetime('now', '+3 hours'))
            ''', (token_query, user_id, message_id))
            
            conn.commit()
            conn.close()
            logger.info(f"saved message_id {message_id} for user {user_id}, token {token_query}")
            return True
            
        except Exception as e:
            logger.error(f"Error сохранения user_token_message: {e}")
            return False

    def get_user_token_message(self, token_query: str, user_id: int) -> Optional[int]:
        """НОВАЯ ФУНКЦИЯ: Получает ID messages о токене for user"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT token_message_id FROM user_token_messages 
                WHERE token_query = ? AND user_id = ?
            ''', (token_query, user_id))
            
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"Error получения user_token_message: {e}")
            return None

    def update_user_growth_message(self, token_query: str, user_id: int, growth_message_id: int, multiplier: int) -> bool:
        """НОВАЯ ФУНКЦИЯ: Обновляет ID messages о росте token"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE user_token_messages 
                SET growth_message_id = ?, current_multiplier = ?, growth_updated_at = datetime('now', '+3 hours')
                WHERE token_query = ? AND user_id = ?
            ''', (growth_message_id, multiplier, token_query, user_id))
            
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error обновления user_growth_message: {e}")
            return False

    def get_user_growth_message(self, token_query: str, user_id: int) -> Optional[Tuple[int, int]]:
        """НОВАЯ ФУНКЦИЯ: Получает ID текущего messages о росте и множитель"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT growth_message_id, current_multiplier 
                FROM user_token_messages 
                WHERE token_query = ? AND user_id = ?
            ''', (token_query, user_id))
            
            result = cursor.fetchone()
            conn.close()
            return result if result else None
            
        except Exception as e:
            logger.error(f"Error получения user_growth_message: {e}")
            return None

    def get_all_users_for_token(self, token_query: str) -> List[Dict[str, Any]]:
        """НОВАЯ ФУНКЦИЯ: Получает всех пользователей для token"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT utm.user_id, utm.token_message_id, utm.growth_message_id, 
                    utm.current_multiplier, u.username, u.is_active
                FROM user_token_messages utm
                LEFT JOIN users u ON utm.user_id = u.user_id
                WHERE utm.token_query = ? AND (u.is_active = 1 OR u.is_active IS NULL)
            ''', (token_query,))
            
            results = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error получения пользователей для token: {e}")
            return []

    def cleanup_old_user_messages(self, days_old: int = 14) -> int:
        """НОВАЯ ФУНКЦИЯ: Удаляет старые записи сообщений (автоочистка)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM user_token_messages 
                WHERE token_sent_at < datetime('now', '-' || ? || ' days')
            ''', (days_old,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"Удалено {deleted_count} старых записей user_token_messages")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error очистки старых user_token_messages: {e}")
            return 0
    
    def create_mcap_monitoring_table(self):
        """Creates table mcap_monitoring в tracker DB"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS mcap_monitoring (
                contract TEXT PRIMARY KEY NOT NULL,
                initial_mcap REAL,
                curr_mcap REAL,
                updated_time TEXT DEFAULT (datetime('now', 'localtime')),
                ath_mcap REAL,
                ath_time TEXT,
                last_alert_multiplier REAL DEFAULT 1.0,
                is_active INTEGER DEFAULT 1,
                created_time TEXT DEFAULT (datetime('now', 'localtime')),
                signal_reached_time TEXT DEFAULT (datetime('now', 'localtime'))
            )
            ''')
            
            # Миграция: Adding поле is_active если его нет
            try:
                cursor.execute("PRAGMA table_info(mcap_monitoring)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'is_active' not in columns:
                    logger.info("🔧 Добавляю поле is_active в существующую таблицу")
                    cursor.execute('ALTER TABLE mcap_monitoring ADD COLUMN is_active INTEGER DEFAULT 1')
                    
                    # Updating is_active для существующих записей
                    cursor.execute('''
                        UPDATE mcap_monitoring 
                        SET is_active = CASE 
                            WHEN curr_mcap >= 25000 THEN 1 
                            ELSE 0 
                        END
                    ''')
                    logger.info("✅ Поле is_active добавлено и проинициализировано")
                
                if 'signal_reached_time' not in columns:
                    logger.info("🔧 Добавляю поле signal_reached_time в существующую таблицу")
                    cursor.execute('ALTER TABLE mcap_monitoring ADD COLUMN signal_reached_time TEXT DEFAULT (datetime("now", "localtime"))')
                    
                    # Для существующих записей устанавливаем signal_reached_time = created_time
                    cursor.execute('''
                        UPDATE mcap_monitoring 
                        SET signal_reached_time = created_time 
                        WHERE signal_reached_time IS NULL
                    ''')
                    logger.info("✅ Поле signal_reached_time добавлено и проинициализировано")
                    
            except Exception as migration_error:
                logger.warning(f"⚠️ Error миграции полей: {migration_error}")
            
            conn.commit()
            conn.close()
            logger.info("✅ table mcap_monitoring создана")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error создания table mcap_monitoring: {e}")
            return False
    
    def create_hotboard_table(self):
        """Creates table hotboard в tracker DB"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS hotboard (
                contract TEXT PRIMARY KEY NOT NULL,
                ticker TEXT,
                initial_mcap REAL,
                initial_time TEXT DEFAULT (datetime('now', 'localtime')),
                ath_mcap REAL,
                ath_multiplier REAL,
                created_time TEXT DEFAULT (datetime('now', 'localtime'))
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ table hotboard создана")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error создания table hotboard: {e}")
            return False

# Глобальный экземпляр
user_db = UserDatabase()