import sqlite3
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class UserDatabase:
    """Управление пользователями в tokens_tracker_database.db"""
    
    def __init__(self, db_path: str = "tokens_tracker_database.db"):
        self.db_path = db_path
        self.init_users_table()
        self.init_potential_users_table()
    
    # Добавить в user_database.py в класс UserDatabase

    def init_potential_users_table(self):
        """Создает таблицу потенциальных пользователей"""
        try:
            logger.info("🔧 НАЧАЛО создания таблицы potential_users")
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
            logger.info("Таблица potential_users создана")
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы potential_users: {e}")

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
            logger.info(f"Потенциальный пользователь {user_id} добавлен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления потенциального пользователя {user_id}: {e}")
            return False

    def get_potential_users(self) -> List[Dict[str, Any]]:
        """Получает список потенциальных пользователей (которые НЕ авторизованы)"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Выбираем тех, кто есть в potential_users, но НЕТ в users (или неактивен)
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
            logger.error(f"Ошибка получения потенциальных пользователей: {e}")
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
                logger.info(f"Потенциальный пользователь {user_id} удален")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Ошибка удаления потенциального пользователя {user_id}: {e}")
            return False

    def init_users_table(self):
        """Создает таблицу пользователей в существующей базе tracker'а"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу пользователей
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
            logger.info("Таблица пользователей создана в tokens_tracker_database.db")
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы пользователей: {e}")
    
    def is_user_authorized(self, user_id: int) -> bool:
        """Проверяет авторизацию пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT is_active FROM users WHERE user_id = ? AND is_active = 1', (user_id,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя {user_id}: {e}")
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
            logger.info(f"Пользователь {user_id} добавлен")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int) -> bool:
        """Удаляет пользователя"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Сначала проверяем, есть ли пользователь
            cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
            existing_user = cursor.fetchone()
            
            if not existing_user:
                logger.warning(f"User {user_id} not found in database")
                conn.close()
                return False
            
            # Удаляем пользователя
            cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
            conn.commit()
            
            # Проверяем, что удаление прошло успешно
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
            
            # Проверяем, существует ли пользователь
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
            
            # Проверяем, существует ли пользователь
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
            logger.error(f"Ошибка получения пользователей: {e}")
            return []



# Глобальный экземпляр
user_db = UserDatabase()