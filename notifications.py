import logging
import asyncio
import time
from typing import Dict, Any, List, Optional, Set
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from handlers.auth_middleware import user_required, admin_required
from telegram import Bot
logger = logging.getLogger(__name__)

async def send_message_with_retry(bot: Bot, chat_id: int, text: str, max_retries: int = 3, **kwargs):
    """
    Отправляет message с повторными попытками при таймауте.
    
    Args:
        bot: Telegram Bot объект
        chat_id: ID чата
        text: Текст messages
        max_retries: Максимальное count попыток
        **kwargs: Дополнительные параметры для send_message
    
    Returns:
        Message объект или None при неудаче
    """
    for attempt in range(max_retries):
        try:
            # Увеличиваем таймаут для каждой попытки
            timeout = 15 + (attempt * 10)  # 15s, 25s, 35s
            
            message = await bot.send_message(
                chat_id=chat_id,
                text=text,
                read_timeout=timeout,
                write_timeout=timeout,
                connect_timeout=timeout,
                **kwargs
            )
            
            if attempt > 0:
                logger.info(f"✅ Message sent to {chat_id} on attempt {attempt + 1}")
            
            return message
            
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Timeout sending to {chat_id}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка: 1s, 2s, 4s
                continue
            else:
                logger.error(f"❌ Failed to send message to {chat_id} after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"❌ Error sending message to {chat_id} on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
                continue
            else:
                raise
    
    return None

# Класс для управления уведомлениями
class NotificationManager:
    def __init__(self, batch_interval: int = 60):
        """
        Инициализирует менеджер уведомлений.
        
        Args:
            batch_interval: Интервал в секундах для батчинга уведомлений
        """
        self.batch_interval = batch_interval
        self.pending_notifications: Dict[int, List[Dict[str, Any]]] = {}  # chat_id -> notifications
        self.last_batch_time: Dict[int, float] = {}  # chat_id -> timestamp
        self.running_tasks: Set[asyncio.Task] = set()
        logger.info(f"Менеджер уведомлений Initialized с интервалом {batch_interval} секунд")
    
    def add_notification(self, chat_id: int, notification: Dict[str, Any]) -> None:
        """
        Добавляет уведомление в очередь.
        
        Args:
            chat_id: ID чата
            notification: data уведомления
        """
        if chat_id not in self.pending_notifications:
            self.pending_notifications[chat_id] = []
        
        self.pending_notifications[chat_id].append(notification)
        logger.debug(f"Добавлено уведомление for chat {chat_id}, total: {len(self.pending_notifications[chat_id])}")
        
        # Checking, нужно ли запустить отправку
        self._check_and_schedule_sending(chat_id)
    
    def _check_and_schedule_sending(self, chat_id: int) -> None:
        """
        Проверяет условия и планирует отправку уведомлений.
        
        Args:
            chat_id: ID чата
        """
        current_time = time.time()
        
        # Если для этого чата уже есть запланированная задача, не Creating новую
        if any(task for task in self.running_tasks if getattr(task, 'chat_id', None) == chat_id):
            return
        
        # Checking, прошло ли достаточно времени с последней отправки
        last_time = self.last_batch_time.get(chat_id, 0)
        
        if current_time - last_time >= self.batch_interval:
            # Creating и Starting задачу отправки
            task = asyncio.create_task(self._send_notifications_batch(chat_id))
            # Saving chat_id как атрибут задачи для идентификации
            setattr(task, 'chat_id', chat_id)
            self.running_tasks.add(task)
            # Adding обработчик завершения
            task.add_done_callback(self._task_done_callback)
    
    def _task_done_callback(self, task: asyncio.Task) -> None:
        """
        Обработчик завершения задачи отправки уведомлений.
        
        Args:
            task: Задача
        """
        self.running_tasks.discard(task)
    
    async def _send_notifications_batch(self, chat_id: int) -> None:
        """
        Отправляет накопленные уведомления for chat.
        
        Args:
            chat_id: ID чата
        """
        try:
            from token_service import get_telegram_context
            context = get_telegram_context()
            
            if not context:
                logger.error(f"failed to получить контекст Telegram for chat {chat_id}")
                return
            
            # Getting уведомления for chat
            notifications = self.pending_notifications.get(chat_id, [])
            
            if not notifications:
                return
            
            # Очищаем список уведомлений
            self.pending_notifications[chat_id] = []
            
            # Updating time последней отправки
            self.last_batch_time[chat_id] = time.time()
            
            # Формируем и Sending агрегированное message
            message = self._format_aggregate_message(notifications)
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            
            logger.info(f"Отправлено {len(notifications)} агрегированных уведомлений for chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error при отправке уведомлений for chat {chat_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _format_aggregate_message(self, notifications: List[Dict[str, Any]]) -> str:
        """
        Форматирует агрегированное message из уведомлений.
        
        Args:
            notifications: Список уведомлений
            
        Returns:
            Отформатированное message
        """
        # Если уведомление только одно, используем его текст
        if len(notifications) == 1:
            return notifications[0].get('text', 'Уведомление')
        
        # Формируем агрегированное message
        message = f"📊 *Обновление по {len(notifications)} токенам:*\n\n"
        
        for i, notification in enumerate(notifications, start=1):
            notification_type = notification.get('type', 'update')
            token_name = notification.get('token_name', 'Unknown')
            market_cap = notification.get('market_cap', 'Unknown')
            multiplier = notification.get('multiplier', 1)
            
            if notification_type == 'growth':
                message += f"{i}. 🔥 *{token_name}* вырос в {multiplier}x! MC: {market_cap}\n"
            else:
                message += f"{i}. ℹ️ *{token_name}*: MC: {market_cap}\n"
        
        return message

# Creating глобальный экземпляр менеджера уведомлений
notification_manager = NotificationManager()

async def send_growth_notification_to_user(
    user_id: int,
    token_name: str,
    multiplier: int, 
    market_cap: str,
    token_message_id: int,
    contract_address: str
) -> bool:
    """
    НОВАЯ ФУНКЦИЯ: Sends notification о росте конкретному пользователю.
    Используется батчингом для персонализированных уведомлений.
    """
    try:
        from token_service import get_telegram_context
        context = get_telegram_context()
        
        if not context:
            logger.error("Could not get Telegram context for growth notification")
            return False
        
        # Формируем message
        fire_emojis = "🔥" * min(int(multiplier), 10)  # ИСПРАВЛЕНО: преобразуем в int
        growth_message = (
            f"{fire_emojis}\n"
            f"*{token_name}* *{int(multiplier)}x* from call!\n\n"  # ИСПРАВЛЕНО: преобразуем в int
            f"MCap: {market_cap}"
        )
        
        # ИСПРАВЛЕНИЕ: Getting data из новой системы user_token_messages
        import sqlite3
        
        try:
            conn = sqlite3.connect("tokens_tracker_database.db")
            cursor = conn.cursor()
            
            cursor.execute('SELECT growth_message_id, current_multiplier FROM user_token_messages WHERE token_query = ? AND user_id = ?', 
                          (contract_address, user_id))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                old_growth_message_id, current_multiplier = result
            else:
                old_growth_message_id, current_multiplier = None, 1
                
        except Exception as e:
            logger.error(f"Error получения growth_message_id: {e}")
            old_growth_message_id, current_multiplier = None, 1
        
        # Deleting старое message о росте (если существует)
        if old_growth_message_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=old_growth_message_id)
                logger.debug(f"Deleted old growth message {old_growth_message_id} for user {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete old growth message {old_growth_message_id}: {e}")
        
        # Sending новое уведомление как reply на message о токене
        try:
            new_growth_msg = await send_message_with_retry(
                context.bot,
                chat_id=user_id,
                text=growth_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_to_message_id=token_message_id
            )
            
            # ИСПРАВЛЕНИЕ: Saving новый growth_message_id в нашу систему
            try:
                conn = sqlite3.connect("tokens_tracker_database.db")
                cursor = conn.cursor()
                
                cursor.execute('''UPDATE user_token_messages 
                                 SET growth_message_id = ?, current_multiplier = ?, growth_updated_at = datetime('now', 'localtime')
                                 WHERE token_query = ? AND user_id = ?''', 
                              (new_growth_msg.message_id, multiplier, contract_address, user_id))
                conn.commit()
                conn.close()
                
                logger.debug(f"Обновлен growth_message_id={new_growth_msg.message_id} для token {contract_address[:10]}... пользователя {user_id}")
                
            except Exception as db_e:
                logger.error(f"Error обновления growth_message_id: {db_e}")
            
            logger.info(f"✅ Sent growth x{multiplier} to user {user_id} as reply to message {token_message_id}")
            return True
            
        except Exception as e:
            if "Message to be replied not found" in str(e):
                # Fallback: обычное message без reply
                try:
                    new_growth_msg = await send_message_with_retry(
                        context.bot,
                        chat_id=user_id,
                        text=growth_message,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    # ИСПРАВЛЕНИЕ: Saving новый growth_message_id в нашу систему (fallback)
                    try:
                        conn = sqlite3.connect("tokens_tracker_database.db")
                        cursor = conn.cursor()
                        
                        cursor.execute('''UPDATE user_token_messages 
                                         SET growth_message_id = ?, current_multiplier = ?, growth_updated_at = datetime('now', 'localtime')
                                         WHERE token_query = ? AND user_id = ?''', 
                                      (new_growth_msg.message_id, multiplier, contract_address, user_id))
                        conn.commit()
                        conn.close()
                        
                        logger.debug(f"Обновлен growth_message_id={new_growth_msg.message_id} для token {contract_address[:10]}... пользователя {user_id} (fallback)")
                        
                    except Exception as db_e:
                        logger.error(f"Error обновления growth_message_id (fallback): {db_e}")
                    logger.info(f"✅ Sent growth x{multiplier} to user {user_id} (fallback without reply)")
                    return True
                    
                except Exception as fallback_e:
                    logger.error(f"❌ Failed to send growth notification to user {user_id}: {fallback_e}")
                    return False
            else:
                logger.error(f"❌ Failed to send growth notification to user {user_id}: {e}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Error in send_growth_notification_to_user: {e}")
        return False

async def add_growth_notification_with_reply(
    chat_id: int, 
    token_name: str, 
    multiplier: int, 
    market_cap: str,
    reply_to_message_id: Optional[int] = None,
    contract_address: Optional[str] = None
) -> None:
    """
    НОВАЯ ФУНКЦИЯ: system уведомлений с reply и удалением предыдущих сообщений.
    Работает ПАРАЛЛЕЛЬНО с существующей add_growth_notification.
    """
    try:
        from token_service import get_telegram_context
        context = get_telegram_context()
        
        if not context:
            logger.error("Could not get Telegram context for growth notification with reply")
            return
        
        # Определяем token для поиска
        token_query = contract_address if contract_address else token_name
        
        # Формируем message
        fire_emojis = "🔥" * min(int(multiplier), 10)  # ИСПРАВЛЕНО: преобразуем в int
        growth_message = (
            f"{fire_emojis}\n"
            f"*{token_name}* *{int(multiplier)}x* from call!\n\n"  # ИСПРАВЛЕНО: преобразуем в int
            f"MCap: {market_cap}"
        )
        
        # Getting пользователей из новой системы
        from handlers.auth_middleware import get_user_db
        user_db = get_user_db()
        users_for_token = user_db.get_all_users_for_token(token_query)
        
        if not users_for_token:
            logger.warning(f"No users found in new system for {token_query} - skipping notification")
            return
        
        # НОВАЯ ЛОГИКА с reply и удалением
        sent_count = 0
        failed_count = 0
        
        for user_info in users_for_token:
            user_id = user_info['user_id']
            token_message_id = user_info['token_message_id']
            old_growth_message_id = user_info['growth_message_id']
            current_multiplier = user_info.get('current_multiplier', 1)
            
            try:
                # Deleting старое message о росте
                if old_growth_message_id and multiplier > current_multiplier:
                    try:
                        await context.bot.delete_message(chat_id=user_id, message_id=old_growth_message_id)
                        logger.info(f"Deleted old growth message for user {user_id}")
                    except Exception as e:
                        logger.warning(f"Could not delete old growth message: {e}")
                
                # Sending reply на message о токене с retry логикой
                if token_message_id:
                    try:
                        new_growth_msg = await send_message_with_retry(
                            context.bot,
                            chat_id=user_id,
                            text=growth_message,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_to_message_id=token_message_id
                        )
                        
                        # Saving новый growth_message_id
                        user_db.update_user_growth_message(token_query, user_id, new_growth_msg.message_id, multiplier)
                        
                        sent_count += 1
                        logger.info(f"Sent growth x{multiplier} to user {user_id} as reply")
                        
                    except Exception as e:
                        if "Message to be replied not found" in str(e):
                            # Fallback: обычное message
                            try:
                                new_growth_msg = await send_message_with_retry(
                                    context.bot,
                                    chat_id=user_id,
                                    text=growth_message,
                                    parse_mode=ParseMode.MARKDOWN
                                )
                                user_db.update_user_growth_message(token_query, user_id, new_growth_msg.message_id, multiplier)
                                sent_count += 1
                                logger.info(f"Sent growth x{multiplier} to user {user_id} as regular message (fallback)")
                            except Exception as fallback_error:
                                logger.error(f"Fallback failed for user {user_id}: {fallback_error}")
                                failed_count += 1
                        else:
                            raise e
                else:
                    # Нет token_message_id - обычное message с retry
                    new_growth_msg = await send_message_with_retry(
                        context.bot,
                        chat_id=user_id,
                        text=growth_message,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    user_db.update_user_growth_message(token_query, user_id, new_growth_msg.message_id, multiplier)
                    sent_count += 1
                    
            except Exception as e:
                logger.error(f"Error sending growth notification to user {user_id}: {e}")
                failed_count += 1
        
        logger.info(f"Growth notification with reply for {token_name} x{multiplier}: sent {sent_count}, failed {failed_count}")
        
    except Exception as e:
        logger.error(f"Error in add_growth_notification_with_reply: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def cleanup_user_token_messages_task() -> None:
    """НОВАЯ ФУНКЦИЯ: Задача для периодической очистки старых записей"""
    try:
        from handlers.auth_middleware import get_user_db
        user_db = get_user_db()
        
        deleted_count = user_db.cleanup_old_user_messages(days_old=14)
        
        if deleted_count > 0:
            logger.info(f"Cleanup task: removed {deleted_count} old user_token_messages records")
        else:
            logger.debug("Cleanup task: no old records to remove")
            
    except Exception as e:
        logger.error(f"Error in cleanup_user_token_messages_task: {e}")
        import traceback
        logger.error(traceback.format_exc())