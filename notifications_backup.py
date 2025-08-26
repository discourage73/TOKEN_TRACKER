import logging
import asyncio
import time
from typing import Dict, Any, List, Optional, Set
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from handlers.auth_middleware import user_required, admin_required
logger = logging.getLogger(__name__)

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
        logger.info(f"Менеджер уведомлений инициализирован с интервалом {batch_interval} секунд")
    
    def add_notification(self, chat_id: int, notification: Dict[str, Any]) -> None:
        """
        Добавляет уведомление в очередь.
        
        Args:
            chat_id: ID чата
            notification: Данные уведомления
        """
        if chat_id not in self.pending_notifications:
            self.pending_notifications[chat_id] = []
        
        self.pending_notifications[chat_id].append(notification)
        logger.debug(f"Добавлено уведомление для чата {chat_id}, всего: {len(self.pending_notifications[chat_id])}")
        
        # Проверяем, нужно ли запустить отправку
        self._check_and_schedule_sending(chat_id)
    
    def _check_and_schedule_sending(self, chat_id: int) -> None:
        """
        Проверяет условия и планирует отправку уведомлений.
        
        Args:
            chat_id: ID чата
        """
        current_time = time.time()
        
        # Если для этого чата уже есть запланированная задача, не создаем новую
        if any(task for task in self.running_tasks if getattr(task, 'chat_id', None) == chat_id):
            return
        
        # Проверяем, прошло ли достаточно времени с последней отправки
        last_time = self.last_batch_time.get(chat_id, 0)
        
        if current_time - last_time >= self.batch_interval:
            # Создаем и запускаем задачу отправки
            task = asyncio.create_task(self._send_notifications_batch(chat_id))
            # Сохраняем chat_id как атрибут задачи для идентификации
            setattr(task, 'chat_id', chat_id)
            self.running_tasks.add(task)
            # Добавляем обработчик завершения
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
        Отправляет накопленные уведомления для чата.
        
        Args:
            chat_id: ID чата
        """
        try:
            from token_service import get_telegram_context
            context = get_telegram_context()
            
            if not context:
                logger.error(f"Не удалось получить контекст Telegram для чата {chat_id}")
                return
            
            # Получаем уведомления для чата
            notifications = self.pending_notifications.get(chat_id, [])
            
            if not notifications:
                return
            
            # Очищаем список уведомлений
            self.pending_notifications[chat_id] = []
            
            # Обновляем время последней отправки
            self.last_batch_time[chat_id] = time.time()
            
            # Формируем и отправляем агрегированное сообщение
            message = self._format_aggregate_message(notifications)
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            
            logger.info(f"Отправлено {len(notifications)} агрегированных уведомлений для чата {chat_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомлений для чата {chat_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _format_aggregate_message(self, notifications: List[Dict[str, Any]]) -> str:
        """
        Форматирует агрегированное сообщение из уведомлений.
        
        Args:
            notifications: Список уведомлений
            
        Returns:
            Отформатированное сообщение
        """
        # Если уведомление только одно, используем его текст
        if len(notifications) == 1:
            return notifications[0].get('text', 'Уведомление')
        
        # Формируем агрегированное сообщение
        message = f"📊 *Обновление по {len(notifications)} токенам:*\n\n"
        
        for i, notification in enumerate(notifications, start=1):
            notification_type = notification.get('type', 'update')
            token_name = notification.get('token_name', 'Неизвестно')
            market_cap = notification.get('market_cap', 'Неизвестно')
            multiplier = notification.get('multiplier', 1)
            
            if notification_type == 'growth':
                message += f"{i}. 🔥 *{token_name}* вырос в {multiplier}x! MC: {market_cap}\n"
            else:
                message += f"{i}. ℹ️ *{token_name}*: MC: {market_cap}\n"
        
        return message

# Создаем глобальный экземпляр менеджера уведомлений
notification_manager = NotificationManager()

async def add_growth_notification(
    chat_id: int, 
    token_name: str, 
    multiplier: int, 
    market_cap: str,
    reply_to_message_id: Optional[int] = None,
    contract_address: Optional[str] = None
) -> None:
    """
    HOTFIX: Исправляет ошибку "Message to be replied not found"
    """
    try:
        from token_service import get_telegram_context
        context = get_telegram_context()
        
        if not context:
            logger.error(f"Could not get Telegram context for growth notification")
            return
        
        # Формируем сообщение с огоньками
        fire_emojis = "🔥" * min(multiplier, 10)
        growth_message = (
            f"{fire_emojis}\n"
            f"*{token_name}* *{multiplier}x* from call!\n\n"
            f"MCap: {market_cap}"
        )
        
        # Получаем получателей
        from handlers.auth_middleware import get_user_db
        from config import CONTROL_ADMIN_IDS
        user_db = get_user_db()
        all_users = user_db.get_all_users()
        active_users = [user for user in all_users if user['is_active']]
        
        recipients = []
        
        # Добавляем админа
        for admin_id in CONTROL_ADMIN_IDS:
            recipients.append({'user_id': admin_id, 'username': 'admin'})
        
        # Добавляем пользователей (исключая админа)
        for user in active_users:
            if user['user_id'] not in CONTROL_ADMIN_IDS:
                recipients.append(user)
        
        if not recipients:
            logger.warning("No recipients for growth notification")
            return
        
        sent_count = 0
        failed_count = 0
        
        # Отправляем уведомления
        for user in recipients:
            try:
                # ИСПРАВЛЕНИЕ: ТОЛЬКО админ получает reply
                if user['user_id'] in CONTROL_ADMIN_IDS and reply_to_message_id:
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=growth_message,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_to_message_id=reply_to_message_id
                    )
                    logger.info(f"Sent growth notification to admin {user['user_id']} as reply")
                else:
                    # ИСПРАВЛЕНИЕ: Обычные пользователи получают обычное сообщение
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=growth_message,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logger.info(f"Sent growth notification to user {user['user_id']} as regular message")
                
                sent_count += 1
                
            except Exception as e:
                error_message = str(e)
                logger.error(f"Error sending growth notification to user {user['user_id']}: {error_message}")
                
                # ДОПОЛНИТЕЛЬНАЯ ОБРАБОТКА ОШИБОК
                if "Message to be replied not found" in error_message:
                    logger.warning(f"Reply message not found for user {user['user_id']}, trying without reply...")
                    
                    # Fallback: отправляем без reply
                    try:
                        await context.bot.send_message(
                            chat_id=user['user_id'],
                            text=growth_message,
                            parse_mode=ParseMode.MARKDOWN
                        )
                        sent_count += 1
                        logger.info(f"Successfully sent fallback message to user {user['user_id']}")
                    except Exception as fallback_error:
                        logger.error(f"Fallback also failed for user {user['user_id']}: {str(fallback_error)}")
                        failed_count += 1
                else:
                    failed_count += 1
        
        logger.info(f"Growth notification for token {token_name} to x{multiplier}: sent {sent_count}, failed {failed_count}")
            
    except Exception as e:
        logger.error(f"Error sending growth notification for token {token_name}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

async def add_growth_notification_with_reply(
    chat_id: int, 
    token_name: str, 
    multiplier: int, 
    market_cap: str,
    reply_to_message_id: Optional[int] = None,
    contract_address: Optional[str] = None
) -> None:
    """
    НОВАЯ ФУНКЦИЯ: Система уведомлений с reply и удалением предыдущих сообщений.
    Работает ПАРАЛЛЕЛЬНО с существующей add_growth_notification.
    """
    try:
        from token_service import get_telegram_context
        context = get_telegram_context()
        
        if not context:
            logger.error("Could not get Telegram context for growth notification with reply")
            return
        
        # Определяем токен для поиска
        token_query = contract_address if contract_address else token_name
        
        # Формируем сообщение
        fire_emojis = "🔥" * min(multiplier, 10)
        growth_message = (
            f"{fire_emojis}\n"
            f"*{token_name}* *{multiplier}x* from call!\n\n"
            f"MCap: {market_cap}"
        )
        
        # Получаем пользователей из новой системы
        from handlers.auth_middleware import get_user_db
        user_db = get_user_db()
        users_for_token = user_db.get_all_users_for_token(token_query)
        
        if not users_for_token:
            logger.info(f"No users found in new system for {token_query}, falling back to old system")
            # Вызываем СТАРУЮ функцию как fallback
            await add_growth_notification(chat_id, token_name, multiplier, market_cap, reply_to_message_id, contract_address)
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
                # Удаляем старое сообщение о росте
                if old_growth_message_id and multiplier > current_multiplier:
                    try:
                        await context.bot.delete_message(chat_id=user_id, message_id=old_growth_message_id)
                        logger.info(f"Deleted old growth message for user {user_id}")
                    except Exception as e:
                        logger.warning(f"Could not delete old growth message: {e}")
                
                # Отправляем reply на сообщение о токене
                if token_message_id:
                    try:
                        new_growth_msg = await context.bot.send_message(
                            chat_id=user_id,
                            text=growth_message,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_to_message_id=token_message_id
                        )
                        
                        # Сохраняем новый growth_message_id
                        user_db.update_user_growth_message(token_query, user_id, new_growth_msg.message_id, multiplier)
                        
                        sent_count += 1
                        logger.info(f"Sent growth x{multiplier} to user {user_id} as reply")
                        
                    except Exception as e:
                        if "Message to be replied not found" in str(e):
                            # Fallback: обычное сообщение
                            try:
                                new_growth_msg = await context.bot.send_message(
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
                    # Нет token_message_id - обычное сообщение
                    new_growth_msg = await context.bot.send_message(
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
        # Fallback на старую функцию
        await add_growth_notification(chat_id, token_name, multiplier, market_cap, reply_to_message_id, contract_address)

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