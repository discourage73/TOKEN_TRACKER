import logging
import functools
from typing import Callable, Any
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Импортируем список админов из config
from config import CONTROL_ADMIN_IDS

def get_user_db():
    """Ленивый импорт базы пользователей"""
    from user_database import user_db
    return user_db

def user_required(func: Callable) -> Callable:
    """Декоратор для авторизованных пользователей"""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs) -> Any:
        user_id = update.effective_user.id
        
        # Админы всегда проходят
        if user_id in CONTROL_ADMIN_IDS:  # ИСПРАВЛЕНО: убрали not
            return await func(update, context, *args, **kwargs)
        
        # Проверяем обычного пользователя в базе
        if get_user_db().is_user_authorized(user_id):
            return await func(update, context, *args, **kwargs)
        
        # 🆕 ДОБАВИТЬ: Записываем неавторизованных в потенциальные
        from user_database import user_db
        user_db.add_potential_user(
            user_id=user_id,
            username=update.effective_user.username,
            first_name=update.effective_user.first_name,
            last_name=update.effective_user.last_name
        )
        logger.info(f"🔧 Неавторизованный {user_id} записан в potential_users")
        
        # Отказ неавторизованному
        await update.message.reply_text(
            f"❌ Доступ запрещен.\n"
            f"ℹ️ Ваш ID: `{user_id}`\n"
            "Обратитесь к администратору."
        )
        logger.info(f"Доступ запрещен пользователю {user_id}")
        return None
    return wrapper

def admin_required(func: Callable) -> Callable:
    """Декоратор только для админа"""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs) -> Any:
        user_id = update.effective_user.id
        
        if user_id not in CONTROL_ADMIN_IDS:  # ИСПРАВЛЕНО: добавили not
            await update.message.reply_text(
                f"❌ Доступ только для администратора.\n"
                f"ℹ️ Ваш ID: `{user_id}`"
            )
            return None
            
        return await func(update, context, *args, **kwargs)
    return wrapper