"""Middleware для проверки авторизации пользователей."""

import logging
from functools import wraps
from typing import Callable
from telegram import Update
from telegram.ext import ContextTypes

from config import CONTROL_ADMIN_IDS
from user_database import user_db

logger = logging.getLogger(__name__)

def user_required(func: Callable) -> Callable:
    """Декоратор для проверки авторизации пользователя."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not user_db.is_user_authorized(user_id):
            await update.message.reply_text(
                "❌ У вас нет доступа к боту. Обратитесь к администратору."
            )
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapper

def admin_required(func: Callable) -> Callable:
    """Декоратор для проверки прав администратора."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        
        if user_id not in CONTROL_ADMIN_IDS:
            await update.message.reply_text(
                "❌ У вас нет прав администратора."
            )
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapper

def get_user_db():
    """Возвращает экземпляр базы данных пользователей."""
    return user_db