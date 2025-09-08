"""Обработчики пользовательских команд."""

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from user_database import user_db

async def start_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start для обычных пользователей."""
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # Добавляем в потенциальные пользователи
    user_db.add_potential_user(
        user_id=user_id,
        username=username,
        first_name=update.effective_user.first_name,
        last_name=update.effective_user.last_name
    )
    
    welcome_text = (
        "🤖 *Добро пожаловать в Token Tracker Bot!*\n\n"
        "📝 Отправьте адрес токена для получения информации\n"
        "📊 Используйте /help для списка команд\n\n"
        "_Ожидайте авторизации от администратора_"
    )
    
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN
    )

async def help_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /help."""
    help_text = (
        "🤖 *Команды бота:*\n\n"
        "/start - Запуск бота\n"
        "/help - Помощь\n"
        "/list - Список токенов (для авторизованных)\n\n"
        "📝 *Использование:*\n"
        "Отправьте адрес токена для получения информации о нем\n\n"
        "_Доступ к функциям предоставляется администратором_"
    )
    
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик для неавторизованных пользователей."""
    user_id = update.effective_user.id
    
    if not user_db.is_user_authorized(user_id):
        await update.message.reply_text(
            "❌ У вас нет доступа к боту. Используйте /start для регистрации.",
            parse_mode=ParseMode.MARKDOWN
        )