# handlers/user_commands.py
import logging
from telegram import Update, BotCommand
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from .auth_middleware import user_required
from config import CONTROL_ADMIN_IDS

logger = logging.getLogger(__name__)

@user_required
async def start_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start для авторизованных пользователей."""
    user = update.effective_user
    user_id = user.id
    first_name = user.first_name or user.username or 'пользователь'
    
    # 🆕 ЗАПИСЫВАЕМ В ПОТЕНЦИАЛЬНЫЕ ПОЛЬЗОВАТЕЛИ
    logger.info(f"🔧 START_USER вызван для {user_id}")
    from user_database import user_db
    logger.info(f"🔧 Импортируем user_db")
    
    result = user_db.add_potential_user(
        user_id=user_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    logger.info(f"🔧 add_potential_user вернул: {result}")
    logger.info(f"🔧 Пользователь {user_id} записан в potential_users")
    
    # Устанавливаем персональное меню
    await setup_user_menu(context, user_id)
    
    # Проверяем, является ли пользователь администратором
    if user_id in CONTROL_ADMIN_IDS:  # ИСПРАВЛЕНО
        welcome_message = (
            f"👑 Добро пожаловать, администратор {first_name}!\n\n"
            "✅ У вас есть права администратора.\n"
            "📊 Используйте /admin для доступа к панели управления.\n"
            "💡 Все ваши сообщения будут отправлены всем пользователям."
        )
    else:
        welcome_message = (
            f"🎉 Добро пожаловать, {first_name}!\n\n"
            "✅ У вас есть доступ к боту.\n"
            "📝 Вы будете получать уведомления о токенах и статистику.\n"
            "ℹ️ Доступные команды: /start, /help"
        )
    
    await update.message.reply_html(welcome_message)
    logger.info(f"Команда /start выполнена для пользователя {user_id}")


@user_required
async def help_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /help для авторизованных пользователей."""
    user_id = update.effective_user.id
    
    if user_id in CONTROL_ADMIN_IDS:  # ИСПРАВЛЕНО
        help_message = (
            "🔧 *Справка для администратора:*\n\n"
            "📊 `/admin` - панель управления\n"
            "👥 Управление пользователями\n"
            "📈 Управление токенами\n"
            "📝 Рассылка сообщений всем пользователям\n\n"
            "💡 *Автоматические функции:*\n"
            "• Мониторинг токенов\n" 
            "• Уведомления о росте\n"
            "• Статистика каждые 12 часов"
        )
    else:
        help_message = (
            "ℹ️ *Справка по боту:*\n\n"
            "🤖 Этот бот отслеживает токены и отправляет уведомления о их росте.\n\n"
            "📊 *Что вы получаете:*\n"
            "• Уведомления о росте токенов (x2, x3, x4...)\n"
            "• Статистику по токенам каждые 12 часов\n"
            "• Информацию от администратора\n\n"
            "🔧 *Доступные команды:*\n"
            "• `/start` - приветствие\n"
            "• `/help` - эта справка"
        )
    
    await update.message.reply_markdown(help_message)
    logger.info(f"Команда /help выполнена для пользователя {user_id}")

async def handle_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка запросов от неавторизованных пользователей."""
    user = update.effective_user
    user_id = user.id
    first_name = user.first_name or user.username or 'пользователь'
    
    # 🆕 ДОБАВИТЬ: Записываем неавторизованных в потенциальные пользователи
    logger.info(f"🔧 HANDLE_UNAUTHORIZED вызван для {user_id}")
    from user_database import user_db
    
    result = user_db.add_potential_user(
        user_id=user_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    logger.info(f"🔧 Неавторизованный {user_id} записан в potential_users: {result}")
    
    # Устанавливаем ограниченное меню для неавторизованных
    await setup_user_menu(context, user_id)
    
    unauthorized_message = (
        f"❌ Доступ запрещен, {first_name}.\n\n"
        "🔒 У вас нет доступа к приватному каналу.\n"
        "📞 Для получения доступа обратитесь к администратору.\n\n"
        f"ℹ️ Ваш Telegram ID: `{user_id}`\n"
        "(Отправьте этот ID администратору)"
    )
    
    await update.message.reply_html(unauthorized_message)
    logger.info(f"Отказ в доступе для неавторизованного пользователя {user_id}")

async def setup_user_menu(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Устанавливает персональное меню команд в зависимости от роли пользователя."""
    try:
        from .auth_middleware import get_user_db
        
        # Определяем роль пользователя
        if user_id in CONTROL_ADMIN_IDS:  # ИСПРАВЛЕНО
            # Меню для администратора
            commands = [
                BotCommand("start", "главное меню"),
                BotCommand("admin", "админ панель"),
                BotCommand("help", "справка")
            ]
        elif get_user_db().is_user_authorized(user_id):
            # Меню для авторизованного пользователя
            commands = [
                BotCommand("start", "главное меню"),
                BotCommand("help", "справка")
            ]
        else:
            # Меню для неавторизованного пользователя
            commands = [
                BotCommand("start", "главное меню"),
                BotCommand("help", "справка")
            ]
        
        # Устанавливаем команды для конкретного пользователя
        await context.bot.set_my_commands(
            commands=commands,
            scope={'type': 'chat', 'chat_id': user_id}
        )
        
        logger.info(f"Персональное меню установлено для пользователя {user_id}: {len(commands)} команд")
        
    except Exception as e:
        logger.error(f"Ошибка при установке меню для пользователя {user_id}: {e}")