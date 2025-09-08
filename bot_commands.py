import logging
from typing import List, Optional
from telegram import Update, BotCommand
from telegram.ext import Application, ContextTypes
from telegram.constants import ParseMode

from config import CONTROL_ADMIN_IDS
from user_database import user_db
from utils import format_tokens_list

logger = logging.getLogger(__name__)

# ============================================================================
# ПРОВЕРКИ ДОСТУПА (упрощенные)
# ============================================================================

def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором."""
    return user_id in CONTROL_ADMIN_IDS

# ============================================================================
# АДМИНСКИЕ КОМАНДЫ (оригинальные)
# ============================================================================

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Главная админ панель с инлайн кнопками (оригинальная версия)"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
        
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        
        message = "👑 *admin panel*"
        
        # Создаем инлайн клавиатуру с 2 кнопками (как в оригинале)
        keyboard = [
            [
                InlineKeyboardButton("📊 Tokens", callback_data="admin_tokens"),
                InlineKeyboardButton("👥 Users", callback_data="admin_users"),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        logger.info(f"Админ панель открыта администратором {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка в админ панели: {str(e)}")
        await update.message.reply_text("Ошибка при получении данных админ панели.")

async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add user: /adduser 123456789 @username (оригинальная версия)"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /adduser USER_ID [@username]")
        return
    
    try:
        target_user_id = int(context.args[0])
        username = context.args[1].replace('@', '') if len(context.args) > 1 else None
        
        if user_db.add_user(target_user_id, username):
            await update.message.reply_text(f"✅ User {target_user_id} added!")
            logger.info(f"User {target_user_id} added by admin")
        else:
            await update.message.reply_text("❌ Error adding user")
    except ValueError:
        await update.message.reply_text("❌ User ID must be a number")
    except Exception as e:
        logger.error(f"Error adding user: {e}")
        await update.message.reply_text("❌ Error occurred")

async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove user: /removeuser 123456789 (оригинальная версия)"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора.")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /removeuser USER_ID")
        return
    
    try:
        target_user_id = int(context.args[0])
        
        if user_db.remove_user(target_user_id):
            await update.message.reply_text(f"✅ User {target_user_id} removed!")
            logger.info(f"User {target_user_id} removed by admin")
        else:
            await update.message.reply_text("❌ User not found")
    except ValueError:
        await update.message.reply_text("❌ User ID must be a number")
    except Exception as e:
        logger.error(f"Error removing user: {e}")
        await update.message.reply_text("❌ Error occurred")

# ============================================================================
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ============================================================================

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /list - показывает список токенов (упрощенная версия)."""
    user_id = update.effective_user.id
    
    if not user_db.is_user_authorized(user_id):
        await update.message.reply_text("❌ У вас нет доступа к боту")
        return
    
    try:
        # Получаем токены из мониторинга
        from token_service import get_monitored_tokens
        tokens_data = get_monitored_tokens()
        
        if not tokens_data:
            await update.message.reply_text(
                "📋 Список токенов пуст\n\nОтправьте адрес токена для добавления в мониторинг",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Форматируем список (первые 10 токенов)
        message, total_pages, current_page = format_tokens_list(tokens_data, page=0, tokens_per_page=10)
        
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        
    except Exception as e:
        logger.error(f"Ошибка в команде /list: {e}")
        await update.message.reply_text("❌ Ошибка при получении списка токенов")

# ============================================================================
# ПРОСТОЙ CALLBACK РОУТЕР (без сложной логики)
# ============================================================================

async def handle_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Роутер для callback запросов админ панели."""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Проверяем права администратора
    if not is_admin(user_id):
        await query.answer("❌ У вас нет прав администратора.")
        return
    
    await query.answer()  # Убираем "часики" на кнопке
    
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        
        # Роутинг callback запросов
        if query.data == "admin_tokens":
            await show_tokens_menu(query, context)
        elif query.data == "admin_users": 
            await show_users_menu(query, context)
        elif query.data == "admin_back":
            await show_main_admin_panel(query, context)
        # Обработка токен-команд
        elif query.data == "tokens_list":
            await handle_tokens_list(query, context)
        elif query.data == "tokens_clear":
            await handle_tokens_clear(query, context)
        elif query.data == "tokens_analytics":
            await handle_tokens_analytics(query, context)
        elif query.data == "tokens_stats":
            await handle_tokens_stats(query, context)
        # Обработка статистики по периодам
        elif query.data == "stats_daily":
            await handle_stats_period(query, context, days=1)
        elif query.data == "stats_weekly":
            await handle_stats_period(query, context, days=7)
        elif query.data == "stats_monthly":
            await handle_stats_period(query, context, days=30)
        # Обработка пользовательских команд
        elif query.data == "users_add":
            await handle_users_add(query, context)
        elif query.data == "users_remove":
            await handle_users_remove(query, context)
        elif query.data == "users_list":
            await handle_users_list(query, context)
        elif query.data == "users_toggle":
            await handle_users_toggle(query, context)
        elif query.data.startswith("activate_"):
            await handle_user_activate(query, context)
        elif query.data.startswith("deactivate_"):
            await handle_user_deactivate(query, context)
        elif query.data.startswith("authorize_"):
            await handle_authorize_user(query, context)
        elif query.data.startswith("remove_"):
            await handle_remove_user(query, context)
        elif query.data.startswith("confirm_remove_"):
            await handle_confirm_remove_user(query, context)
        else:
            await query.answer("Неизвестная команда")
            
    except Exception as e:
        logger.error(f"Ошибка в callback роутере: {str(e)}")
        await query.answer("❌ Произошла ошибка")

async def show_main_admin_panel(query, context):
    """Показать главную админ панель."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    message = "👑 *admin panel*"
    keyboard = [
        [
            InlineKeyboardButton("📊 Tokens", callback_data="admin_tokens"),
            InlineKeyboardButton("👥 Users", callback_data="admin_users"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def show_tokens_menu(query, context):
    """Показать меню управления токенами (полная оригинальная версия)."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from token_service import get_monitored_tokens
    
    tokens = get_monitored_tokens()
    token_count = len(tokens) if tokens else 0
    
    message = f"📊 *Управление токенами*\n\nВсего токенов: {token_count}"
    
    keyboard = [
        [
            InlineKeyboardButton("📋 List", callback_data="tokens_list"),
            InlineKeyboardButton("🗑️ Clear", callback_data="tokens_clear"),
        ],
        [
            InlineKeyboardButton("📊 Analytics", callback_data="tokens_analytics"),
            InlineKeyboardButton("📈 Stats", callback_data="tokens_stats"),
        ],
        [
            InlineKeyboardButton("↩️ Back", callback_data="admin_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def show_users_menu(query, context):
    """Показать меню управления пользователями.""" 
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    all_users = user_db.get_all_users()
    active_users = [u for u in all_users if u.get('is_active')]
    
    message = f"👥 *Управление пользователями*\n\nВсего: {len(all_users)}\nАктивных: {len(active_users)}"
    
    keyboard = [
        [
            InlineKeyboardButton("➕ Добавить", callback_data="users_add"),
            InlineKeyboardButton("🗑️ Удалить", callback_data="users_remove"),
        ],
        [
            InlineKeyboardButton("👥 Список", callback_data="users_list"),
            InlineKeyboardButton("🔄 Активация", callback_data="users_toggle"),
        ],
        [
            InlineKeyboardButton("↩️ Назад", callback_data="admin_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

# ============================================================================
# ОБРАБОТЧИКИ CALLBACK КОМАНД (ОРИГИНАЛЬНЫЕ)
# ============================================================================

async def handle_tokens_list(query, context):
    """Показать список токенов."""
    await query.answer("📋 Список токенов")
    message = "📋 *Список отслеживаемых токенов*\n\nИспользуйте команду `/list` для получения полного списка"
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

async def handle_tokens_clear(query, context):
    """Очистка токенов."""
    await query.answer("🗑️ Очистка токенов")
    message = "🗑️ *Очистка токенов*\n\nФункция очистки токенов из мониторинга"
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

async def handle_tokens_analytics(query, context):
    """Аналитика токенов."""
    await query.answer("📊 Аналитика токенов")
    message = "📊 *Аналитика токенов*\n\nФункция генерации аналитики по токенам"
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

async def handle_tokens_stats(query, context):
    """Показывает кнопки выбора периода статистики."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    message = "📈 *Token Statistics*\n\nВыберите период для анализа:"
    
    keyboard = [
        [
            InlineKeyboardButton("📅 Daily", callback_data="stats_daily"),
            InlineKeyboardButton("📊 Weekly", callback_data="stats_weekly"),
        ],
        [
            InlineKeyboardButton("📈 Monthly", callback_data="stats_monthly"),
        ],
        [
            InlineKeyboardButton("↩️ Back", callback_data="admin_tokens")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_stats_period(query, context, days: int):
    """Отправляет статистику за указанный период."""
    try:
        period_name = "Daily" if days == 1 else "Weekly" if days == 7 else "Monthly"
        
        # Отправляем сообщение о формировании статистики
        await query.edit_message_text(
            f"📊 Формирую {period_name.lower()} статистику токенов за {days} дн...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Отправляем статистику за указанный период
        from token_service import send_token_stats
        await send_token_stats(context, days=days)
        
        # Возвращаемся к кнопкам выбора периода
        await handle_tokens_stats(query, context)
        
    except Exception as e:
        logger.error(f"Ошибка при отправке {period_name.lower()} статистики: {e}")
        await query.edit_message_text(
            f"❌ Ошибка при формировании {period_name.lower()} статистики",
            parse_mode=ParseMode.MARKDOWN
        )

async def handle_users_add(query, context):
    """Добавление пользователей."""
    await query.answer("➕ Добавить пользователя")
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    potential_users = user_db.get_potential_users()
    
    if potential_users:
        message = "➕ *Потенциальные пользователи*\n\nВыберите пользователя для авторизации:\n\n"
        keyboard = []
        
        for i, user in enumerate(potential_users[:8]):  # Показываем первые 8
            user_id = user.get('user_id')
            username = user.get('username', 'None')
            first_name = user.get('first_name', 'Unknown')
            
            message += f"`{user_id}` | @{username} | {first_name}\n"
            keyboard.append([
                InlineKeyboardButton(
                    f"✅ {user_id} (@{username})", 
                    callback_data=f"authorize_{user_id}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("↩️ Back", callback_data="admin_users")])
    else:
        message = "🔭 **Нет потенциальных пользователей**\n\nИспользуйте `/adduser USER_ID [@username]`"
        keyboard = [[InlineKeyboardButton("↩️ Back", callback_data="admin_users")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_users_remove(query, context):
    """Удаление пользователей."""
    await query.answer("🗑️ Удалить пользователя")
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    all_users = user_db.get_all_users()
    
    if all_users:
        message = "🗑️ *Удаление пользователей*\n\nВыберите пользователя для удаления:\n\n"
        keyboard = []
        
        for i, user in enumerate(all_users[:8]):  # Показываем первые 8
            user_id = user.get('user_id')
            username = user.get('username', 'N/A')
            status_icon = "✅" if user.get('is_active') else "❌"
            
            message += f"{status_icon} `{user_id}` | @{username}\n"
            keyboard.append([
                InlineKeyboardButton(
                    f"🗑️ {status_icon} {user_id} (@{username})", 
                    callback_data=f"remove_{user_id}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("↩️ Back", callback_data="admin_users")])
    else:
        message = "🔭 **Нет пользователей для удаления**"
        keyboard = [[InlineKeyboardButton("↩️ Back", callback_data="admin_users")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_users_list(query, context):
    """Список пользователей."""
    await query.answer("👥 Список пользователей")
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    users = user_db.get_all_users()
    
    if users:
        message = "👥 *Список пользователей*\n\n"
        for i, user in enumerate(users[:10]):  # Показываем первые 10
            status = "✅ Активен" if user.get('is_active') else "❌ Неактивен"
            username = user.get('username') or 'N/A'
            added_date = user.get('added_date', 'Неизвестно')
            message += f"{status} `{user['user_id']}` | @{username} | {added_date}\n"
        
        if len(users) > 10:
            message += f"\n... и еще {len(users) - 10} пользователей"
    else:
        message = "👥 *Список пользователей пуст*"
    
    keyboard = [[InlineKeyboardButton("↩️ Back", callback_data="admin_users")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_users_toggle(query, context):
    """Активация/деактивация пользователей."""
    await query.answer("🔄 Управление статусом")
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    users = user_db.get_all_users()
    
    if users:
        message = "🔄 *Активация/деактивация пользователей*\n\nНажмите на пользователя чтобы изменить его статус:\n\n"
        keyboard = []
        
        for user in users[:8]:  # Показываем первые 8
            user_id = user.get('user_id')
            username = user.get('username', 'N/A')
            is_active = user.get('is_active')
            
            if is_active:
                button_text = f"❌ Деактивировать {user_id}"
                callback_data = f"deactivate_{user_id}"
            else:
                button_text = f"✅ Активировать {user_id}"
                callback_data = f"activate_{user_id}"
            
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("↩️ Back", callback_data="admin_users")])
    else:
        message = "🔭 В базе данных нет пользователей для управления."
        keyboard = [[InlineKeyboardButton("↩️ Back", callback_data="admin_users")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_user_activate(query, context):
    """Активация пользователя."""
    user_id = int(query.data.replace("activate_", ""))
    if user_db.update_user_status(user_id, is_active=True):
        await query.answer(f"✅ Пользователь {user_id} активирован")
    else:
        await query.answer(f"❌ Ошибка активации пользователя {user_id}")
    await handle_users_toggle(query, context)

async def handle_user_deactivate(query, context):
    """Деактивация пользователя.""" 
    user_id = int(query.data.replace("deactivate_", ""))
    if user_db.update_user_status(user_id, is_active=False):
        await query.answer(f"❌ Пользователь {user_id} деактивирован")
    else:
        await query.answer(f"❌ Ошибка деактивации пользователя {user_id}")
    await handle_users_toggle(query, context)

async def handle_authorize_user(query, context):
    """Авторизация пользователя."""
    user_id = int(query.data.replace("authorize_", ""))
    if user_db.authorize_potential_user(user_id):
        await query.answer(f"✅ Пользователь {user_id} авторизован")
    else:
        await query.answer(f"❌ Ошибка авторизации пользователя {user_id}")
    await handle_users_add(query, context)

async def handle_remove_user(query, context):
    """Подтверждение удаления пользователя."""
    user_id = int(query.data.replace("remove_", ""))
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    message = f"🗑️ *Подтверждение удаления*\n\nВы уверены что хотите удалить пользователя `{user_id}`?\n\nПользователь будет полностью удален из базы данных."
    
    keyboard = [
        [
            InlineKeyboardButton("🗑️ Да, удалить", callback_data=f"confirm_remove_{user_id}"),
            InlineKeyboardButton("❌ Отмена", callback_data="users_remove")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

async def handle_confirm_remove_user(query, context):
    """Окончательное удаление пользователя."""
    user_id = int(query.data.replace("confirm_remove_", ""))
    if user_db.remove_user(user_id):
        await query.answer(f"🗑️ Пользователь {user_id} удален")
    else:
        await query.answer(f"❌ Ошибка удаления пользователя {user_id}")
    await handle_users_remove(query, context)

# ============================================================================
# НАСТРОЙКА КОМАНД БОТА
# ============================================================================

async def setup_bot_commands(application: Application) -> None:
    """Устанавливает команды бота в меню (оригинальная версия)."""
    try:
        commands = [
            BotCommand("start", "Запуск бота"),
            BotCommand("help", "Помощь"),
            BotCommand("list", "Список токенов"),
            BotCommand("admin", "Админ панель"),
            BotCommand("adduser", "Добавить пользователя"),
            BotCommand("removeuser", "Удалить пользователя"),
        ]
        
        # Устанавливаем команды
        await application.bot.set_my_commands(commands)
        
        logger.info("✅ Команды бота установлены")
        
    except Exception as e:
        logger.error(f"Ошибка при установке команд бота: {e}")
        raise