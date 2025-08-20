# handlers/admin_commands.py
import logging
import asyncio
import time
from typing import Dict, Any, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import BadRequest

from handlers.auth_middleware import admin_required
from config import CONTROL_ADMIN_IDS

logger = logging.getLogger(__name__)

# Кеш для данных меню (избегаем дублированных запросов)
_menu_cache = {}
_cache_timeout = 60  # секунд

async def handle_admin_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Маршрутизатор для admin callback'ов с защитой от timeout'ов"""
    query = update.callback_query
    data = query.data
    
    try:
        # КРИТИЧНО: Отвечаем на callback СРАЗУ, чтобы избежать timeout
        await query.answer("Обрабатываю запрос...")
        
        # Маршрутизация по типу callback'а
        if data == "admin_panel":
            await show_admin_panel(query, context)
        elif data == "admin_tokens":
            await show_tokens_menu(query, context)
        elif data == "admin_users":
            await show_users_menu(query, context)
        elif data == "admin_back":
            await show_main_panel(query, context)
        
        # Обработка токен-команд
        elif data == "tokens_list":
            await handle_tokens_list(query, context)
        elif data == "tokens_clear":
            await handle_tokens_clear(query, context)
        elif data == "tokens_analytics":
            await handle_tokens_analytics(query, context)
        elif data == "tokens_stats":
            await handle_tokens_stats(query, context)
        
        # Обработка пользовательских команд
        elif data == "users_add":
            await handle_users_add(query, context)
        elif data == "users_remove":
            await handle_users_remove(query, context)
        elif data == "users_list":
            await handle_users_list(query, context)
        elif data == "users_toggle":
            await handle_users_toggle(query, context)
        elif data.startswith("activate_"):
            await handle_user_activate(query, context)
        elif data.startswith("deactivate_"):
            await handle_user_deactivate(query, context)
        elif data.startswith("authorize_"):
            await handle_authorize_user(query, context)
        elif data.startswith("remove_"):
            await handle_remove_user(query, context)
        elif data.startswith("confirm_remove_"):
            await handle_confirm_remove_user(query, context)
        
        # Обработка существующих callback'ов из test_bot_commands
        else:
            await handle_legacy_callbacks(query, context, data)
            
    except BadRequest as e:
        error_msg = str(e)
        
        # Обрабатываем специфичные ошибки Telegram
        if "Message is not modified" in error_msg:
            logger.warning(f"Попытка обновить идентичное сообщение: {data}")
            # НЕ пытаемся отвечать повторно - это вызовет еще одну ошибку
            return
            
        elif "Query is too old" in error_msg:
            logger.warning(f"Timeout callback'а: {data}")
            # НЕ пытаемся отвечать повторно - это невозможно
            return
            
        else:
            logger.error(f"BadRequest при обработке {data}: {e}")
            # Пытаемся уведомить о неизвестной ошибке только если запрос свежий
            try:
                await query.edit_message_text("❌ Произошла ошибка. Попробуйте еще раз.")
            except:
                pass  # Если и это не сработало, просто игнорируем
                
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке callback {data}: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def handle_legacy_callbacks(query, context, data):
    """Обработчик для существующих callback'ов из test_bot_commands"""
    try:
        from test_bot_commands import (
            handle_clear_cancel, handle_refresh_list, handle_delete_all_confirm,
            handle_delete_confirm, handle_delete_selective, handle_delete_token,
            handle_clear_return
        )
        
        # Создаем fake update для совместимости
        fake_update = create_fake_update(query)
        
        # Пробуем обработать существующими функциями
        if data == "clear_cancel":
            await handle_clear_cancel(fake_update, context)
        elif data == "refresh_list":
            await handle_refresh_list(fake_update, context)
        elif data == "delete_all_confirm":
            await handle_delete_all_confirm(fake_update, context)
        elif data == "delete_confirm":
            await handle_delete_confirm(fake_update, context)
        elif data == "delete_selective":
            await handle_delete_selective(fake_update, context)
        elif data.startswith("delete_token_"):
            await handle_delete_token(fake_update, context)
        elif data == "clear_return":
            await handle_clear_return(fake_update, context)
        else:
            logger.warning(f"Неизвестный callback: {data}")
            
    except Exception as e:
        logger.error(f"Ошибка в handle_legacy_callbacks для {data}: {e}")


# === ПАНЕЛИ И МЕНЮ ===

@admin_required
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /admin - главная точка входа в админ панель"""
    try:
        user = update.effective_user
        first_name = user.first_name or user.username or 'Admin'
        
        message = """🔧 *Административная панель*

Добро пожаловать в панель управления ботом.
Выберите раздел для управления:"""

        keyboard = [
            [
                InlineKeyboardButton("📊 Токены", callback_data="admin_tokens"),
                InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
        logger.info(f"Админ панель открыта администратором {user.id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отображении админ панели: {e}")
        await update.message.reply_text("❌ Ошибка при загрузке админ панели")


async def show_admin_panel(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать главную админ панель"""
    try:
        message = """🔧 *Административная панель*

Выберите раздел для управления:"""

        keyboard = [
            [
                InlineKeyboardButton("📊 Токены", callback_data="admin_tokens"),
                InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Проверяем на дублирование
        if query.message.text != message:
            await query.edit_message_text(
                message, 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=reply_markup
            )
            
    except Exception as e:
        logger.error(f"Ошибка при отображении админ панели: {e}")


async def show_main_panel(query, context):
    """Возврат к главной панели"""
    await show_admin_panel(query, context)


# === МЕНЮ ТОКЕНОВ ===

async def show_tokens_menu(query, context):
    """Показывает меню управления токенами."""
    try:
        message = """📊 *Управление токенами*

📝 *Действия:*"""

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
                InlineKeyboardButton("↩️ Назад", callback_data="admin_back")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Проверяем на дублирование
        current_text = query.message.text if query.message else ""
        if current_text != message:
            await query.edit_message_text(
                message, 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=reply_markup
            )
        
    except Exception as e:
        logger.error(f"Ошибка при отображении меню токенов: {e}")


# === ОБРАБОТЧИКИ ТОКЕН-КОМАНД ===

async def handle_tokens_list(query, context):
    """Вызывает функцию list_tokens через callback с защитой от timeout."""
    try:
        # Сразу уведомляем пользователя
        logger.info("Запрошен список токенов через админ панель")
        
        from test_bot_commands import list_tokens
        
        # Создаем fake update для совместимости
        fake_update = type('Update', (), {
            'message': query.message, 
            'effective_user': query.from_user,
            'callback_query': query
        })()
        
        await list_tokens(fake_update, context)
        logger.info("Список токенов успешно отправлен")
        
    except Exception as e:
        logger.error(f"Ошибка при вызове list_tokens: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при загрузке списка токенов")
        except:
            pass


async def handle_tokens_clear(query, context):
    """Вызывает функцию clear_tokens через callback с защитой от timeout."""
    try:
        logger.info("Запрошена очистка токенов через админ панель")
        
        from test_bot_commands import clear_tokens
        
        # Создаем fake update для совместимости
        fake_update = type('Update', (), {
            'message': query.message,
            'effective_user': query.from_user,
            'callback_query': query
        })()
        
        await clear_tokens(fake_update, context)
        logger.info("Меню очистки токенов открыто")
        
    except Exception as e:
        logger.error(f"Ошибка при вызове clear_tokens: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при открытии управления токенами")
        except:
            pass


async def handle_tokens_analytics(query, context):
    """Вызывает функцию analytics_command через callback с защитой от timeout."""
    try:
        logger.info("Запрошена аналитика токенов через админ панель")
        
        from test_bot_commands import analytics_command
        
        # Создаем fake update для совместимости
        fake_update = type('Update', (), {
            'message': query.message,
            'effective_user': query.from_user,
            'callback_query': query
        })()
        
        await analytics_command(fake_update, context)
        logger.info("Аналитика токенов запущена")
        
    except Exception as e:
        logger.error(f"Ошибка при вызове analytics_command: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при генерации аналитики")
        except:
            pass


async def handle_tokens_stats(query, context):
    """Вызывает функцию stats_command через callback с защитой от timeout."""
    try:
        logger.info("Запрошена статистика токенов через админ панель")
        
        # Импортируем функцию stats из test_bot4.py
        from test_bot4 import stats_command
        
        # Создаем fake update для совместимости
        fake_update = type('Update', (), {
            'message': query.message,
            'effective_user': query.from_user,
            'callback_query': query
        })()
        
        await stats_command(fake_update, context)
        logger.info("Статистика токенов отправлена")
        
    except Exception as e:
        logger.error(f"Ошибка при вызове stats_command: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при отправке статистики")
        except:
            pass


# === МЕНЮ ПОЛЬЗОВАТЕЛЕЙ ===

async def show_users_menu(query, context):
    """Показывает меню управления пользователями с кешированием."""
    try:
        # Получаем данные пользователей с кешированием
        message, reply_markup = await get_users_menu_data()
        
        # Проверяем на дублирование
        current_text = query.message.text if query.message else ""
        if current_text != message:
            await query.edit_message_text(
                message, 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=reply_markup
            )
        
    except Exception as e:
        logger.error(f"Ошибка при отображении меню пользователей: {e}")


async def get_users_menu_data() -> tuple[str, InlineKeyboardMarkup]:
    """Получить данные для меню пользователей с кешированием"""
    cache_key = "users_menu"
    current_time = time.time()
    
    # Проверяем кеш
    if (cache_key in _menu_cache and 
        current_time - _menu_cache[cache_key]["timestamp"] < _cache_timeout):
        logger.debug("Используем кешированные данные меню пользователей")
        cached_data = _menu_cache[cache_key]
        return cached_data["message"], cached_data["markup"]
    
    # Получаем свежие данные
    logger.debug("Загружаем свежие данные пользователей")
    
    try:
        # Пытаемся получить данные пользователей
        try:
            from user_database import user_db
            all_users = user_db.get_all_users()
            active_count = len([u for u in all_users if u.get('is_active', False)])
            total_count = len(all_users)
        except:
            # Если нет базы пользователей, показываем базовое меню
            active_count = 0
            total_count = 0
        
        # Формируем сообщение
        message = f"""👥 *Управление пользователями*

📊 *Статистика:*
• Всего пользователей: {total_count}
• Активных: {active_count}

📝 *Действия:*"""

        # Создаем кнопки
        keyboard = [
            [
                InlineKeyboardButton("➕ Добавить", callback_data="users_add"),
                InlineKeyboardButton("🗑 Удалить", callback_data="users_remove"),
            ],
            [
                InlineKeyboardButton("📋 Показать всех", callback_data="users_list"),
                InlineKeyboardButton("🔄 Переключить", callback_data="users_toggle"),
            ],
            [
                InlineKeyboardButton("↩️ Назад", callback_data="admin_back")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Сохраняем в кеш
        _menu_cache[cache_key] = {
            "message": message,
            "markup": reply_markup, 
            "timestamp": current_time
        }
        
        return message, reply_markup
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных пользователей: {e}")
        # Возвращаем базовое сообщение об ошибке
        error_message = "❌ Ошибка загрузки данных пользователей"
        error_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("↩️ Назад", callback_data="admin_back")
        ]])
        return error_message, error_markup


# === ОБРАБОТЧИКИ ПОЛЬЗОВАТЕЛЬСКИХ КОМАНД ===

async def handle_users_list(query, context):
    """Показывает список всех пользователей с защитой от timeout."""
    try:
        logger.info("Запрошен список пользователей через админ панель")
        
        from user_database import user_db
        all_users = user_db.get_all_users()
        
        if not all_users:
            message = "👥 *Список пользователей*\n\n❌ Пользователей не найдено"
        else:
            message = f"👥 *Список пользователей* ({len(all_users)})\n\n"
            
            for i, user in enumerate(all_users[:20], 1):  # Показываем только первые 20
                status = "✅" if user.get('is_active', False) else "❌"
                username = user.get('username', 'Нет username')
                user_id = user.get('user_id', 'Неизвестно')
                message += f"{i}. {status} `{user_id}` @{username}\n"
            
            if len(all_users) > 20:
                message += f"\n... и еще {len(all_users) - 20} пользователей"
        
        keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке списка пользователей: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при загрузке списка пользователей")
        except:
            pass


async def handle_users_add(query, context):
    """Показывает интерфейс добавления пользователей с потенциальными пользователями"""
    try:
        from user_database import user_db
        
        # Получаем потенциальных пользователей
        potential_users = user_db.get_potential_users()
        
        if potential_users:
            message = "👥 **Потенциальные пользователи**\n\n"
            message += "📋 Пользователи, которые писали боту, но не авторизованы:\n\n"
            
            keyboard = []
            for i, user in enumerate(potential_users[:10], 1):  # Показываем первые 10
                username = user.get('username', 'Нет username')
                user_id = user.get('user_id')
                first_name = user.get('first_name', '')
                
                message += f"{i}. `{user_id}` - {first_name} (@{username})\n"
                
                # Добавляем кнопку авторизации
                keyboard.append([
                    InlineKeyboardButton(
                        f"✅ Авторизовать {first_name}", 
                        callback_data=f"authorize_{user_id}"
                    )
                ])
            
            if len(potential_users) > 10:
                message += f"\n... и еще {len(potential_users) - 10} пользователей"
        else:
            message = "👥 **Добавление пользователей**\n\n"
            message += "❌ Нет потенциальных пользователей для авторизации.\n\n"
            message += "💡 Пользователи появятся здесь после того, как напишут боту."
            keyboard = []
        
        # Добавляем кнопку назад
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="admin_users")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка в handle_users_add: {e}")
        try:
            await query.edit_message_text("❌ Ошибка при загрузке списка для добавления")
        except:
            pass


async def handle_authorize_user(query, context):
    """Авторизует пользователя по его ID из callback data"""
    try:
        # Извлекаем user_id из callback data
        user_id = int(query.data.split("_")[1])
        
        from user_database import user_db
        
        # Проверяем, не авторизован ли уже пользователь
        if user_db.is_user_authorized(user_id):
            await query.answer("⚠️ Пользователь уже авторизован")
            return
        
        # Авторизуем пользователя
        if user_db.add_user(user_id):
            # Удаляем из потенциальных
            user_db.remove_potential_user(user_id)
            
            logger.info(f"Админ авторизовал пользователя {user_id} через интерфейс")
            
            # Очищаем кеш
            global _menu_cache
            _menu_cache.clear()
            
            # Обновляем интерфейс
            await handle_users_add(query, context)
            
        else:
            try:
                await query.edit_message_text("❌ Ошибка при авторизации")
            except:
                pass
            
    except Exception as e:
        logger.error(f"Ошибка в handle_authorize_user: {e}")
        try:
            await query.edit_message_text("❌ Произошла ошибка")
        except:
            pass


async def handle_users_remove(query, context):
    """Показывает интерфейс удаления пользователей с кнопками"""
    try:
        from user_database import user_db
        
        # Получаем всех пользователей
        all_users = user_db.get_all_users()
        
        if all_users:
            active_users = [user for user in all_users if user['is_active']]
            inactive_users = [user for user in all_users if not user['is_active']]
            
            message = "🗑️ **Удаление пользователей**\n\n"
            message += f"📊 Всего пользователей: {len(all_users)}\n"
            message += f"✅ Активных: {len(active_users)}\n"
            message += f"❌ Неактивных: {len(inactive_users)}\n\n"
            message += "⚠️ **Осторожно! Удаление необратимо.**\n\n"
            
            keyboard = []
            
            # Показываем активных пользователей для удаления
            if active_users:
                message += "**Активные пользователи:**\n"
                for i, user in enumerate(active_users[:5], 1):  # Первые 5
                    username = user.get('username', 'Нет username')
                    user_id = user.get('user_id')
                    first_name = user.get('first_name', '')
                    
                    message += f"{i}. `{user_id}` - {first_name} (@{username})\n"
                    
                    # Кнопка удаления
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🗑 Удалить {first_name}", 
                            callback_data=f"remove_{user_id}"
                        )
                    ])
                
                if len(active_users) > 5:
                    message += f"... и еще {len(active_users) - 5} активных\n"
        else:
            message = "🗑️ **Удаление пользователей**\n\n❌ Пользователей не найдено"
            keyboard = []
        
        # Кнопка назад
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="admin_users")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка в handle_users_remove: {e}")


async def handle_remove_user(query, context):
    """Подтверждение удаления пользователя"""
    try:
        user_id = int(query.data.split("_")[1])
        
        from user_database import user_db
        user_info = user_db.get_user_info(user_id)
        
        if not user_info:
            await query.answer("❌ Пользователь не найден")
            return
        
        first_name = user_info.get('first_name', str(user_id))
        username = user_info.get('username', 'Нет username')
        
        message = f"⚠️ **Подтвердите удаление**\n\n"
        message += f"Пользователь: {first_name} (@{username})\n"
        message += f"ID: `{user_id}`\n\n"
        message += "❗ Это действие необратимо!"
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_remove_{user_id}"),
                InlineKeyboardButton("❌ Отмена", callback_data="users_remove")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка в handle_remove_user: {e}")


async def handle_confirm_remove_user(query, context):
    """Окончательное удаление пользователя"""
    try:
        user_id = int(query.data.split("_")[2])
        
        from user_database import user_db
        
        if user_db.remove_user(user_id):
            logger.info(f"Админ удалил пользователя {user_id}")
            
            # Очищаем кеш
            global _menu_cache
            _menu_cache.clear()
            
            # Возвращаемся к списку удаления
            await handle_users_remove(query, context)
        else:
            await query.answer("❌ Ошибка при удалении")
            
    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_remove_user: {e}")


async def handle_users_toggle(query, context):
    """Переключение активности пользователей"""
    try:
        from user_database import user_db
        all_users = user_db.get_all_users()
        
        if not all_users:
            await query.answer("❌ Пользователей не найдено")
            return
        
        message = "🔄 **Переключение активности**\n\n"
        
        keyboard = []
        for user in all_users[:8]:  # Первые 8 пользователей
            user_id = user.get('user_id')
            first_name = user.get('first_name', str(user_id))
            is_active = user.get('is_active', False)
            
            status = "✅" if is_active else "❌"
            action = "deactivate" if is_active else "activate"
            action_text = "Деактивировать" if is_active else "Активировать"
            
            message += f"{status} {first_name} (`{user_id}`)\n"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{action_text} {first_name}", 
                    callback_data=f"{action}_{user_id}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="admin_users")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message, 
            parse_mode=ParseMode.MARKDOWN, 
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка в handle_users_toggle: {e}")


async def handle_user_activate(query, context):
    """Активация пользователя"""
    try:
        user_id = int(query.data.split("_")[1])
        
        from user_database import user_db
        if user_db.set_user_active(user_id, True):
            logger.info(f"Админ активировал пользователя {user_id}")
            
            # Очищаем кеш
            global _menu_cache
            _menu_cache.clear()
            
            # Обновляем интерфейс
            await handle_users_toggle(query, context)
        else:
            await query.answer("❌ Ошибка при активации")
            
    except Exception as e:
        logger.error(f"Ошибка в handle_user_activate: {e}")


async def handle_user_deactivate(query, context):
    """Деактивация пользователя"""
    try:
        user_id = int(query.data.split("_")[1])
        
        from user_database import user_db
        if user_db.set_user_active(user_id, False):
            logger.info(f"Админ деактивировал пользователя {user_id}")
            
            # Очищаем кеш
            global _menu_cache
            _menu_cache.clear()
            
            # Обновляем интерфейс
            await handle_users_toggle(query, context)
        else:
            await query.answer("❌ Ошибка при деактивации")
            
    except Exception as e:
        logger.error(f"Ошибка в handle_user_deactivate: {e}")


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def clear_menu_cache():
    """Очистка кеша меню (для принудительного обновления)"""
    global _menu_cache
    _menu_cache.clear()
    logger.info("Кеш меню очищен")


def is_menu_cached(cache_key: str) -> bool:
    """Проверка наличия данных в кеше"""
    return (cache_key in _menu_cache and 
            time.time() - _menu_cache[cache_key]["timestamp"] < _cache_timeout)


async def safe_edit_message(query, message: str, reply_markup=None, parse_mode=ParseMode.MARKDOWN):
    """Безопасное обновление сообщения с проверкой на дублирование"""
    try:
        current_text = query.message.text if query.message else ""
        
        # Проверяем на дублирование
        if current_text == message:
            logger.debug("Сообщение идентично текущему, пропускаем обновление")
            return False
        
        # Обновляем сообщение
        await query.edit_message_text(
            message, 
            parse_mode=parse_mode, 
            reply_markup=reply_markup
        )
        return True
        
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug("Telegram предотвратил дублирование сообщения")
            return False
        else:
            raise e


async def safe_answer_callback(query, text: str = "✅ Выполнено", show_alert: bool = False):
    """Безопасный ответ на callback с обработкой timeout'ов"""
    try:
        await query.answer(text, show_alert=show_alert)
        return True
    except BadRequest as e:
        if "Query is too old" in str(e):
            logger.warning("Callback query устарел, пропускаем ответ")
            return False
        else:
            logger.error(f"Ошибка при ответе на callback: {e}")
            return False


# === ФУНКЦИИ ДЛЯ ИНТЕГРАЦИИ С СУЩЕСТВУЮЩИМ КОДОМ ===

def create_fake_update(query, user=None):
    """Создает fake update для совместимости с существующими функциями"""
    if user is None:
        user = query.from_user
        
    return type('Update', (), {
        'message': query.message,
        'effective_user': user,
        'callback_query': query
    })()


async def call_legacy_function(func, query, context, *args, **kwargs):
    """Безопасный вызов функций из test_bot_commands с обработкой ошибок"""
    try:
        # Создаем fake update
        fake_update = create_fake_update(query)
        
        # Вызываем функцию
        await func(fake_update, context, *args, **kwargs)
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при вызове legacy функции {func.__name__}: {e}")
        return False


# === ОТЛАДОЧНЫЕ ФУНКЦИИ ===

def get_cache_info():
    """Получить информацию о кеше для отладки"""
    info = {
        "cache_size": len(_menu_cache),
        "cache_keys": list(_menu_cache.keys()),
        "cache_timeout": _cache_timeout
    }
    
    for key, data in _menu_cache.items():
        age = time.time() - data["timestamp"]
        info[f"{key}_age"] = f"{age:.1f}s"
    
    return info


def log_cache_stats():
    """Логирование статистики кеша"""
    cache_info = get_cache_info()
    logger.debug(f"Статистика кеша: {cache_info}")


# === ЭКСПОРТ ОСНОВНЫХ ФУНКЦИЙ ===

__all__ = [
    'admin_panel',
    'handle_admin_callbacks', 
    'show_admin_panel',
    'show_tokens_menu',
    'show_users_menu',
    'clear_menu_cache',
    'get_cache_info'
]