import logging
import asyncio
import traceback
import telegram
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from config import TELEGRAM_TOKEN, logger
# Импортируем модули проекта
import token_storage
from utils import format_tokens_list
from token_service import check_market_cap
from handlers.auth_middleware import admin_required, user_required
from config import CONTROL_ADMIN_IDS
# Настройка логирования
debug_logger = logging.getLogger('debug')


@user_required  
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет приветственное сообщение при команде /start."""
    try:
        user = update.effective_user
        user_id = user.id
        
        debug_logger.info(f"🔧 START функция вызвана для {user_id}")
        
        # ЗАПИСЫВАЕМ В ПОТЕНЦИАЛЬНЫЕ ПОЛЬЗОВАТЕЛИ
        debug_logger.info(f"🔧 Пытаемся импортировать user_db")
        from user_database import user_db
        debug_logger.info(f"🔧 user_db импортирован успешно")
        
        debug_logger.info(f"🔧 Вызываем add_potential_user для {user_id}")
        result = user_db.add_potential_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        debug_logger.info(f"🔧 add_potential_user вернул: {result}")
        
        # УСТАНАВЛИВАЕМ ПЕРСОНАЛЬНОЕ МЕНЮ
        debug_logger.info(f"🔧 Импортируем setup_user_menu")
        from handlers.user_commands import setup_user_menu
        await setup_user_menu(context, user_id)
        debug_logger.info(f"🔧 setup_user_menu выполнен")
        
        # ПРИВЕТСТВИЕ
        await update.message.reply_text(
            "I can provide information about tokens.\n\n"
            "Just send me a contract address or token name, and I'll show you its data.\n"
            "I also track Market Cap growth and send notifications for significant growth (x2, x3, x4...).\n\n"
            "Available commands:\n"
            "/start - start the bot\n"
            "/help - show help\n"
            "/list - show list of tracked tokens\n"
            "/clear - delete/manage tokens\n\n"
        )
        debug_logger.info(f"Отправлено приветственное сообщение пользователю {user_id}")
        
    except Exception as e:
        debug_logger.error(f"🔧 ОШИБКА в start: {str(e)}")
        debug_logger.error(traceback.format_exc())

@user_required
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет справочное сообщение с учетом роли пользователя."""
    try:
        user_id = update.effective_user.id
        
        if user_id in CONTROL_ADMIN_IDS:
            # Справка для администратора
            help_text = (
                "📚 **Справка (Администратор)**\n\n"
                "**Доступные команды:**\n"
                "/start - главное меню\n"
                "/help - эта справка\n" 
                "/list - список токенов\n"
                "/clear - управление токенами\n"
                "/analytics - полная аналитика\n"
                "/stats - статистика за 12ч\n\n"
                "**Отправьте адрес токена** для добавления в отслеживание."
            )
        else:
            # Справка для обычного пользователя
            help_text = (
                "📚 **Справка**\n\n"
                "**Доступные команды:**\n"
                "/start - главное меню\n"
                "/help - эта справка\n\n"
                "🤖 Вы будете получать уведомления о новых токенах и их росте.\n"
                "📊 Администратор управляет списком отслеживаемых токенов."
            )
        
        await update.message.reply_text(help_text)
        debug_logger.info(f"Команда /help выполнена для пользователя {user_id}")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при отправке справочного сообщения: {str(e)}")
        debug_logger.error(traceback.format_exc())
@admin_required
async def list_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает список всех отслеживаемых токенов с пагинацией."""
    try:
        debug_logger.info("Запрошен список отслеживаемых токенов")
        
        # Получаем chat_id для идентификации чата
        chat_id = update.message.chat_id
        
        # Извлекаем номер страницы, если он передан в аргументах команды
        page = 0
        if context.args and len(context.args) > 0:
            try:
                page = int(context.args[0]) - 1  # Преобразуем в 0-based индекс
                if page < 0:
                    page = 0
            except ValueError:
                page = 0
        
        # Проверяем, есть ли предыдущее сообщение со списком токенов для этого чата
        prev_message_id = token_storage.get_list_message_id(chat_id)
        
        # Удаляем команду /list пользователя (если бот имеет права на удаление сообщений)
        try:
            await update.message.delete()
            debug_logger.info(f"Команда /list удалена из чата {chat_id}")
        except Exception as e:
            debug_logger.warning(f"Не удалось удалить команду /list: {e}")
        
        # Если есть предыдущее сообщение, удаляем его
        if prev_message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
                debug_logger.info(f"Предыдущее сообщение со списком токенов удалено (ID: {prev_message_id})")
            except Exception as e:
                debug_logger.warning(f"Не удалось удалить предыдущее сообщение: {e}")
        
        # Отправляем новое сообщение с уведомлением об обновлении данных
        wait_message = await context.bot.send_message(
            chat_id=chat_id,
            text="Обновляю данные о токенах...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Сохраняем ID нового сообщения
        token_storage.store_list_message_id(chat_id, wait_message.message_id)
        
        # Получаем все токены (БЕЗ параметра include_hidden)
        active_tokens = token_storage.get_all_tokens()
        
        if not active_tokens:
            await wait_message.edit_text(
                "Нет активных токенов в списке отслеживаемых."
            )
            debug_logger.info("Список токенов пуст")
            return
        
        # Форматируем список токенов с пагинацией
        tokens_per_page = 10  # Отображаем по 10 токенов на странице
        message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
        
        # Создаем кнопки навигации
        keyboard = []
        
        # Кнопки для навигации по страницам
        nav_buttons = []
        if total_pages > 1:
            if current_page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
            
            if current_page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопки для действий со списком
        action_buttons = [
            InlineKeyboardButton("🔄 Обновить", callback_data=f"list_page:{current_page}")
        ]
        keyboard.append(action_buttons)
        
        # Кнопка для управления токенами
        keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сообщение с актуальными данными
        try:
            await wait_message.edit_text(
                message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            debug_logger.info(f"Список токенов успешно отправлен (страница {current_page+1} из {total_pages})")
        except telegram.error.BadRequest as e:
            if "Message is too long" in str(e):
                # Если сообщение слишком длинное, уменьшаем количество токенов на странице
                tokens_per_page = 5
                message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
                
                # Обновляем кнопки навигации
                keyboard = []
                nav_buttons = []
                
                if total_pages > 1:
                    if current_page > 0:
                        nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
                    
                    if current_page < total_pages - 1:
                        nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
                
                if nav_buttons:
                    keyboard.append(nav_buttons)
                
                keyboard.append(action_buttons)
                keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await wait_message.edit_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True
                )
                debug_logger.info(f"Список токенов успешно отправлен (страница {current_page+1} из {total_pages}, уменьшено количество токенов на странице)")
            else:
                # Если произошла другая ошибка
                error_message = "Произошла ошибка при формировании списка токенов. Пожалуйста, попробуйте позже."
                await wait_message.edit_text(error_message)
                debug_logger.error(f"Ошибка при отправке списка токенов: {str(e)}")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при выполнении команды /list: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при формировании списка токенов. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass


async def handle_refresh_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает обновление списка токенов."""
    query = update.callback_query
    chat_id = query.message.chat_id
    
    try:
        # Получаем текущую страницу из данных callback, по умолчанию - первая страница
        page = 0
        if ":" in query.data:
            _, page = query.data.split(":", 1)
            page = int(page)
        
        # Уведомляем пользователя о начале обновления
        await query.answer("Обновляю список токенов...")
        
        # Сохраняем ID текущего сообщения
        token_storage.store_list_message_id(chat_id, query.message.message_id)
        
        # Обновляем данные о токенах перед получением списка
        # Создаем задачу для проверки Market Cap всех токенов
        await check_market_cap(context)
        
        # Получаем свежие данные о всех токенах (БЕЗ параметра include_hidden)
        active_tokens = token_storage.get_all_tokens()
        
        if not active_tokens:
            await query.edit_message_text(
                "Нет активных токенов в списке отслеживаемых.",
                parse_mode=ParseMode.MARKDOWN
            )
            debug_logger.info("Список токенов пуст после обновления")
            return
        
        # Форматируем список токенов с пагинацией
        tokens_per_page = 10  # Отображаем по 10 токенов на странице
        message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
        
        # Создаем кнопки навигации
        keyboard = []
        
        # Кнопки для навигации по страницам
        nav_buttons = []
        if total_pages > 1:
            if current_page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
            
            if current_page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопки для действий со списком
        action_buttons = [
            InlineKeyboardButton("🔄 Обновить", callback_data=f"list_page:{current_page}")
        ]
        keyboard.append(action_buttons)
        
        # Кнопка для управления токенами
        keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сообщение с актуальными данными
        try:
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            debug_logger.info(f"Список из {len(active_tokens)} токенов успешно обновлен (страница {current_page+1} из {total_pages})")
        except telegram.error.BadRequest as e:
            if "Message is too long" in str(e):
                # Если сообщение слишком длинное, уменьшаем количество токенов на странице
                tokens_per_page = 5
                message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
                
                # Обновляем кнопки навигации
                keyboard = []
                nav_buttons = []
                
                if total_pages > 1:
                    if current_page > 0:
                        nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
                    
                    if current_page < total_pages - 1:
                        nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
                
                if nav_buttons:
                    keyboard.append(nav_buttons)
                
                keyboard.append(action_buttons)
                keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True
                )
                debug_logger.info(f"Список токенов успешно обновлен (страница {current_page+1} из {total_pages}, уменьшено количество токенов на странице)")
            else:
                # Если произошла другая ошибка
                debug_logger.error(f"Ошибка при обновлении списка токенов: {str(e)}")
                await query.answer("Ошибка при обновлении списка. Пожалуйста, попробуйте позже.")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при обновлении списка токенов: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка при обновлении списка. Пожалуйста, попробуйте позже.")
        except Exception:
            pass
@admin_required
async def clear_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает меню управления токенами (удаление)."""
    try:
        debug_logger.info("Запрошено управление токенами")
        
        # Получаем словарь со всеми токенами (БЕЗ параметра include_hidden)
        all_tokens = token_storage.get_all_tokens()
        tokens_count = len(all_tokens)
        
        if tokens_count == 0:
            await update.message.reply_text(
                "Нет сохраненных токенов для управления."
            )
            return
        
        # Только опции для удаления токенов
        keyboard = [
            [InlineKeyboardButton("⛔ Удалить все", callback_data="delete_all_confirm")],
            [InlineKeyboardButton("🔍 Выборочное удаление", callback_data="delete_selective")],
            [InlineKeyboardButton("❌ Отмена", callback_data="clear_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Получаем число токенов
        visible_tokens_count = len(token_storage.get_all_tokens())
        
        # Отправляем сообщение с опциями
        await update.message.reply_text(
            f"Выберите действие для токенов (всего: {visible_tokens_count}):\n\n"
            "⛔ *Удалить все* - удалит все токены полностью.\n"
            "🔍 *Выборочное удаление* - позволит выбрать токены для удаления.\n"
            "❌ *Отмена* - отменить операцию.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        debug_logger.info(f"Отправлен запрос на управление токенами (всего: {visible_tokens_count})")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при выполнении команды /clear: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при обработке запроса. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass
@admin_required
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает статистику по токенам за последние 12 часов."""
    try:
        debug_logger.info("Запрошена статистика по токенам")
        
        # Отправляем сообщение о начале формирования статистики
        wait_message = await update.message.reply_text(
            "Формирую статистику по токенам за последние 12 часов...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Импортируем функцию для отправки статистики
        from token_service import send_token_stats
        
        # Вызываем функцию для отправки статистики
        await send_token_stats(context)
        
        # Удаляем сообщение об ожидании
        try:
            await wait_message.delete()
        except Exception as e:
            debug_logger.error(f"Ошибка при удалении сообщения об ожидании: {e}")
        
        debug_logger.info("Статистика по токенам успешно отправлена")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при формировании статистики: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при формировании статистики. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass

# ДОБАВИТЬ ЭТУ КОМАНДУ В test_bot_commands.py после stats_command

@admin_required
async def weekly_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает статистику по токенам за последние 7 дней."""
    try:
        debug_logger.info("Запрошена недельная статистика по токенам")
        
        # Отправляем сообщение о начале формирования статистики
        wait_message = await update.message.reply_text(
            "📊 Формирую недельную статистику по токенам за последние 7 дней...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Импортируем функцию для отправки недельной статистики
        from token_service import send_weekly_token_stats
        
        # Вызываем функцию для отправки недельной статистики
        await send_weekly_token_stats(context)
        
        # Удаляем сообщение об ожидании
        try:
            await wait_message.delete()
        except Exception as e:
            debug_logger.error(f"Ошибка при удалении сообщения об ожидании: {e}")
        
        debug_logger.info("Недельная статистика по токенам успешно отправлена")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при формировании недельной статистики: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при формировании недельной статистики. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass

@admin_required
async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает полную аналитику по всем токенам (включая старые)."""
    try:
        debug_logger.info("Запрошена полная аналитика по всем токенам")
        
        # Отправляем сообщение о начале формирования аналитики
        wait_message = await update.message.reply_text(
            "Формирую полную аналитику по всем токенам (включая старые)...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Импортируем функцию для отправки аналитики
        from token_service import generate_analytics_excel
        
        # Вызываем функцию для отправки полной аналитики
        await generate_analytics_excel(context, update.message.chat_id)
        
        # Удаляем сообщение об ожидании
        try:
            await wait_message.delete()
        except Exception as e:
            debug_logger.error(f"Ошибка при удалении сообщения об ожидании: {e}")
        
        debug_logger.info("Полная аналитика по всем токенам успешно отправлена")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при формировании полной аналитики: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при формировании полной аналитики. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass


async def handle_clear_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает отмену удаления всех токенов."""
    query = update.callback_query
    
    try:
        # Обновляем сообщение
        await query.edit_message_text(
            "❌ Операция удаления отменена.\n\n"
            "Все данные о токенах сохранены.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        await query.answer("Операция отменена")
        debug_logger.info("Удаление токенов отменено пользователем")
    except Exception as e:
        debug_logger.error(f"Ошибка при отмене удаления токенов: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка при отмене удаления.")
        except:
            pass


async def handle_clear_return(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает запрос на возврат к основному меню управления токенами."""
    query = update.callback_query
    
    try:
        # Создаем основные кнопки меню управления
        keyboard = [
            [InlineKeyboardButton("⛔ Удалить все", callback_data="delete_all_confirm")],
            [InlineKeyboardButton("🔍 Выборочное удаление", callback_data="delete_selective")],
            [InlineKeyboardButton("❌ Отмена", callback_data="clear_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Получаем число токенов (БЕЗ параметра include_hidden)
        visible_tokens_count = len(token_storage.get_all_tokens())
        
        # Обновляем сообщение
        await query.edit_message_text(
            f"Выберите действие для токенов (всего: {visible_tokens_count}):\n\n"
            "⛔ *Удалить все* - удалит все токены полностью.\n"
            "🔍 *Выборочное удаление* - позволит выбрать токены для удаления.\n"
            "❌ *Отмена* - отменить операцию.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        
        await query.answer()
        debug_logger.info("Возврат к основному меню управления токенами")
    except Exception as e:
        debug_logger.error(f"Ошибка при возврате к меню: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass


async def handle_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает подтверждение полного удаления всех токенов."""
    query = update.callback_query
    
    try:
        # Логируем действие
        debug_logger.info("Подтверждено полное удаление всех токенов")
        
        # Получаем число токенов перед удалением (для логирования)
        tokens_count = len(token_storage.get_all_tokens())
        
        # Полностью удаляем все токены
        deleted_count = token_storage.delete_all_tokens()
        
        # Обновляем сообщение
        await query.edit_message_text(
            f"✅ *Все токены удалены ({deleted_count} шт.)*\n\n"
            "Они полностью удалены из базы данных и не могут быть восстановлены.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        await query.answer("Токены успешно удалены")
        debug_logger.info(f"Удалены все токены ({deleted_count} шт.)")
    except Exception as e:
        debug_logger.error(f"Ошибка при подтверждении удаления токенов: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка при удалении токенов.")
        except:
            pass


async def handle_delete_all_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает запрос на полное удаление всех токенов."""
    query = update.callback_query
    
    try:
        # Создаем кнопки для подтверждения
        keyboard = [
            [
                InlineKeyboardButton("✅ Да, удалить все", callback_data="delete_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="clear_cancel")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Получаем число токенов (БЕЗ параметра include_hidden)
        tokens_count = len(token_storage.get_all_tokens())
        
        # Обновляем сообщение с запросом подтверждения
        await query.edit_message_text(
            f"Вы уверены, что хотите полностью удалить *все* токены? ({tokens_count} шт.)\n\n"
            "⚠️ Это действие нельзя отменить. Все данные будут полностью удалены из базы данных.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        
        await query.answer()
        debug_logger.info(f"Запрос подтверждения полного удаления всех токенов ({tokens_count} шт.)")
    except Exception as e:
        debug_logger.error(f"Ошибка при запросе подтверждения удаления: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass


async def handle_delete_selective(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает запрос на выборочное удаление токенов."""
    query = update.callback_query
    page = 0
    
    try:
        # Извлекаем номер страницы из callback_data, если он есть
        if ":" in query.data:
            _, page = query.data.split(":", 1)
            page = int(page)
        
        # Получаем все токены (БЕЗ параметра include_hidden)
        tokens = token_storage.get_all_tokens()
        
        if not tokens:
            await query.edit_message_text(
                "Нет токенов для удаления.",
                parse_mode=ParseMode.MARKDOWN
            )
            await query.answer()
            return
        
        # Формируем список токенов
        tokens_list = list(tokens.items())
        tokens_count = len(tokens_list)
        
        # Настройки пагинации
        tokens_per_page = 5
        total_pages = (tokens_count + tokens_per_page - 1) // tokens_per_page  # округление вверх
        
        # Проверяем, что страница в допустимых пределах
        if page >= total_pages:
            page = 0
        
        # Получаем токены для текущей страницы
        start_idx = page * tokens_per_page
        end_idx = min(start_idx + tokens_per_page, tokens_count)
        page_tokens = tokens_list[start_idx:end_idx]
        
        # Создаем кнопки для выбора токенов
        keyboard = []
        
        for idx, (token_query, token_data) in enumerate(page_tokens, start=start_idx + 1):
            # Получаем тикер для отображения
            ticker = "Неизвестно"
            if 'token_info' in token_data and 'ticker' in token_data['token_info']:
                ticker = token_data['token_info']['ticker']
            
            # Добавляем кнопку для токена
            keyboard.append([
                InlineKeyboardButton(
                    f"{idx}. {ticker}",
                    callback_data=f"delete_token:{token_query}"
                )
            ])
        
        # Добавляем навигационные кнопки, если страниц больше одной
        nav_buttons = []
        
        if total_pages > 1:
            # Кнопка предыдущей страницы
            prev_page = (page - 1) % total_pages
            nav_buttons.append(InlineKeyboardButton(
                "⬅️ Назад",
                callback_data=f"delete_selective:{prev_page}"
            ))
            
            # Кнопка следующей страницы
            next_page = (page + 1) % total_pages
            nav_buttons.append(InlineKeyboardButton(
                "Вперёд ➡️",
                callback_data=f"delete_selective:{next_page}"
            ))
        
        # Добавляем навигационные кнопки в клавиатуру
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Добавляем кнопку отмены
        keyboard.append([
            InlineKeyboardButton("↩️ Вернуться", callback_data="clear_return")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сообщение
        await query.edit_message_text(
            f"Выберите токен для удаления (страница {page + 1}/{total_pages}):\n\n"
            "⚠️ Выбранные токены будут полностью удалены из базы данных. "
            "Это действие нельзя отменить.\n",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        
        await query.answer()
        debug_logger.info(f"Отображен список токенов для выборочного удаления (страница {page + 1}/{total_pages})")
    except Exception as e:
        debug_logger.error(f"Ошибка при выборочном удалении: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass


async def handle_delete_token(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает запрос на удаление конкретного токена."""
    query = update.callback_query
    
    try:
        # Извлекаем запрос токена из callback_data
        _, token_query = query.data.split(":", 1)
        
        # Получаем тикер для отображения в сообщении
        token_data = token_storage.get_token_data(token_query)
        ticker = "Неизвестно"
        if token_data and 'token_info' in token_data and 'ticker' in token_data['token_info']:
            ticker = token_data['token_info']['ticker']
        
        # Удаляем токен
        success = token_storage.delete_token(token_query)
        
        if success:
            await query.answer(f"Токен {ticker} удален")
            debug_logger.info(f"Токен {token_query} ({ticker}) удален")
            
            # Проверяем, остались ли еще токены перед возвратом к списку
            remaining_tokens = token_storage.get_all_tokens()
            if not remaining_tokens:
                # Если токенов больше нет, показываем сообщение об этом
                await query.edit_message_text(
                    "Все токены были удалены. Список пуст.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
                
            # Возвращаемся к списку выборочного удаления
            await handle_delete_selective(update, context)
        else:
            await query.answer("Не удалось удалить токен")
            debug_logger.warning(f"Не удалось удалить токен {token_query}")
    except Exception as e:
        debug_logger.error(f"Ошибка при удалении токена: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка при удалении токена")
        except:
            pass


async def setup_bot_commands(application) -> None:
    """Устанавливает список команд бота с описаниями."""
    try:
        # Добавляем команды
        commands = [
            BotCommand("start", "start the bot"),
            BotCommand("help", "show help"),
            BotCommand("list", "list of tracked tokens"),
            BotCommand("clear", "delete/manage tokens"),
            BotCommand("analytics", "Token analytics"),
            BotCommand("stats", "stats (12 h)"),
            BotCommand("weekly_stats", "weekly stats (7 days)")
        ]
        
        await application.bot.set_my_commands(commands)
        debug_logger.info("Команды бота успешно настроены")
    except Exception as e:
        debug_logger.error(f"Ошибка при настройке команд бота: {str(e)}")
        debug_logger.error(traceback.format_exc())


def setup_commands_direct(token):
    """Устанавливает команды бота напрямую через HTTP API."""
    try:
        import requests
        
        # Добавляем команды
        commands = [
            {"command": "start", "description": "start the bot"},
            {"command": "help", "description": "show help"},
            {"command": "list", "description": "list of tracked tokens"},
            {"command": "clear", "description": "delete/manage tokens"},
            {"command": "stats", "description": "stats (12 h)"},
            {"command": "weekly_stats", "description": "weekly stats (7 days)"}
        ]
        
        url = f"https://api.telegram.org/bot{token}/setMyCommands"
        response = requests.post(url, json={"commands": commands})
        
        if response.status_code == 200 and response.json().get("ok"):
            debug_logger.info("Команды бота успешно настроены напрямую через API")
        else:
            debug_logger.error(f"Ошибка при настройке команд бота через API: {response.text}")
    except Exception as e:
        debug_logger.error(f"Ошибка при настройке команд бота через API: {str(e)}")
        debug_logger.error(traceback.format_exc())

# Добавить эту команду в test_bot_commands.py

@admin_required
async def weekly_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает статистику по токенам за последние 7 дней."""
    try:
        debug_logger.info("Запрошена недельная статистика по токенам")
        
        # Отправляем сообщение о начале формирования статистики
        wait_message = await update.message.reply_text(
            "📊 Формирую недельную статистику по токенам за последние 7 дней...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Импортируем функцию для отправки недельной статистики
        from token_service import send_weekly_token_stats
        
        # Вызываем функцию для отправки недельной статистики
        await send_weekly_token_stats(context)
        
        # Удаляем сообщение об ожидании
        try:
            await wait_message.delete()
        except Exception as e:
            debug_logger.error(f"Ошибка при удалении сообщения об ожидании: {e}")
        
        debug_logger.info("Недельная статистика по токенам успешно отправлена")
        
    except Exception as e:
        debug_logger.error(f"Ошибка при формировании недельной статистики: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await update.message.reply_text(
                "Произошла ошибка при формировании недельной статистики. Пожалуйста, попробуйте позже."
            )
        except Exception:
            pass

@admin_required
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add user: /adduser 123456789 @username"""
    if not context.args:
        await update.message.reply_text("Usage: /adduser USER_ID [@username]")
        return
    
    try:
        user_id = int(context.args[0])
        username = context.args[1].replace('@', '') if len(context.args) > 1 else None
        
        from user_database import user_db
        if user_db.add_user(user_id, username):
            await update.message.reply_text(f"✅ User {user_id} added!")
        else:
            await update.message.reply_text("❌ Error adding user")
    except ValueError:
        await update.message.reply_text("❌ User ID must be a number")

@admin_required  
async def remove_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove user: /removeuser 123456789"""
    if not context.args:
        await update.message.reply_text("Usage: /removeuser USER_ID")
        return
    
    try:
        user_id = int(context.args[0])
        
        from user_database import user_db
        if user_db.remove_user(user_id):
            await update.message.reply_text(f"✅ User {user_id} removed!")
        else:
            await update.message.reply_text("❌ User not found")
    except ValueError:
        await update.message.reply_text("❌ User ID must be a number")

@admin_required
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Простая админ панель"""
    try:
        # Получаем статистику пользователей
        from user_database import user_db
        all_users = user_db.get_all_users()
        active_users = [u for u in all_users if u['is_active']]
        
        admin_message = (
            f"👑 **Админ панель**\n\n"
            f"👥 Всего пользователей: {len(all_users)}\n"
            f"✅ Активных: {len(active_users)}\n\n"
            f"**Последние 5 пользователей:**\n"
        )
        
        # Показываем последних 5 пользователей
        for user in all_users[:5]:
            status = "✅" if user['is_active'] else "❌"
            username = user['username'] or 'N/A'
            admin_message += f"{status} `{user['user_id']}` @{username}\n"
        
        if not all_users:
            admin_message += "Нет пользователей в базе."
            
        await update.message.reply_markdown(admin_message)
        debug_logger.info("Админ панель показана")
        
    except Exception as e:
        debug_logger.error(f"Ошибка в админ панели: {str(e)}")
        await update.message.reply_text("Ошибка при получении данных админ панели.")