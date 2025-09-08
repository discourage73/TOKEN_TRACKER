import logging
import asyncio
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# Импорты конфигурации и базовых компонентов
from config import TELEGRAM_TOKEN, logger
from user_database import user_db
from logging_config import setup_logging

# Настройка логирования
bot_logger = setup_logging('bot')

# Глобальный контекст для доступа из других модулей
_bot_context: Optional[ContextTypes.DEFAULT_TYPE] = None

def get_bot_context() -> Optional[ContextTypes.DEFAULT_TYPE]:
    """Возвращает глобальный контекст бота для использования в других модулях."""
    return _bot_context

def set_bot_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Устанавливает глобальный контекст бота."""
    global _bot_context
    _bot_context = context

# ============================================================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка команды /start."""
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # Добавляем пользователя в потенциальные
    user_db.add_potential_user(
        user_id=user_id,
        username=username,
        first_name=update.effective_user.first_name,
        last_name=update.effective_user.last_name
    )
    
    ascii_art = (
        "```\n"
        "░░░█▀▀░█▀█░█▀█░█▀▄░░░░▀█▀░█▀█░█▀█░█▀▄░█▀▀░░░░░\n"
        "░░░█░█░█░█░█░█░█░█░░░░░█░░█▀▄░█▀█░█░█░█▀▀░░░░░\n"
        "░░░▀▀▀░▀▀▀░▀▀▀░▀▀░░░░░░▀░░▀░▀░▀░▀░▀▀░░▀▀▀░░░░░\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "░░░█▀▀░█▀█░█▀▀░▀█▀░░░░▀█▀░█▀█░█▀█░█▀▄░█▀▀░░░░░\n"
        "░░░█▀░░█▀█░▀▀█░░█░░░░░░█░░█▀▄░█▀█░█░█░█▀▀░░░░░\n"
        "░░░▀░░░▀░▀░▀▀▀░░▀░░░░░░▀░░▀░▀░▀░▀░▀▀░░▀▀▀░░░░░\n"
        "```\n\n"
    )
    
    welcome_text = (
        ascii_art +
        "🤖 *Добро пожаловать в Token Tracker Bot!*\n\n"
        "📝 Отправьте адрес токена для получения информации\n"
        "📊 Используйте /help для списка команд\n\n"
        "_Ожидайте авторизации от администратора_"
    )
    
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN
    )
    
    bot_logger.info(f"Новый пользователь {user_id} ({username}) нажал /start")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка команды /help."""
    help_text = (
        "🤖 *Команды бота:*\n\n"
        "/start - Запуск бота\n"
        "/help - Помощь\n\n"
        "📝 *Использование:*\n"
        "Отправьте адрес токена для получения информации о нем\n\n"
        "_Доступ к функциям предоставляется администратором_"
    )
    
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.MARKDOWN
    )

# ============================================================================
# ОБРАБОТЧИКИ СООБЩЕНИЙ
# ============================================================================

async def handle_token_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка запросов токенов от авторизованных пользователей."""
    user_id = update.effective_user.id
    query = update.message.text.strip()
    
    bot_logger.info(f"Запрос токена от пользователя {user_id}: {query}")
    
    try:
        # Проверяем, является ли этот пользователь TARGET_BOT и отправляет ли контракт
        from config import TARGET_BOT
        
        # Если это пользователь 7037966490 (TARGET_BOT) и сообщение содержит "Contract:", то это контракт для рассылки всем
        if query.startswith("Contract:") and user_id == 7037966490:
            bot_logger.info(f"🚨 Получен контракт от TARGET_BOT {user_id} для рассылки всем: {query}")
            
            # Извлекаем адрес контракта с помощью regex (как в test_bot4)
            import re
            
            # Ищем строку с "Contract:" и извлекаем адрес
            contract_pattern = r'(?:Контракт|Contract):\s*([a-zA-Z0-9]{32,44})'
            matches = re.search(contract_pattern, query)
            
            if matches:
                contract_address = matches.group(1)
                bot_logger.info(f"📋 Извлеченный адрес контракта через regex: '{contract_address}' (длина: {len(contract_address)})")
            else:
                bot_logger.error(f"❌ Не удалось извлечь адрес контракта из: {query[:100]}...")
                await update.message.reply_text("❌ Неверный формат сообщения с контрактом")
                return
            
            # Импортируем функцию рассылки
            from token_service import broadcast_token_to_all_users, fetch_token_from_dexscreener
            from utils import process_token_data
            
            # Валидация адреса контракта
            if len(contract_address) < 32:
                bot_logger.error(f"❌ Неверная длина адреса контракта: {len(contract_address)} символов")
                await update.message.reply_text("❌ Неверный формат адреса контракта")
                return
            
            # Получаем данные о токене
            bot_logger.info(f"🔍 Запрашиваем данные токена через DexScreener API...")
            token_data_raw = await fetch_token_from_dexscreener(contract_address)
            
            if token_data_raw:
                bot_logger.info(f"✅ Получены данные токена, обрабатываем...")
                # Обрабатываем данные токена
                token_data = process_token_data(token_data_raw)
                
                # Рассылаем всем активным пользователям
                bot_logger.info(f"📤 Начинаем рассылку токена всем активным пользователям...")
                await broadcast_token_to_all_users(contract_address, token_data)
                
                # Отправляем подтверждение TARGET_BOT
                token_ticker = token_data.get('ticker', contract_address[:8] + '...')
                await update.message.reply_text(
                    f"✅ Токен {token_ticker} разослан всем активным пользователям",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                bot_logger.error(f"❌ DexScreener API не вернул данные для контракта {contract_address}")
                await update.message.reply_text(
                    f"❌ Не удалось получить данные о токене {contract_address[:8]}..."
                )
            
            return
    
        # Проверяем авторизацию для обычных пользователей
        if not user_db.is_user_authorized(user_id):
            await update.message.reply_text(
                "❌ У вас нет доступа к боту. Обратитесь к администратору.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Отправляем индикатор поиска для обычных пользователей
        search_msg = await update.message.reply_text("🔍 Поиск информации о токене...")
        
        # Импортируем и вызываем сервис получения токена
        from token_service import get_token_info
        
        result = await get_token_info(
            token_query=query,
            chat_id=update.effective_chat.id,
            message_id=None,
            context=context
        )
        
        # Удаляем сообщение о поиске
        await search_msg.delete()
        
        if not result:
            await update.message.reply_text(
                "❌ Не удалось получить информацию о токене. Проверьте адрес.",
                parse_mode=ParseMode.MARKDOWN
            )
        
    except Exception as e:
        bot_logger.error(f"Ошибка при обработке запроса токена: {e}")
        
        try:
            await search_msg.delete()
        except:
            pass
        
        await update.message.reply_text(
            "❌ Произошла ошибка при обработке запроса. Попробуйте позже.",
            parse_mode=ParseMode.MARKDOWN
        )

async def handle_unauthorized_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка сообщений от неавторизованных пользователей."""
    await update.message.reply_text(
        "❌ У вас нет доступа к боту. Используйте /start для регистрации.",
        parse_mode=ParseMode.MARKDOWN
    )

async def monitoring_watchdog() -> None:
    """Watchdog для контроля системы мониторинга токенов."""
    import asyncio
    from token_service import is_monitoring_active, restart_monitoring_system
    
    bot_logger.info("🐕‍🦺 Запущен watchdog мониторинга токенов")
    
    check_interval = 60  # Проверка каждые 60 секунд
    restart_count = 0
    max_restarts = 10
    
    while True:
        try:
            await asyncio.sleep(check_interval)
            
            if not is_monitoring_active():
                restart_count += 1
                bot_logger.warning(f"🚨 Мониторинг неактивен! Перезапуск #{restart_count}")
                
                if restart_count > max_restarts:
                    bot_logger.critical(f"❌ КРИТИЧНО: {max_restarts} перезапусков мониторинга! Останавливаем watchdog")
                    break
                
                await restart_monitoring_system()
                bot_logger.info(f"✅ Мониторинг перезапущен watchdog'ом")
            else:
                # Сбрасываем счетчик при успешной работе
                if restart_count > 0:
                    bot_logger.info(f"💚 Мониторинг работает стабильно. Счетчик перезапусков сброшен")
                    restart_count = 0
                    
        except Exception as e:
            bot_logger.error(f"Ошибка в watchdog мониторинга: {e}")
            await asyncio.sleep(30)
    
    bot_logger.error("🛑 Watchdog мониторинга остановлен")

async def daily_stats_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Задача для отправки ежедневной статистики с защитой от дубликатов."""
    try:
        # Проверяем, была ли статистика уже отправлена сегодня
        from datetime import datetime
        import os
        
        today = datetime.now().strftime('%Y-%m-%d')
        stats_marker_file = f"daily_stats_sent_{today}.marker"
        
        if os.path.exists(stats_marker_file):
            bot_logger.info(f"📊 Статистика уже была отправлена сегодня ({today}), пропускаем")
            return
        
        from token_service import send_daily_token_stats
        await send_daily_token_stats(context)
        
        # Создаем маркер успешной отправки
        with open(stats_marker_file, 'w') as f:
            f.write(f"Stats sent at {datetime.now()}")
        
        bot_logger.info("📊 Ежедневная статистика отправлена автоматически")
        
        # Удаляем старые маркеры (старше 7 дней)
        import glob
        old_markers = glob.glob("daily_stats_sent_*.marker")
        for marker in old_markers:
            try:
                marker_date = marker.split('_')[3].split('.')[0]  # извлекаем дату
                marker_datetime = datetime.strptime(marker_date, '%Y-%m-%d')
                if (datetime.now() - marker_datetime).days > 7:
                    os.remove(marker)
            except:
                pass
                
    except Exception as e:
        bot_logger.error(f"Ошибка при автоматической отправке статистики: {e}")

# ============================================================================
# ОБРАБОТЧИКИ CALLBACK
# ============================================================================

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка inline кнопок."""
    query = update.callback_query
    user_id = update.effective_user.id
    
    # Проверяем авторизацию
    if not user_db.is_user_authorized(user_id):
        await query.answer("❌ У вас нет доступа к боту")
        return
    
    try:
        # Импортируем роутер callback'ов из bot_commands
        from bot_commands import handle_callback_router
        await handle_callback_router(update, context)
        
    except Exception as e:
        bot_logger.error(f"Ошибка при обработке callback: {e}")
        await query.answer("❌ Произошла ошибка")

# ============================================================================
# ОБРАБОТЧИК ОШИБОК
# ============================================================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобальный обработчик ошибок."""
    bot_logger.error(f"Ошибка в боте: {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ Произошла внутренняя ошибка. Обратитесь к администратору."
            )
        except:
            pass

# ============================================================================
# ИНИЦИАЛИЗАЦИЯ И ЗАПУСК
# ============================================================================

def create_application() -> Application:
    """Создает и настраивает приложение бота."""
    # Создаем приложение с JobQueue
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Регистрируем команды
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    
    # Импортируем команды из bot_commands
    from bot_commands import admin_command, adduser_command, removeuser_command, list_command
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("adduser", adduser_command))
    app.add_handler(CommandHandler("removeuser", removeuser_command))
    app.add_handler(CommandHandler("list", list_command))
    
    # Регистрируем обработчики callback'ов
    app.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Регистрируем обработчики сообщений
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_token_request
    ))
    
    # Обработчик для неавторизованных (должен быть последним среди message handlers)
    app.add_handler(MessageHandler(filters.ALL, handle_unauthorized_message))
    
    # Регистрируем обработчик ошибок
    app.add_error_handler(error_handler)
    
    # Устанавливаем post_init
    app.post_init = post_init
    
    return app

async def post_init(application: Application) -> None:
    """Выполняется после инициализации приложения."""
    try:
        # Сохраняем контекст для использования в других модулях  
        set_bot_context(application)
        
        # Также устанавливаем контекст для token_service
        from token_service import set_telegram_context
        set_telegram_context(application)
        
        # Устанавливаем команды бота
        from bot_commands import setup_bot_commands
        await setup_bot_commands(application)
        
        # Запускаем систему мониторинга токенов
        from token_service import start_monitoring_system
        await start_monitoring_system(application)
        
        # Запускаем watchdog для мониторинга
        asyncio.create_task(monitoring_watchdog())
        
        # Настраиваем ежедневную отправку статистики в 10:00
        from datetime import time, datetime
        
        job_queue = application.job_queue
        if job_queue:
            # Ежедневная задача
            job_queue.run_daily(
                callback=daily_stats_job,
                time=time(hour=14, minute=50),  # 14:50 каждый день
                name="daily_token_stats"
            )
            
            # Добавляем retry задачи на случай пропуска основной
            job_queue.run_daily(
                callback=daily_stats_job,
                time=time(hour=14, minute=52),  # +2 минуты retry
                name="daily_token_stats_retry1"
            )
            
            job_queue.run_daily(
                callback=daily_stats_job,
                time=time(hour=14, minute=55),  # +5 минут retry
                name="daily_token_stats_retry2"
            )
            
            # Проверяем, не прошло ли сегодня 14:50
            now = datetime.now()
            today_1450 = now.replace(hour=14, minute=50, second=0, microsecond=0)
            
            if now < today_1450:
                # Если 14:50 еще не было сегодня - запланируем на сегодня
                seconds_until_1450 = (today_1450 - now).total_seconds()
                job_queue.run_once(
                    callback=daily_stats_job,
                    when=seconds_until_1450,
                    name="daily_token_stats_today"
                )
                print(f"STATS: Статистика будет отправлена через {int(seconds_until_1450)} секунд в 14:50")
            else:
                print("STATS: 14:50 уже прошло сегодня, статистика будет завтра")
            
            bot_logger.info("📊 Настроена ежедневная отправка статистики в 14:50 (+ retry в 14:52 и 14:55)")
        
        bot_logger.info("✅ Бот успешно инициализирован")
        
    except Exception as e:
        bot_logger.error(f"Ошибка при инициализации бота: {e}")
        raise

def main() -> None:
    """Точка входа для запуска бота."""
    try:
        bot_logger.info("🚀 Запуск Token Tracker Bot")
        
        # Создаем и запускаем приложение
        app = create_application()
        
        # Запускаем polling
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
    except KeyboardInterrupt:
        bot_logger.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        bot_logger.critical(f"💥 Критическая ошибка при запуске бота: {e}")
        raise
    finally:
        bot_logger.info("🔚 Завершение работы бота")

if __name__ == "__main__":
    main()